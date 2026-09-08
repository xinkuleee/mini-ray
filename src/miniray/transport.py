"""Small, synchronous request/reply transport used by mini-Ray.

The transport deliberately knows nothing about mini-Ray's task, object, or actor
protocols.  A caller names a handler and sends it one Python object.  The
handler returns another Python object.

Frames use a four-byte, network-order length prefix followed by a pickle
payload.  ``cloudpickle`` is preferred when it is installed so teaching
examples can carry ordinary Python objects; the standard library ``pickle``
module is the fallback.  Pickle is unsafe for untrusted input, so both clients
and servers are restricted to ``127.0.0.1``.
"""

from __future__ import annotations

import math
import pickle
import socket
import socketserver
import struct
import threading
import time
import traceback
import uuid
from contextlib import nullcontext
from dataclasses import dataclass
from types import MappingProxyType
from typing import Callable, ContextManager, Mapping, Optional, Tuple

from .trace import (
    EventSink,
    _advance_causal_cursor,
    causal_scope,
    current_cause_id,
    current_event_sink,
)

try:
    import cloudpickle as _pickler
except ImportError:  # pragma: no cover - exercised only in minimal installs.
    _pickler = pickle


LOOPBACK_HOST = "127.0.0.1"
DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024
# Trace metadata is outside the business frame budget.  The allowance is
# deliberately small and fixed, and _encode drops the sidecar if it cannot fit
# so tracing can never turn an otherwise-valid business value into a failure.
_MAX_TRACE_SIDECAR_BYTES = 4096
_LENGTH = struct.Struct("!I")
# Pickle never produces an empty payload, so a zero-length frame is a private
# escape introducing exactly one sidecar frame followed by one ordinary frame.
# Ordinary untraced frames retain their byte-for-byte wire representation and
# their original memory limit.
_TRACE_PREFIX = _LENGTH.pack(0)

Handler = Callable[[object], object]
Address = Tuple[str, int]
DEFAULT_TRACE_COMPONENT = "transport"


class TransportError(RuntimeError):
    """Base class for local transport failures."""


class TransportTimeout(TransportError):
    """A connect, send, or receive operation exceeded its timeout."""


class TransportConnectionError(TransportError):
    """A connection could not be established, before any request was sent."""


class TransportConnectionTimeout(TransportConnectionError, TransportTimeout):
    """Connection establishment exceeded its timeout."""


class ConnectionClosed(TransportError):
    """The peer closed a connection before a complete frame arrived."""


class FrameTooLarge(TransportError):
    """A frame exceeded the configured memory-safety limit."""


class _PreSendTransportError(TransportError):
    """A private marker proving no reply bytes were sent."""

    def __init__(self, original: TransportError) -> None:
        self.original = original
        try:
            message = str(original)
        except BaseException:
            message = "pre-send transport failure"
        super().__init__(message)


class RemoteCallError(TransportError):
    """A named server handler failed.

    Remote exceptions are represented as text instead of being re-raised or
    unpickled as arbitrary exception classes.
    """

    def __init__(
        self,
        handler: str,
        remote_type: str,
        message: str,
        traceback_text: str,
    ) -> None:
        self.handler = handler
        self.remote_type = remote_type
        self.remote_message = message
        self.remote_traceback = traceback_text
        super().__init__(
            "remote handler {!r} failed with {}: {}".format(
                handler, remote_type, message
            )
        )


@dataclass(frozen=True)
class _WireTraceSidecar:
    """Private causal metadata, deliberately separate from business DTOs.

    On a request this names the client's send-attempt event.  On a reply it
    names the server's send-attempt event.  A generic predecessor name keeps
    the same private envelope meaningful in both directions.
    """

    rpc_id: str
    predecessor_event_id: Optional[str]

    def __post_init__(self) -> None:
        if not isinstance(self.rpc_id, str) or not self.rpc_id:
            raise ValueError("rpc_id must be a non-empty string")
        if len(self.rpc_id.encode("utf-8")) > 128:
            raise ValueError("rpc_id is too long")
        value = self.predecessor_event_id
        if value is not None and (not isinstance(value, str) or not value):
            raise ValueError(
                "predecessor_event_id must be a non-empty string or None"
            )
        if value is not None and len(value.encode("utf-8")) > 1024:
            raise ValueError("predecessor_event_id is too long")


@dataclass(frozen=True)
class _TracedWireValue:
    """In-memory value plus optional wire-only causal metadata."""

    value: object
    trace: _WireTraceSidecar


@dataclass(frozen=True)
class _WireRequest:
    handler: str
    payload: object


@dataclass(frozen=True)
class _WireReply:
    ok: bool
    value: object = None
    error_type: str = ""
    error_message: str = ""
    traceback_text: str = ""


def _validate_host(host: str) -> None:
    if host != LOOPBACK_HOST:
        raise ValueError(
            "mini-Ray's pickle transport is restricted to 127.0.0.1"
        )


def _validate_timeout(
    name: str, value: Optional[float], *, allow_none: bool = False
) -> None:
    if value is None:
        if allow_none:
            return
        raise ValueError("{} must be a positive finite number".format(name))
    if not math.isfinite(value) or value <= 0:
        raise ValueError("{} must be a positive finite number".format(name))


def _remaining_timeout(
    deadline: Optional[float], fallback: Optional[float]
) -> Optional[float]:
    """Return the next socket timeout without extending an absolute deadline."""

    if deadline is None:
        return fallback
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TransportTimeout("RPC deadline expired")
    return remaining if fallback is None else min(fallback, remaining)


def _validate_max_frame_bytes(value: int) -> None:
    if value <= 0 or value > 0xFFFFFFFF:
        raise ValueError("max_frame_bytes must be between 1 and 2**32 - 1")


def _serialize(value: object) -> bytes:
    try:
        return _pickler.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception as exc:
        raise TransportError("could not serialize transport value") from exc


def _encode(value: object, max_frame_bytes: int) -> bytes:
    business_value = value
    sidecar = None  # type: Optional[_WireTraceSidecar]
    if isinstance(value, _TracedWireValue):
        business_value = value.value
        sidecar = value.trace
        try:
            candidate = _serialize(sidecar)
            if len(candidate) <= _MAX_TRACE_SIDECAR_BYTES:
                sidecar = value.trace
            else:
                sidecar = None
        except TransportError:
            # Trace metadata is observational.  Falling back to the exact
            # untraced envelope is always preferable to failing an RPC.
            sidecar = None

    payload = _serialize(business_value)
    if len(payload) > max_frame_bytes:
        raise FrameTooLarge(
            "outgoing frame is {} bytes; limit is {}".format(
                len(payload), max_frame_bytes
            )
        )
    business_frame = _LENGTH.pack(len(payload)) + payload
    if sidecar is None:
        return business_frame
    try:
        sidecar_payload = _serialize(sidecar)
    except TransportError:
        return business_frame
    if len(sidecar_payload) > _MAX_TRACE_SIDECAR_BYTES:
        return business_frame
    return (
        _TRACE_PREFIX
        + _LENGTH.pack(len(sidecar_payload))
        + sidecar_payload
        + business_frame
    )


def _recv_exact(
    connection: socket.socket, size: int, *, deadline: Optional[float] = None,
    request_timeout: Optional[float] = None,
) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        try:
            if deadline is not None:
                connection.settimeout(
                    _remaining_timeout(deadline, request_timeout)
                )
            chunk = connection.recv(size - len(chunks))
        except socket.timeout as exc:
            raise TransportTimeout("timed out while receiving a frame") from exc
        except OSError as exc:
            raise TransportError("failed while receiving a frame") from exc
        if not chunk:
            raise ConnectionClosed(
                "peer closed the connection after {} of {} bytes".format(
                    len(chunks), size
                )
            )
        chunks.extend(chunk)
    return bytes(chunks)


def _receive_payload(
    connection: socket.socket, size: int, max_frame_bytes: int,
    *, deadline: Optional[float] = None,
    request_timeout: Optional[float] = None,
) -> object:
    if size == 0:
        raise TransportError("empty transport frame")
    if size > max_frame_bytes:
        raise FrameTooLarge(
            "incoming frame declares {} bytes; limit is {}".format(
                size, max_frame_bytes
            )
        )
    payload = _recv_exact(
        connection, size, deadline=deadline, request_timeout=request_timeout
    )
    try:
        value = pickle.loads(payload)
    except Exception as exc:
        raise TransportError("could not deserialize transport value") from exc
    return value


def _receive(
    connection: socket.socket, max_frame_bytes: int,
    *, deadline: Optional[float] = None,
    request_timeout: Optional[float] = None,
) -> object:
    header = _recv_exact(
        connection, _LENGTH.size, deadline=deadline,
        request_timeout=request_timeout,
    )
    (size,) = _LENGTH.unpack(header)
    if size != 0:
        return _receive_payload(
            connection, size, max_frame_bytes, deadline=deadline,
            request_timeout=request_timeout,
        )

    # A traced frame has exactly two non-empty subframes.  Parse them without
    # recursion so malformed peers cannot build nested trace prefixes.
    sidecar_header = _recv_exact(
        connection, _LENGTH.size, deadline=deadline,
        request_timeout=request_timeout,
    )
    (sidecar_size,) = _LENGTH.unpack(sidecar_header)
    sidecar = _receive_payload(
        connection, sidecar_size, _MAX_TRACE_SIDECAR_BYTES, deadline=deadline,
        request_timeout=request_timeout,
    )
    if not isinstance(sidecar, _WireTraceSidecar):
        raise TransportError("expected a trace sidecar envelope")
    business_header = _recv_exact(
        connection, _LENGTH.size, deadline=deadline,
        request_timeout=request_timeout,
    )
    (business_size,) = _LENGTH.unpack(business_header)
    value = _receive_payload(
        connection, business_size, max_frame_bytes, deadline=deadline,
        request_timeout=request_timeout,
    )
    return _TracedWireValue(value, sidecar)


def _send(connection: socket.socket, value: object, max_frame_bytes: int) -> None:
    try:
        frame = _encode(value, max_frame_bytes)
    except TransportError as exc:
        raise _PreSendTransportError(exc) from exc
    try:
        connection.sendall(frame)
    except socket.timeout as exc:
        raise TransportTimeout("timed out while sending a frame") from exc
    except OSError as exc:
        raise TransportError("failed while sending a frame") from exc


def _failure_reply(exc: BaseException) -> _WireReply:
    try:
        message = str(exc)
    except BaseException:
        message = "<unprintable {}>".format(type(exc).__name__)
    try:
        traceback_text = traceback.format_exc()
    except BaseException:
        traceback_text = "<traceback unavailable>"
    return _WireReply(
        ok=False,
        error_type=type(exc).__name__,
        error_message=message,
        traceback_text=traceback_text,
    )


def _format_address(address: object) -> str:
    try:
        host, port = address  # type: ignore[misc]
        return "{}:{}".format(host, port)
    except Exception:
        return "unknown"


def _trace_sidecar(value: object) -> Optional[_WireTraceSidecar]:
    try:
        if not isinstance(value, _TracedWireValue) or not isinstance(
            value.trace, _WireTraceSidecar
        ):
            return None
        sidecar = value.trace
        if not isinstance(sidecar.rpc_id, str) or not sidecar.rpc_id:
            return None
        if len(sidecar.rpc_id.encode("utf-8")) > 128:
            return None
        predecessor = sidecar.predecessor_event_id
        if predecessor is not None and (
            not isinstance(predecessor, str)
            or not predecessor
            or len(predecessor.encode("utf-8")) > 1024
        ):
            return None
        return sidecar
    except Exception:
        pass
    return None


def _trace_predecessor(sidecar: object) -> Optional[str]:
    """Read untrusted private wire metadata without affecting the RPC."""

    try:
        if not isinstance(sidecar, _WireTraceSidecar):
            return None
        value = sidecar.predecessor_event_id
        if isinstance(value, str) and value:
            return value
    except Exception:
        pass
    return None


def _safe_emit(
    sink: Optional[EventSink],
    name: str,
    *,
    component: str,
    cause_id: Optional[str] = None,
    **attributes: object
) -> Optional[str]:
    """Emit observably, tolerating even a badly behaved sink subclass."""

    if sink is None:
        return None
    try:
        event = sink.emit(
            name,
            component=component,
            cause_id=cause_id,
            attributes=attributes,
        )
        event_id = getattr(event, "event_id", None)
        if isinstance(event_id, str) and event_id:
            _advance_causal_cursor(event_id)
            return event_id
    except BaseException as exc:
        # Tracing must not perturb RPC control flow.  Process-control
        # exceptions are still swallowed here: unlike a user handler, a sink
        # is strictly observational and has no authority over the runtime.
        del exc
    return None


def _trace_scope(
    sink: Optional[EventSink], predecessor: Optional[str]
) -> ContextManager[None]:
    if sink is None and predecessor is None:
        return nullcontext()
    return causal_scope(predecessor, event_sink=sink)


def _reply_with_predecessor(
    reply: _WireReply, rpc_id: Optional[str], predecessor: Optional[str]
) -> object:
    if rpc_id is None:
        return reply
    return _TracedWireValue(
        reply, _WireTraceSidecar(rpc_id, predecessor)
    )


def _reply_failure_stage(exc: TransportError) -> str:
    if isinstance(exc, _PreSendTransportError):
        return "reply_encode"
    return "reply_send"


def _transport_error_type(exc: TransportError) -> str:
    if isinstance(exc, _PreSendTransportError):
        return type(exc.original).__name__
    return type(exc).__name__


def _send_server_reply(
    reply: _WireReply,
    sender: Callable[[object], None],
    *,
    sink: Optional[EventSink],
    component: str,
    handler: str,
    peer: str,
    rpc_id: Optional[str],
) -> None:
    """Send one reply, preserving the existing one-shot fallback.

    ``rpc_reply_sent`` marks a physical send-attempt boundary.  It is emitted
    before serialization so its event ID can travel in that very envelope;
    ``delivery=unknown`` is explicit because this event alone never proves the
    bytes reached the client.
    """

    predecessor = current_cause_id()
    send_event_id = _safe_emit(
        sink,
        "rpc_reply_sent",
        component=component,
        cause_id=predecessor,
        handler=handler,
        peer=peer,
        rpc_id=rpc_id or "",
        ok=reply.ok,
        delivery="unknown",
    )
    send_predecessor = send_event_id or predecessor
    traced_reply = _reply_with_predecessor(
        reply, rpc_id, send_predecessor
    )
    try:
        sender(traced_reply)
        return
    except TransportError as exc:
        failure_stage = _reply_failure_stage(exc)
        failed_event_id = _safe_emit(
            sink,
            "rpc_reply_failed",
            component=component,
            cause_id=send_predecessor,
            handler=handler,
            peer=peer,
            rpc_id=rpc_id or "",
            ok=reply.ok,
            stage=failure_stage,
            error_type=_transport_error_type(exc),
            delivery=(
                "not_sent"
                if failure_stage == "reply_encode"
                else "unknown"
            ),
        )
        failure_predecessor = failed_event_id or send_predecessor
        if not reply.ok or failure_stage != "reply_encode":
            return

        # A successful handler can produce an unserializable/oversized value.
        # Preserve the old best-effort error reply, but give this second
        # physical attempt its own trace event and sidecar.
        fallback_cause = (
            exc.original
            if isinstance(exc, _PreSendTransportError)
            else exc
        )
        fallback = _failure_reply(fallback_cause)
        fallback_event_id = _safe_emit(
            sink,
            "rpc_reply_sent",
            component=component,
            cause_id=failure_predecessor,
            handler=handler,
            peer=peer,
            rpc_id=rpc_id or "",
            ok=False,
            delivery="unknown",
        )
        fallback_predecessor = fallback_event_id or failure_predecessor
        try:
            sender(
                _reply_with_predecessor(
                    fallback, rpc_id, fallback_predecessor
                )
            )
        except TransportError as fallback_exc:
            _safe_emit(
                sink,
                "rpc_reply_failed",
                component=component,
                cause_id=fallback_predecessor,
                handler=handler,
                peer=peer,
                rpc_id=rpc_id or "",
                ok=False,
                stage=_reply_failure_stage(fallback_exc),
                error_type=_transport_error_type(fallback_exc),
                delivery="unknown",
            )


def _serve_wire_request(
    request_value: object,
    handlers: Mapping[str, Handler],
    sender: Callable[[object], None],
    *,
    sink: Optional[EventSink],
    component: str,
    peer: str,
) -> None:
    """Dispatch and reply to one decoded request without owning a socket.

    Keeping this boundary independent of ``socketserver`` makes the causal
    state machine directly testable without opening a listening socket.
    """

    sidecar = _trace_sidecar(request_value)
    request = (
        request_value.value
        if isinstance(request_value, _TracedWireValue)
        else request_value
    )
    if not isinstance(request, _WireRequest):
        raise TransportError("expected a request envelope")
    incoming = _trace_predecessor(sidecar)
    rpc_id = sidecar.rpc_id if sidecar is not None else None
    if rpc_id is None and sink is not None:
        rpc_id = uuid.uuid4().hex
    with _trace_scope(sink, incoming):
        received_event_id = _safe_emit(
            sink,
            "rpc_request_received",
            component=component,
            cause_id=incoming,
            handler=request.handler,
            peer=peer,
            rpc_id=rpc_id or "",
        )
        received_predecessor = received_event_id or incoming
        try:
            handler = handlers.get(request.handler)
            if handler is None:
                raise KeyError(
                    "unknown handler {!r}".format(request.handler)
                )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            _safe_emit(
                sink,
                "rpc_handler_failed",
                component=component,
                cause_id=received_predecessor,
                handler=request.handler,
                peer=peer,
                rpc_id=rpc_id or "",
                stage="dispatch",
                error_type=type(exc).__name__,
            )
            reply = _failure_reply(exc)
        else:
            started_event_id = _safe_emit(
                sink,
                "rpc_handler_started",
                component=component,
                cause_id=received_predecessor,
                handler=request.handler,
                peer=peer,
                rpc_id=rpc_id or "",
            )
            started_predecessor = (
                started_event_id or received_predecessor
            )
            try:
                value = handler(request.payload)
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                # EventSink.emit automatically follows any business event or
                # nested RPC emitted by the handler in this causal scope.
                _safe_emit(
                    sink,
                    "rpc_handler_failed",
                    component=component,
                    cause_id=current_cause_id() or started_predecessor,
                    handler=request.handler,
                    peer=peer,
                    rpc_id=rpc_id or "",
                    stage="handler",
                    error_type=type(exc).__name__,
                )
                reply = _failure_reply(exc)
            else:
                _safe_emit(
                    sink,
                    "rpc_handler_finished",
                    component=component,
                    cause_id=current_cause_id() or started_predecessor,
                    handler=request.handler,
                    peer=peer,
                    rpc_id=rpc_id or "",
                )
                reply = _WireReply(ok=True, value=value)

        _send_server_reply(
            reply,
            sender,
            sink=sink,
            component=component,
            handler=request.handler,
            peer=peer,
            rpc_id=rpc_id,
        )


class _RequestHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        server = self.server
        assert isinstance(server, _ThreadingTCPServer)
        self.request.settimeout(server.request_timeout)
        peer = _format_address(self.client_address)

        try:
            request_value = _receive(self.request, server.max_frame_bytes)
            request = (
                request_value.value
                if isinstance(request_value, _TracedWireValue)
                else request_value
            )
            if not isinstance(request, _WireRequest):
                raise TransportError("expected a request envelope")
        except BaseException as exc:
            # Do not let user exceptions escape into socketserver, which would
            # print them to stderr and close the connection without a reply.
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            reply = _failure_reply(exc)
            _send_server_reply(
                reply,
                lambda value: _send(
                    self.request, value, server.max_frame_bytes
                ),
                sink=server.event_sink,
                component=server.trace_component,
                handler="<transport>",
                peer=peer,
                rpc_id=None,
            )
            return

        _serve_wire_request(
            request_value,
            server.handlers,
            lambda value: _send(
                self.request, value, server.max_frame_bytes
            ),
            sink=server.event_sink,
            component=server.trace_component,
            peer=peer,
        )


class _ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True
    block_on_close = False

    def __init__(
        self,
        address: Address,
        handlers: Mapping[str, Handler],
        request_timeout: float,
        max_frame_bytes: int,
        event_sink: Optional[EventSink],
        trace_component: str,
    ) -> None:
        self.handlers = MappingProxyType(dict(handlers))
        self.request_timeout = request_timeout
        self.max_frame_bytes = max_frame_bytes
        self.event_sink = event_sink
        self.trace_component = trace_component
        super().__init__(address, _RequestHandler)


class TCPClient:
    """A client that performs one synchronous call per TCP connection."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        connect_timeout: float = 2.0,
        request_timeout: Optional[float] = 5.0,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        sink: Optional[EventSink] = None,
        event_sink: Optional[EventSink] = None,
        component: Optional[str] = None,
        trace_component: str = DEFAULT_TRACE_COMPONENT,
        deadline: Optional[float] = None,
    ) -> None:
        _validate_host(host)
        if not 1 <= port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        _validate_timeout("connect_timeout", connect_timeout)
        _validate_timeout(
            "request_timeout", request_timeout, allow_none=True
        )
        if deadline is not None and (
            not isinstance(deadline, (int, float))
            or isinstance(deadline, bool)
            or not math.isfinite(deadline)
        ):
            raise ValueError("deadline must be a finite monotonic timestamp or None")
        _validate_max_frame_bytes(max_frame_bytes)
        if sink is not None and event_sink is not None:
            raise ValueError("sink and event_sink are aliases; pass only one")
        if sink is not None:
            event_sink = sink
        if component is not None:
            trace_component = component
        if not isinstance(trace_component, str) or not trace_component:
            raise ValueError("trace_component must be a non-empty string")
        self._address = (host, port)
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self._max_frame_bytes = max_frame_bytes
        self._event_sink = event_sink
        self._trace_component = trace_component
        self._deadline = deadline

    @property
    def address(self) -> Address:
        return self._address

    def request(self, handler: str, payload: object) -> object:
        if not isinstance(handler, str) or not handler:
            raise ValueError("handler must be a non-empty string")
        peer = _format_address(self._address)
        event_sink = self._event_sink or current_event_sink()
        caller_predecessor = current_cause_id()
        tracing_active = (
            event_sink is not None or caller_predecessor is not None
        )
        rpc_id = uuid.uuid4().hex if tracing_active else None
        sent_event_id = _safe_emit(
            event_sink,
            "rpc_request_sent",
            component=self._trace_component,
            cause_id=caller_predecessor,
            handler=handler,
            peer=peer,
            rpc_id=rpc_id or "",
            delivery="unknown",
        )
        request_predecessor = sent_event_id or caller_predecessor
        request_value = _WireRequest(handler=handler, payload=payload)
        request = (
            _TracedWireValue(
                request_value,
                _WireTraceSidecar(rpc_id, request_predecessor),
            )
            if rpc_id is not None
            else request_value
        )
        # Serialize before connecting so local serialization failures do not
        # leave an accepted connection waiting for a frame.
        try:
            frame = _encode(request, self._max_frame_bytes)
        except TransportError as exc:
            _safe_emit(
                event_sink,
                "rpc_request_failed",
                component=self._trace_component,
                cause_id=request_predecessor,
                handler=handler,
                peer=peer,
                rpc_id=rpc_id or "",
                stage="request_encode",
                error_type=type(exc).__name__,
                delivery="not_sent",
            )
            raise
        try:
            try:
                connect_timeout = _remaining_timeout(
                    self._deadline, self._connect_timeout
                )
            except TransportTimeout:
                _safe_emit(
                    event_sink, "rpc_request_failed",
                    component=self._trace_component,
                    cause_id=request_predecessor, handler=handler, peer=peer,
                    rpc_id=rpc_id or "", stage="connect",
                    error_type="TransportConnectionTimeout",
                    delivery="not_sent",
                )
                raise
            connection = socket.create_connection(
                self._address, timeout=connect_timeout
            )
        except socket.timeout as exc:
            _safe_emit(
                event_sink,
                "rpc_request_failed",
                component=self._trace_component,
                cause_id=request_predecessor,
                handler=handler,
                peer=peer,
                rpc_id=rpc_id or "",
                stage="connect",
                error_type="TransportConnectionTimeout",
                delivery="not_sent",
            )
            raise TransportConnectionTimeout(
                "timed out while connecting to server"
            ) from exc
        except OSError as exc:
            _safe_emit(
                event_sink,
                "rpc_request_failed",
                component=self._trace_component,
                cause_id=request_predecessor,
                handler=handler,
                peer=peer,
                rpc_id=rpc_id or "",
                stage="connect",
                error_type="TransportConnectionError",
                delivery="not_sent",
            )
            raise TransportConnectionError("could not connect to server") from exc

        with connection:
            # ``None`` restores blocking mode after the bounded connect.  This
            # lets callers wait for a deliberately long-running handler
            # without weakening the connection-establishment bound.
            try:
                connection.settimeout(
                    _remaining_timeout(self._deadline, self._request_timeout)
                )
            except TransportTimeout:
                _safe_emit(
                    event_sink, "rpc_request_failed",
                    component=self._trace_component,
                    cause_id=request_predecessor, handler=handler, peer=peer,
                    rpc_id=rpc_id or "", stage="request_send",
                    error_type="TransportTimeout", delivery="not_sent",
                )
                raise
            try:
                connection.sendall(frame)
            except socket.timeout as exc:
                _safe_emit(
                    event_sink,
                    "rpc_request_failed",
                    component=self._trace_component,
                    cause_id=request_predecessor,
                    handler=handler,
                    peer=peer,
                    rpc_id=rpc_id or "",
                    stage="request_send",
                    error_type="TransportTimeout",
                    delivery="unknown",
                )
                raise TransportTimeout("timed out while sending request") from exc
            except OSError as exc:
                _safe_emit(
                    event_sink,
                    "rpc_request_failed",
                    component=self._trace_component,
                    cause_id=request_predecessor,
                    handler=handler,
                    peer=peer,
                    rpc_id=rpc_id or "",
                    stage="request_send",
                    error_type="TransportError",
                    delivery="unknown",
                )
                raise TransportError("failed while sending request") from exc
            try:
                if self._deadline is None:
                    # Preserve the legacy seam used by deterministic transport
                    # tests and non-deadline callers.
                    reply = _receive(connection, self._max_frame_bytes)
                else:
                    reply = _receive(
                        connection, self._max_frame_bytes,
                        deadline=self._deadline,
                        request_timeout=self._request_timeout,
                    )
            except TransportError as exc:
                stage = (
                    "reply_receive"
                    if not str(exc).startswith("could not deserialize")
                    else "reply_decode"
                )
                _safe_emit(
                    event_sink,
                    "rpc_request_failed",
                    component=self._trace_component,
                    cause_id=request_predecessor,
                    handler=handler,
                    peer=peer,
                    rpc_id=rpc_id or "",
                    stage=stage,
                    error_type=type(exc).__name__,
                    delivery="unknown",
                )
                raise

        reply_sidecar = _trace_sidecar(reply)
        if (
            reply_sidecar is not None
            and rpc_id is not None
            and reply_sidecar.rpc_id != rpc_id
        ):
            reply_sidecar = None
        reply_value = (
            reply.value if isinstance(reply, _TracedWireValue) else reply
        )
        if not isinstance(reply_value, _WireReply):
            _safe_emit(
                event_sink,
                "rpc_request_failed",
                component=self._trace_component,
                cause_id=request_predecessor,
                handler=handler,
                peer=peer,
                rpc_id=rpc_id or "",
                stage="reply_envelope",
                error_type="TransportError",
                delivery="unknown",
            )
            raise TransportError("expected a reply envelope")
        reply = reply_value
        reply_predecessor = (
            _trace_predecessor(reply_sidecar) or request_predecessor
        )
        _safe_emit(
            event_sink,
            "rpc_reply_received",
            component=self._trace_component,
            cause_id=reply_predecessor,
            handler=handler,
            peer=peer,
            rpc_id=rpc_id or "",
            ok=reply.ok,
            error_type=reply.error_type if not reply.ok else "",
        )
        if not reply.ok:
            raise RemoteCallError(
                handler=handler,
                remote_type=reply.error_type,
                message=reply.error_message,
                traceback_text=reply.traceback_text,
            )
        return reply.value


class TCPServer:
    """A stoppable, threaded loopback request/reply server.

    The supplied mapping is copied at construction time.  This makes dispatch
    explicit and avoids races from mutating a global handler registry.
    """

    def __init__(
        self,
        handlers: Mapping[str, Handler],
        *,
        host: str = LOOPBACK_HOST,
        port: int = 0,
        request_timeout: float = 5.0,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        sink: Optional[EventSink] = None,
        event_sink: Optional[EventSink] = None,
        component: Optional[str] = None,
        trace_component: str = DEFAULT_TRACE_COMPONENT,
    ) -> None:
        _validate_host(host)
        if not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        _validate_timeout("request_timeout", request_timeout)
        _validate_max_frame_bytes(max_frame_bytes)
        if sink is not None and event_sink is not None:
            raise ValueError("sink and event_sink are aliases; pass only one")
        if sink is not None:
            event_sink = sink
        if component is not None:
            trace_component = component
        if not isinstance(trace_component, str) or not trace_component:
            raise ValueError("trace_component must be a non-empty string")

        copied_handlers = dict(handlers)
        for name, handler in copied_handlers.items():
            if not isinstance(name, str) or not name:
                raise ValueError("handler names must be non-empty strings")
            if not callable(handler):
                raise TypeError("handler {!r} is not callable".format(name))

        self._server = _ThreadingTCPServer(
            (host, port),
            copied_handlers,
            request_timeout,
            max_frame_bytes,
            event_sink,
            trace_component,
        )
        bound_host, bound_port = self._server.server_address[:2]
        self._address = (str(bound_host), int(bound_port))
        self._state_lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._closed = False

    @property
    def address(self) -> Address:
        return self._address

    @property
    def is_running(self) -> bool:
        with self._state_lock:
            return self._thread is not None and self._thread.is_alive()

    def start(self) -> Address:
        with self._state_lock:
            if self._closed:
                raise RuntimeError("a stopped TCPServer cannot be restarted")
            if self._thread is not None:
                return self._address
            thread = threading.Thread(
                target=self._server.serve_forever,
                kwargs={"poll_interval": 0.05},
                name="miniray-tcp-server",
                daemon=True,
            )
            self._thread = thread
            try:
                thread.start()
            except BaseException:
                # ``socketserver.shutdown`` waits for serve_forever's private
                # shutdown event.  If Thread.start itself failed there is no
                # serving loop to publish that event, so leave stop() an
                # accurate "bound but never started" state: it will close
                # the listening socket without calling shutdown or join.
                self._thread = None
                raise
        return self._address

    def stop(self, join_timeout: float = 2.0) -> None:
        _validate_timeout("join_timeout", join_timeout)
        with self._state_lock:
            if self._closed:
                return
            thread = self._thread
            self._closed = True

        if thread is not None:
            self._server.shutdown()
        self._server.server_close()
        if thread is not None and thread is not threading.current_thread():
            thread.join(join_timeout)

        with self._state_lock:
            self._thread = None

    def __enter__(self) -> "TCPServer":
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()


def request(
    address: Address,
    handler: str,
    payload: object,
    *,
    connect_timeout: float = 2.0,
    request_timeout: Optional[float] = 5.0,
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    sink: Optional[EventSink] = None,
    event_sink: Optional[EventSink] = None,
    component: Optional[str] = None,
    trace_component: str = DEFAULT_TRACE_COMPONENT,
    deadline: Optional[float] = None,
) -> object:
    """Convenience wrapper for a single synchronous request."""

    client = TCPClient(
        address[0],
        address[1],
        connect_timeout=connect_timeout,
        request_timeout=request_timeout,
        max_frame_bytes=max_frame_bytes,
        sink=sink,
        event_sink=event_sink,
        component=component,
        trace_component=trace_component,
        deadline=deadline,
    )
    return client.request(handler, payload)


__all__ = [
    "Address",
    "ConnectionClosed",
    "DEFAULT_TRACE_COMPONENT",
    "DEFAULT_MAX_FRAME_BYTES",
    "FrameTooLarge",
    "Handler",
    "LOOPBACK_HOST",
    "RemoteCallError",
    "TCPClient",
    "TCPServer",
    "TransportConnectionError",
    "TransportConnectionTimeout",
    "TransportError",
    "TransportTimeout",
    "request",
]
