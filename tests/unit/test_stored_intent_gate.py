"""Unified output-gate wire contracts; only three socketpair tests are L1.

The historical filename remains stable for focused test selection. Pure cases
use value objects and fake sockets/clocks, never a runtime, thread or real wait.
"""

from __future__ import annotations

import hashlib
import socket
import struct
import threading
import time
from dataclasses import replace

import pytest

import miniray as ray
from miniray import protocol, publication_gate as gates
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputValue,
)
from miniray.task_outputs import TaskExecution


# No module-level unit marker: the three real socketpair cases stay opt-in.
@pytest.fixture(autouse=True)
def _no_runtime_in_pure_cases(request, monkeypatch):
    if request.node.get_closest_marker("loopback_smoke") is not None:
        return

    def forbidden(*_args, **_kwargs):
        pytest.fail("pure output gate contract attempted runtime work")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "socketpair", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _manifest(*, task_byte=2, lease_byte=4, attempt_number=3, stored=False):
    task = TaskID(bytes((task_byte,)) * 16)
    attempt = AttemptID(task, attempt_number)
    execution = (TaskExecution(attempt))
    publication = OutputPublicationID(LeaseID(bytes((lease_byte,)) * 16), execution)
    header = OutputPublicationHeader(
        publication, JobID(b"J" * 16), WorkerID(b"E" * 16), WorkerID(b"O" * 16),
        OutputPublicationNodeIncarnation(NodeID(b"N" * 16), 4321, 7),
    )
    slots = tuple(
        (OutputValue(protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE, 1, hashlib.sha256(bytes((index,))).hexdigest())) for index, object_id in enumerate(((publication.object_id,)))
    )
    return OutputPublicationManifest.create(header, (slots[0]))


def _arrival(*, phase=None, **manifest_options):
    if phase is None:
        phase = gates.OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY
    return gates.OutputPublicationGateArrival.from_manifest(_manifest(**manifest_options), phase)


@pytest.mark.loopback_smoke
def test_arrival_frame_round_trips_exact_incarnation_and_publication() -> None:
    for phase in gates.OutputPublicationGatePhase:
        arrival = _arrival(phase=phase)
        sender, receiver = socket.socketpair()
        try:
            sender.settimeout(1.0)
            receiver.settimeout(1.0)
            sender.sendall(arrival.to_bytes())
            assert gates.recv_output_publication_gate_arrival(receiver) == arrival
        finally:
            sender.close()
            receiver.close()


@pytest.mark.loopback_smoke
def test_stored_arrival_frame_preserves_single_output_and_attempt() -> None:
    arrival = _arrival(
        stored=True, attempt_number=7,
        phase=gates.OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK,
    )
    sender, receiver = socket.socketpair()
    try:
        sender.settimeout(1.0)
        receiver.settimeout(1.0)
        sender.sendall(arrival.to_bytes())
        observed = gates.recv_output_publication_gate_arrival(receiver)
        assert observed == arrival
        execution = observed.publication_id.execution
        assert type(execution) is TaskExecution
        assert execution.attempt_id.attempt_number == 7
        assert ((execution.object_id.return_index,)) == (0,)
    finally:
        sender.close()
        receiver.close()


@pytest.mark.loopback_smoke
def test_receiver_rejects_truncated_frame_before_peer_exit() -> None:
    payload = _arrival().to_bytes()
    sender, receiver = socket.socketpair()
    try:
        sender.settimeout(1.0)
        receiver.settimeout(1.0)
        sender.sendall(payload[:-1])
        sender.shutdown(socket.SHUT_WR)
        with pytest.raises(RuntimeError, match="frame"):
            gates.recv_output_publication_gate_arrival(receiver)
    finally:
        sender.close()
        receiver.close()


@pytest.mark.unit
@pytest.mark.parametrize("phase", tuple(gates.OutputPublicationGatePhase))
@pytest.mark.parametrize("stored", (False, True))
def test_fixed_frame_round_trip_preserves_single_execution(phase, stored):
    manifest = _manifest(stored=stored)
    arrival = gates.OutputPublicationGateArrival.from_manifest(manifest, phase)
    frame = arrival.to_bytes()
    restored = gates.OutputPublicationGateArrival.from_bytes(frame)
    assert restored == arrival
    assert restored.publication_id == manifest.publication_id
    assert restored.manifest_digest == manifest.manifest_digest
    assert len(frame) == struct.calcsize("!8s16sQQ16s16sQ32sB") == 113
    assert type(restored.publication_id.execution) is TaskExecution
    assert ((restored.publication_id.object_id,))[0].return_index == 0
    assert restored.node_id == manifest.header.node_incarnation.node_id
    assert restored.node_pid == manifest.header.node_incarnation.node_pid
    assert restored.registration_epoch == manifest.header.node_incarnation.registration_epoch


@pytest.mark.unit
def test_distinct_task_attempt_or_lease_never_aliases_one_publication():
    full = _arrival()
    for other in (_arrival(task_byte=5), _arrival(lease_byte=6), _arrival(attempt_number=4)):
        assert full.publication_id != other.publication_id
        assert full.to_bytes() != other.to_bytes()
        assert gates.OutputPublicationGateArrival.from_bytes(other.to_bytes()) == other


@pytest.mark.unit
def test_arrival_frame_rejects_corruption_and_drifted_identity() -> None:
    arrival = _arrival()
    payload = arrival.to_bytes()
    with pytest.raises(ValueError, match="size"):
        gates.OutputPublicationGateArrival.from_bytes(payload[:-1])
    with pytest.raises(ValueError, match="size"):
        gates.OutputPublicationGateArrival.from_bytes(payload + b"x")
    with pytest.raises(ValueError, match="magic"):
        gates.OutputPublicationGateArrival.from_bytes(b"BADMAGIC" + payload[8:])
    with pytest.raises(ValueError, match="node_pid"):
        replace(arrival, node_pid=0)
    with pytest.raises(ValueError, match="registration_epoch"):
        replace(arrival, registration_epoch=0)
    with pytest.raises(TypeError):
        replace(arrival, publication_id=object())


@pytest.mark.unit
@pytest.mark.parametrize("field,value", ((2, 0), (3, 0), (8, 3), (8, 255)))
def test_frame_rejects_invalid_incarnation_or_phase(field, value):
    frame = struct.Struct("!8s16sQQ16s16sQ32sB")
    values = list(frame.unpack(_arrival().to_bytes()))
    values[field] = value
    with pytest.raises(ValueError):
        gates.OutputPublicationGateArrival.from_bytes(frame.pack(*values))


@pytest.mark.unit
@pytest.mark.parametrize("digest", ("", "11" * 31, "g" * 64, b"x" * 32))
def test_arrival_requires_a_complete_metadata_digest(digest):
    with pytest.raises((TypeError, ValueError)):
        replace(_arrival(), manifest_digest=digest)


@pytest.mark.unit
@pytest.mark.parametrize("value,error", ((True, TypeError), (-1, ValueError), ("0", TypeError)))
def test_config_rejects_invalid_node_index(value, error):
    with pytest.raises(error, match="node_index"):
        gates.OutputPublicationGateConfig(value, ("127.0.0.1", 20000))


@pytest.mark.unit
@pytest.mark.parametrize("address", (
    ("", 20000), ("example.com", 20000), ("0.0.0.0", 20000),
    ("127.0.0.1", 0), ("127.0.0.1", True), "127.0.0.1:20000",
))
def test_config_rejects_unbound_address(address):
    with pytest.raises(ValueError, match="address"):
        gates.OutputPublicationGateConfig(0, address)


@pytest.mark.unit
def test_config_rejects_invalid_phase():
    with pytest.raises(TypeError, match="phase"):
        gates.OutputPublicationGateConfig(0, ("127.0.0.1", 20000), "AFTER_PROMOTIONS_ACK")
    with pytest.raises(TypeError, match="phase"):
        replace(_arrival(), phase="AFTER_INTENT_ACK")


@pytest.mark.unit
@pytest.mark.parametrize("timeout", (True, 0, -1, 11, float("inf"), float("nan"), "10"))
def test_config_rejects_invalid_timeout(timeout):
    with pytest.raises(ValueError, match="timeout_seconds"):
        gates.OutputPublicationGateConfig(0, ("127.0.0.1", 20000), timeout_seconds=timeout)


@pytest.mark.unit
def test_init_rejects_wrong_gate_type_before_spawning():
    with pytest.raises(TypeError, match="_test_output_publication_gate"):
        ray.init(_test_output_publication_gate=object(), enable_tracing=False)
    with pytest.raises(ValueError, match="configured node"):
        ray.init(_test_output_publication_gate=gates.OutputPublicationGateConfig(
            1, ("127.0.0.1", 20000)
        ), enable_tracing=False)
    assert not ray.is_initialized()


class _Receiver:
    def __init__(self, chunks, *, timeout=1.0, clock=None, advance=0.0):
        self.chunks = list(chunks)
        self.timeout = timeout
        self.clock = clock
        self.advance = advance
        self.timeouts = []
        self.received = []

    def gettimeout(self):
        return self.timeout

    def settimeout(self, value):
        assert value > 0
        self.timeouts.append(value)

    def recv(self, size):
        self.received.append(size)
        chunk = self.chunks.pop(0)
        assert len(chunk) <= size
        if self.clock is not None:
            self.clock[0] += self.advance
        return chunk


@pytest.mark.unit
def test_receiver_requires_a_real_socket_contract_and_positive_bound(monkeypatch):
    monkeypatch.setattr(gates.socket, "socket", _Receiver)
    with pytest.raises(TypeError, match="socket"):
        gates.recv_output_publication_gate_arrival(object())
    for timeout in (None, 0, -1):
        connection = _Receiver([], timeout=timeout)
        with pytest.raises(ValueError, match="timeout"):
            gates.recv_output_publication_gate_arrival(connection)
        assert connection.received == []


@pytest.mark.unit
def test_receiver_uses_one_deadline_for_fragmented_frame(monkeypatch):
    arrival = _arrival(stored=True)
    frame = arrival.to_bytes()
    now = [100.0]
    connection = _Receiver((frame[:7], frame[7:23], frame[23:]), clock=now, advance=0.2)
    monkeypatch.setattr(gates.socket, "socket", _Receiver)
    monkeypatch.setattr(gates.time, "monotonic", lambda: now[0])
    assert gates.recv_output_publication_gate_arrival(connection) == arrival
    assert connection.timeouts == pytest.approx([1.0, 0.8, 0.6])
    assert connection.received == [len(frame), len(frame) - 7, len(frame) - 23]


@pytest.mark.unit
def test_receiver_does_not_reset_deadline_after_partial_frame(monkeypatch):
    frame = _arrival().to_bytes()
    now = [100.0]
    connection = _Receiver((frame[:1], frame[1:]), timeout=0.25, clock=now, advance=0.25)
    monkeypatch.setattr(gates.socket, "socket", _Receiver)
    monkeypatch.setattr(gates.time, "monotonic", lambda: now[0])
    with pytest.raises(TimeoutError, match="deadline"):
        gates.recv_output_publication_gate_arrival(connection)
    assert connection.timeouts == [0.25]
    assert connection.received == [len(frame)]
