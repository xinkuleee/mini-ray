"""One private, bounded checkpoint for unified output fault acceptance.

An unconfigured runtime never constructs this gate. A configured Node selects
its first publication at owner registration and pauses at one semantic phase. Complete and
outcome delivery share the same gate, so neither can leak the completed bytes
before a no-delivery crash. Frames contain exact metadata only.

This is test observation, not a publication authority. No caller may hold its
journal, resource or ownership lock while a checkpoint performs I/O or waits.
"""

from __future__ import annotations

import math
import socket
import struct
import threading
import time
from dataclasses import dataclass, replace
from enum import Enum
from typing import Callable, Optional, Tuple

from .ids import AttemptID, LeaseID, NodeID, TaskID
from .output_publication import (
    OutputPublicationID, OutputPublicationManifest, _checksum, _opaque, _uint,
)
from .task_outputs import TaskExecution


class OutputPublicationGatePhase(str, Enum):
    AFTER_OWNER_REGISTER_ACK = "AFTER_OWNER_REGISTER_ACK"
    AFTER_PROMOTIONS_ACK = "AFTER_PROMOTIONS_ACK"
    AFTER_COMPLETE_BEFORE_TASK_REPLY = "AFTER_COMPLETE_BEFORE_TASK_REPLY"
    BEFORE_GRAPH_PREPARE = "BEFORE_GRAPH_PREPARE"
    AFTER_GRAPH_PREPARE_REPLY = "AFTER_GRAPH_PREPARE_REPLY"
    BEFORE_TERMINAL_REPORT = "BEFORE_TERMINAL_REPORT"
    AFTER_TERMINAL_ACCEPTED_BEFORE_ACK = "AFTER_TERMINAL_ACCEPTED_BEFORE_ACK"


class GraphReservationOutcome(str, Enum):
    UNOBSERVED = "UNOBSERVED"
    ACCEPTED = "ACCEPTED"
    CYCLE = "CYCLE"
    REJECTED = "REJECTED"


OUTPUT_PUBLICATION_GATE_RELEASE = b"G"
_MAGIC = b"MROPG003"
# The output is always ObjectID(task_id, 0), so the frame needs no slot scope.
_FRAME = struct.Struct("!8s16sQQ16s16sQ32sBB")
_PHASES = tuple(OutputPublicationGatePhase)
_GRAPH_OUTCOMES = tuple(GraphReservationOutcome)


@dataclass(frozen=True)
class OutputPublicationGateConfig:
    node_index: int
    address: Tuple[str, int]
    phase: OutputPublicationGatePhase = OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY
    timeout_seconds: float = 10.0
    graph_reservation_barrier: bool = False
    attempt_number: Optional[int] = None

    def __post_init__(self) -> None:
        if type(self.node_index) is not int:
            raise TypeError("output gate node_index must be an integer")
        if self.node_index < 0:
            raise ValueError("output gate node_index must be non-negative")
        if (type(self.address) is not tuple or len(self.address) != 2
                or self.address[0] != "127.0.0.1" or type(self.address[1]) is not int
                or not 1 <= self.address[1] <= 65535):
            raise ValueError("output gate address must be a bound loopback endpoint")
        if type(self.phase) is not OutputPublicationGatePhase:
            raise TypeError("output gate phase must be an OutputPublicationGatePhase")
        timeout = self.timeout_seconds
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or not 0 < timeout <= 10.0):
            raise ValueError("output gate timeout_seconds must be in (0, 10]")
        object.__setattr__(self, "timeout_seconds", float(timeout))
        if type(self.graph_reservation_barrier) is not bool:
            raise TypeError("graph reservation barrier must be boolean")
        if self.attempt_number is not None:
            _uint(self.attempt_number, "gate attempt_number")
        if self.graph_reservation_barrier and self.attempt_number != 1:
            raise ValueError("graph reservation barrier selects reconstruction attempt one")


@dataclass(frozen=True)
class OutputPublicationGateArrival:
    node_id: NodeID
    node_pid: int
    registration_epoch: int
    publication_id: OutputPublicationID
    manifest_digest: str
    phase: OutputPublicationGatePhase
    graph_outcome: GraphReservationOutcome = GraphReservationOutcome.UNOBSERVED

    def __post_init__(self) -> None:
        object.__setattr__(self, "node_id", _opaque(self.node_id, NodeID, "output gate Node"))
        _uint(self.node_pid, "node_pid", positive=True)
        _uint(self.registration_epoch, "registration_epoch", positive=True)
        if type(self.publication_id) is not OutputPublicationID:
            raise TypeError("output gate requires an OutputPublicationID")
        object.__setattr__(self, "publication_id", replace(self.publication_id))
        object.__setattr__(self, "manifest_digest", _checksum(self.manifest_digest, "manifest_digest"))
        if type(self.phase) is not OutputPublicationGatePhase:
            raise TypeError("output gate arrival requires an exact phase")
        if type(self.graph_outcome) is not GraphReservationOutcome:
            raise TypeError("graph gate requires an exact observed outcome")
        if ((self.phase is OutputPublicationGatePhase.AFTER_GRAPH_PREPARE_REPLY)
                != (self.graph_outcome is not GraphReservationOutcome.UNOBSERVED)):
            raise ValueError("only graph reply checkpoint carries an observed outcome")

    @property
    def graph_accepted(self):
        return None if self.graph_outcome is GraphReservationOutcome.UNOBSERVED else self.graph_outcome is GraphReservationOutcome.ACCEPTED

    @property
    def graph_cycle_rejected(self):
        return self.graph_outcome is GraphReservationOutcome.CYCLE

    @classmethod
    def from_manifest(cls, manifest: OutputPublicationManifest, phase: OutputPublicationGatePhase,
                      graph_outcome: GraphReservationOutcome = GraphReservationOutcome.UNOBSERVED):
        if type(manifest) is not OutputPublicationManifest:
            raise TypeError("output gate requires an OutputPublicationManifest")
        manifest = replace(manifest)
        node = manifest.header.node_incarnation
        return cls(node.node_id, node.node_pid, node.registration_epoch,
                   manifest.publication_id, manifest.manifest_digest, phase, graph_outcome)

    def to_bytes(self) -> bytes:
        arrival = replace(self)
        publication = arrival.publication_id
        return _FRAME.pack(
            _MAGIC, bytes(arrival.node_id), arrival.node_pid, arrival.registration_epoch,
            bytes(publication.lease_id), bytes(publication.task_id), publication.attempt_id.attempt_number,
            bytes.fromhex(arrival.manifest_digest), _PHASES.index(arrival.phase), _GRAPH_OUTCOMES.index(arrival.graph_outcome),
        )

    @classmethod
    def from_bytes(cls, data: bytes):
        if type(data) is not bytes or len(data) != _FRAME.size:
            raise ValueError("output gate frame has the wrong type or size")
        magic, node, pid, epoch, lease, task, attempt, digest, phase, outcome = _FRAME.unpack(data)
        if magic != _MAGIC:
            raise ValueError("output gate frame has the wrong magic")
        if phase >= len(_PHASES):
            raise ValueError("output gate frame has an invalid phase")
        if outcome >= len(_GRAPH_OUTCOMES):
            raise ValueError("output gate frame has an invalid graph outcome")
        task_id = TaskID(task)
        attempt_id = AttemptID(task_id, attempt)
        execution = TaskExecution(attempt_id)
        return cls(NodeID(node), pid, epoch, OutputPublicationID(LeaseID(lease), execution),
                   digest.hex(), _PHASES[phase], _GRAPH_OUTCOMES[outcome])


def recv_output_publication_gate_arrival(connection: socket.socket) -> OutputPublicationGateArrival:
    """Receive one fixed frame using a single absolute timeout budget."""
    if not isinstance(connection, socket.socket):
        raise TypeError("output gate receiver requires a socket")
    timeout = connection.gettimeout()
    if timeout is None or timeout <= 0:
        raise ValueError("output gate receiver requires a positive socket timeout")
    deadline = time.monotonic() + timeout
    chunks = bytearray()
    while len(chunks) < _FRAME.size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("output gate frame exceeded its receive deadline")
        connection.settimeout(remaining)
        chunk = connection.recv(_FRAME.size - len(chunks))
        if not chunk:
            raise RuntimeError("publisher exited before completing its gate frame")
        chunks.extend(chunk)
    return OutputPublicationGateArrival.from_bytes(bytes(chunks))


class OutputPublicationGate:
    """One-shot phase selector; leader/followers share the same deadline."""

    def __init__(self, config: OutputPublicationGateConfig) -> None:
        if type(config) is not OutputPublicationGateConfig:
            raise TypeError("output publication gate requires typed configuration")
        self.config = replace(config)
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._selected: Optional[OutputPublicationGateArrival] = None
        self._started = False
        self._deadline: Optional[float] = None
        self._error: Optional[str] = None
        self._graph_gates = ({phase: OutputPublicationGate(replace(self.config,
            graph_reservation_barrier=False, phase=phase)) for phase in (
                OutputPublicationGatePhase.BEFORE_GRAPH_PREPARE,
                OutputPublicationGatePhase.AFTER_GRAPH_PREPARE_REPLY)}
            if self.config.graph_reservation_barrier else None)

    def checkpoint(self, arrival: OutputPublicationGateArrival,
                   ensure_terminal: Optional[Callable[[float], None]] = None) -> None:
        if type(arrival) is not OutputPublicationGateArrival:
            raise TypeError("output gate requires typed arrival")
        arrival = replace(arrival)
        if (self.config.attempt_number is not None
                and arrival.publication_id.attempt_id.attempt_number != self.config.attempt_number):
            return
        if self._graph_gates is not None:
            gate = self._graph_gates.get(arrival.phase)
            if gate is not None:
                gate.checkpoint(arrival, ensure_terminal)
            return
        if ensure_terminal is not None and not callable(ensure_terminal):
            raise TypeError("output gate preparation must be callable")
        leader = False
        with self._lock:
            if self._selected is None:
                self._selected = arrival
            elif self._selected.publication_id != arrival.publication_id:
                return
            elif replace(self._selected, phase=arrival.phase, graph_outcome=arrival.graph_outcome) != arrival:
                raise RuntimeError("output gate publication incarnation or manifest changed")
            if arrival.phase is not self.config.phase:
                return
            if not self._started:
                self._started = True
                self._deadline = time.monotonic() + self.config.timeout_seconds
                leader = True
            deadline = self._deadline
        assert deadline is not None
        if not leader:
            if not self._done.is_set() and not self._done.wait(max(0.0, deadline - time.monotonic())):
                raise TimeoutError("output gate leader did not finish before the shared deadline")
            if self._error is not None:
                raise RuntimeError(self._error)
            return

        def remaining() -> float:
            value = deadline - time.monotonic()
            if value <= 0:
                raise TimeoutError("output publication gate exceeded its deadline")
            return value

        try:
            if ensure_terminal is not None:
                ensure_terminal(deadline)
            with socket.create_connection(self.config.address, timeout=remaining()) as connection:
                connection.settimeout(remaining())
                connection.sendall(arrival.to_bytes())
                connection.settimeout(remaining())
                if connection.recv(1) != OUTPUT_PUBLICATION_GATE_RELEASE:
                    raise RuntimeError("output gate closed without exact release")
        except BaseException as exc:
            with self._lock:
                self._error = "output publication test gate failed: {}".format(exc)
            raise
        finally:
            self._done.set()


__all__ = [
    "OUTPUT_PUBLICATION_GATE_RELEASE", "OutputPublicationGateConfig",
    "OutputPublicationGatePhase", "OutputPublicationGateArrival",
    "GraphReservationOutcome",
    "OutputPublicationGate", "recv_output_publication_gate_arrival",
]
