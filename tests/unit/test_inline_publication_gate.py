"""Pure unified gate selection/deadline and Node delivery contracts.

The historical filename is retained, but no legacy INLINE protocol is used.
Fake sockets, clocks and completion events prevent all real waits and I/O.
"""

from dataclasses import replace
import socket
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol, publication_gate as gates
from miniray.ids import NodeID
from miniray.output_publication_journal import (
    OutputPublicationAdoptionProof, OutputPublicationJournalState,
)
from tests.unit.test_output_publication_node_server import _node
from tests.unit.test_stored_intent_gate import _arrival


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure output gate attempted real runtime work")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "socketpair", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _Completion:
    def __init__(self, *, on_wait=None):
        self.finished = False
        self.on_wait = on_wait
        self.waits = []

    def is_set(self):
        return self.finished

    def set(self):
        self.finished = True

    def wait(self, timeout):
        self.waits.append(timeout)
        assert timeout >= 0
        if self.on_wait is not None:
            self.on_wait(timeout)
        return self.finished


class _Connection:
    def __init__(self, events, *, release=gates.OUTPUT_PUBLICATION_GATE_RELEASE,
                 clock=None, advance=0.0, probe=None):
        self.events = events
        self.release = release
        self.clock = clock
        self.advance = advance
        self.probe = probe
        self.timeouts = []

    def _check(self):
        if self.probe is not None:
            self.probe()

    def __enter__(self):
        self._check()
        return self

    def __exit__(self, *_args):
        self._check()
        self.events.append("closed")

    def settimeout(self, timeout):
        self._check()
        assert 0 < timeout <= 10.0
        self.timeouts.append(timeout)

    def sendall(self, data):
        self._check()
        arrival = gates.OutputPublicationGateArrival.from_bytes(data)
        assert arrival.publication_id.object_id.return_index == 0
        assert arrival.publication_id.execution.attempt_id == arrival.publication_id.attempt_id
        self.events.append(arrival)
        if self.clock is not None:
            self.clock[0] += self.advance

    def recv(self, size):
        self._check()
        assert size == 1
        self.events.append("released")
        return self.release


def _gate(*, phase=gates.OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY,
          timeout=10.0):
    gate = gates.OutputPublicationGate(gates.OutputPublicationGateConfig(
        0, ("127.0.0.1", 21000), phase, timeout_seconds=timeout,
    ))
    gate._done = _Completion()
    return gate


def _selector_is_unlocked(gate):
    assert gate._lock.acquire(blocking=False)
    gate._lock.release()


@pytest.mark.parametrize("phase", tuple(gates.OutputPublicationGatePhase))
def test_phase_selector_binds_first_publication_before_its_pause(monkeypatch, phase):
    gate = _gate(phase=phase)
    phases = tuple(gates.OutputPublicationGatePhase)
    first = _arrival(phase=phases[0])
    events = []
    monkeypatch.setattr(gates.socket, "create_connection", lambda *_a, **_k: _Connection(events))
    gate.checkpoint(first)
    if phase is not phases[0]:
        assert events == [] and not gate._done.is_set()
        assert gate._selected.publication_id == first.publication_id
        assert gate._deadline is None
    gate.checkpoint(_arrival(phase=phase, lease_byte=5), lambda _: pytest.fail("gated another publication"))
    for current in phases[1:]:
        gate.checkpoint(replace(first, phase=current))
    assert events == [replace(first, phase=phase), "released", "closed"]
    assert gate._done.is_set()


def test_terminal_precedes_arrival_and_one_gate_does_not_pause_new_publication(monkeypatch):
    gate = _gate()
    events = []
    first = _arrival()
    monkeypatch.setattr(gates.socket, "create_connection", lambda *_a, **_k: _Connection(events))
    gate.checkpoint(first, lambda _deadline: events.append("terminal"))
    gate.checkpoint(first, lambda _deadline: pytest.fail("exact replay repeated terminal"))
    gate.checkpoint(_arrival(lease_byte=5), lambda _deadline: pytest.fail("next publication was gated"))
    assert events == ["terminal", first, "released", "closed"]
    assert gate._done.is_set() and gate._done.waits == []


@pytest.mark.parametrize("changed", ("node_id", "node_pid", "registration_epoch", "manifest_digest"))
@pytest.mark.parametrize("completed", (False, True))
def test_selected_identity_rebinding_is_rejected_even_at_another_phase(monkeypatch, changed, completed):
    gate = _gate()
    first = _arrival(phase=gates.OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK)
    monkeypatch.setattr(gates.socket, "create_connection", lambda *_a, **_k: _Connection([]))
    gate.checkpoint(first)
    if completed:
        gate.checkpoint(replace(first, phase=gate.config.phase))
    values = {
        "node_id": NodeID(b"X" * 16), "node_pid": first.node_pid + 1,
        "registration_epoch": first.registration_epoch + 1,
        "manifest_digest": "00" * 32 if first.manifest_digest != "00" * 32 else "11" * 32,
    }
    changed_arrival = replace(first, phase=gates.OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK,
                              **{changed: values[changed]})
    with pytest.raises(RuntimeError, match="incarnation|manifest"):
        gate.checkpoint(changed_arrival)
    assert gate._selected == first


def test_execution_attempt_is_part_of_one_shot_publication_identity(monkeypatch):
    gate = _gate()
    first = _arrival(phase=gates.OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK)
    gate.checkpoint(first)
    # Same Task/Lease but another attempt is a different publication.
    target = _arrival(attempt_number=4)
    gate.checkpoint(target, lambda _: pytest.fail("new attempt replaced selected publication"))
    assert not gate._done.is_set()
    events = []
    monkeypatch.setattr(gates.socket, "create_connection", lambda *_a, **_k: _Connection(events))
    gate.checkpoint(replace(first, phase=gate.config.phase))
    assert events[0].publication_id == first.publication_id


def test_failed_terminal_has_no_socket_effect_and_failed_replay_is_exact():
    gate = _gate()
    arrival = _arrival()

    def fail(_deadline):
        raise RuntimeError("terminal not acknowledged")

    with pytest.raises(RuntimeError, match="not acknowledged"):
        gate.checkpoint(arrival, fail)
    assert gate._done.is_set()
    with pytest.raises(RuntimeError, match="not acknowledged"):
        gate.checkpoint(arrival, lambda _: pytest.fail("failed gate retried effects"))
    assert gate._done.waits == []


@pytest.mark.parametrize("release", (b"", b"X", b"GG"))
def test_only_exact_release_finishes_gate_successfully(monkeypatch, release):
    gate = _gate()
    events = []
    monkeypatch.setattr(gates.socket, "create_connection", lambda *_a, **_k: _Connection(events, release=release))
    with pytest.raises(RuntimeError, match="exact release"):
        gate.checkpoint(_arrival())
    assert gate._done.is_set() and events[-1] == "closed"
    with pytest.raises(RuntimeError, match="exact release"):
        gate.checkpoint(_arrival())
    assert len(events) == 3


def test_checkpoint_releases_selector_lock_across_terminal_and_socket_callbacks(monkeypatch):
    gate = _gate()
    events = []

    def check():
        _selector_is_unlocked(gate)

    def terminal(_deadline):
        check()
        events.append("terminal")

    def connect(*_args, **_kwargs):
        check()
        return _Connection(events, probe=check)

    monkeypatch.setattr(gates.socket, "create_connection", connect)
    arrival = _arrival()
    gate.checkpoint(arrival, terminal)
    assert events == ["terminal", arrival, "released", "closed"]


def test_terminal_connect_and_send_consume_one_absolute_deadline(monkeypatch):
    gate = _gate(timeout=1.0)
    now = [100.0]
    monkeypatch.setattr(gates.time, "monotonic", lambda: now[0])
    connection = _Connection([], clock=now, advance=0.2)
    connect_timeouts = []

    def terminal(deadline):
        assert deadline == 101.0
        now[0] += 0.3

    def connect(address, timeout):
        assert address == gate.config.address
        connect_timeouts.append(timeout)
        now[0] += 0.1
        return connection

    monkeypatch.setattr(gates.socket, "create_connection", connect)
    gate.checkpoint(_arrival(), terminal)
    assert connect_timeouts == pytest.approx([0.7])
    assert connection.timeouts == pytest.approx([0.6, 0.4])
    assert gate._deadline == 101.0


def test_terminal_proof_consumes_the_same_gate_deadline(monkeypatch):
    gate = _gate(timeout=0.25)
    now = [100.0]
    monkeypatch.setattr(gates.time, "monotonic", lambda: now[0])

    def terminal(deadline):
        assert deadline == 100.25
        now[0] = deadline

    with pytest.raises(TimeoutError, match="deadline"):
        gate.checkpoint(_arrival(), terminal)
    assert gate._done.is_set()


@pytest.mark.parametrize("released", (False, True))
def test_follower_uses_remaining_leader_budget_without_real_wait(monkeypatch, released):
    gate = _gate(timeout=1.0)
    arrival = _arrival()
    gate._selected = arrival
    gate._started = True
    gate._deadline = 101.0
    monkeypatch.setattr(gates.time, "monotonic", lambda: 100.75)

    def wait(timeout):
        _selector_is_unlocked(gate)
        assert timeout == 0.25
        if released:
            gate._done.set()

    gate._done = _Completion(on_wait=wait)
    if released:
        gate.checkpoint(arrival, lambda _: pytest.fail("follower repeated terminal"))
    else:
        with pytest.raises(TimeoutError, match="shared deadline"):
            gate.checkpoint(arrival)
    assert gate._done.waits == [0.25]


def test_follower_after_deadline_gets_no_new_timeout_allowance(monkeypatch):
    gate = _gate(timeout=1.0)
    gate._selected = _arrival()
    gate._started = True
    gate._deadline = 101.0
    monkeypatch.setattr(gates.time, "monotonic", lambda: 102.0)
    with pytest.raises(TimeoutError, match="shared deadline"):
        gate.checkpoint(gate._selected)
    assert gate._done.waits == [0.0]


@pytest.mark.parametrize("phase", (
    gates.OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK,
    gates.OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK,
))
def test_node_preparation_gate_observes_acknowledged_phase_outside_locks(monkeypatch, phase):
    fixture, node, record, _complete = _node(refs=False, stored=False)
    gate = _gate(phase=phase)
    node._output_publication_gate = gate
    fixture.adapter._test_checkpoint = node._test_output_publication_checkpoint
    events = []

    def connect(*_args, **_kwargs):
        _selector_is_unlocked(gate)
        assert not node._state_lock._is_owned()
        assert not fixture.journal._lock._is_owned()
        snapshot = fixture.journal.snapshot(fixture.id)
        handoff = fixture.handoffs.query(fixture.id)
        assert record.state is protocol.LeaseExecutionState.RUNNING
        assert snapshot.complete is None and fixture.ledger.available != fixture.ledger.total
        if phase is gates.OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK:
            assert snapshot.materialized is False and not snapshot.ready_to_complete
        else:
            assert snapshot.ready_to_complete and snapshot.materialized is True
        assert handoff.manifest == fixture.manifest and handoff.complete is None
        return _Connection(events)

    monkeypatch.setattr(gates.socket, "create_connection", connect)
    request = wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))
    assert node._handle_prepare_output_publication(request).accepted
    assert node._handle_prepare_output_publication(request).accepted
    assert events == [gates.OutputPublicationGateArrival.from_manifest(
        fixture.manifest, phase,
    ), "released", "closed"]
    assert gate._selected.phase is gates.OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK
    assert fixture.journal.snapshot(fixture.id).ready_to_complete
    assert fixture.handoffs.query(fixture.id).complete is None


def _completed_node(*, stored=False):
    fixture, node, record, complete = _node(stored=stored)
    prepared = node._handle_prepare_output_publication(wire.PrepareOutputPublication(
        fixture.manifest, (fixture.values.payload),
    ))
    assert prepared.accepted
    reply = node._handle_complete_worker_lease_inner(complete)
    assert reply.accepted and fixture.journal.snapshot(fixture.id).complete == fixture.values.witness
    return fixture, node, record, complete, reply


def _outcome_request(fixture, record):
    values = fixture.values
    return protocol.GetWorkerLeaseOutcome(
        values.lease, values.task, values.attempt, values.executor,
        values.owner, ((fixture.id.object_id,)),
    )


@pytest.mark.parametrize("first_exit", ("complete", "outcome"))
@pytest.mark.parametrize("stored", (False, True))
def test_complete_and_outcome_share_one_gate_without_authority_locks(monkeypatch, first_exit, stored):
    fixture, node, record, complete, _reply = _completed_node(stored=stored)
    gate = _gate()
    node._output_publication_gate = gate
    events = []
    terminal_calls = []

    def unlocked():
        _selector_is_unlocked(gate)
        assert not node._state_lock._is_owned()
        assert not fixture.journal._lock._is_owned()
        assert fixture.ledger.available == fixture.ledger.total

    original_report = fixture.adapter._report_complete

    def report(witness):
        unlocked()
        assert witness == fixture.values.witness
        terminal_calls.append(witness)
        return original_report(witness)

    def connect(*_args, **_kwargs):
        unlocked()
        assert fixture.handoffs.query(fixture.id).complete == fixture.values.witness
        return _Connection(events, probe=unlocked)

    fixture.adapter._report_complete = report
    monkeypatch.setattr(gates.socket, "create_connection", connect)
    exits = {
        "complete": lambda: node._handle_complete_worker_lease(complete),
        "outcome": lambda: node._handle_get_worker_lease_outcome(_outcome_request(fixture, record)),
    }
    first = exits[first_exit]()
    other = exits["outcome" if first_exit == "complete" else "complete"]()
    assert first.output_publication == other.output_publication == fixture.values.envelope
    assert (first.output_publication.result) == (fixture.values.result)
    assert events == ([gates.OutputPublicationGateArrival.from_manifest(fixture.manifest, gates.OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY), 'released', 'closed'])
    assert (len(terminal_calls)) == (1)
    assert gate._done.waits == []


def test_outcome_cannot_bypass_the_delivery_checkpoint():
    fixture, node, record, _complete, _reply = _completed_node()
    observed = []

    class _PausedGate:
        def checkpoint(self, arrival, ensure_terminal=None):
            assert not node._state_lock._is_owned()
            assert not fixture.journal._lock._is_owned()
            observed.append(arrival)
            raise RuntimeError("delivery paused")

    node._output_publication_gate = _PausedGate()
    with pytest.raises(RuntimeError, match="delivery paused"):
        node._handle_get_worker_lease_outcome(_outcome_request(fixture, record))
    assert observed == [gates.OutputPublicationGateArrival.from_manifest(
        fixture.manifest, gates.OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY,
    )]
    assert fixture.ledger.available == fixture.ledger.total


def test_unconfigured_node_checkpoint_has_no_gate_effect():
    fixture, node, record, _complete, reply = _completed_node()
    node._output_publication_gate = None
    for phase in gates.OutputPublicationGatePhase:
        node._test_output_publication_checkpoint(fixture.manifest, phase)
    node._test_output_result_delivery_checkpoint(reply)
    outcome = node._handle_get_worker_lease_outcome(_outcome_request(fixture, record))
    assert outcome.output_publication == fixture.values.envelope
    assert fixture.handoffs.query(fixture.id).complete is None


def test_delivery_revalidates_payload_retirement_after_the_gate_opens():
    fixture, node, _record, _complete, reply = _completed_node()

    class _RetiringGate:
        def checkpoint(self, arrival, ensure_terminal=None):
            assert not node._state_lock._is_owned()
            assert not fixture.journal._lock._is_owned()
            assert arrival.publication_id == fixture.id
            fixture.journal.retire_completed(OutputPublicationAdoptionProof(
                fixture.values.witness, fixture.values.owner, "gate-owner-cas",
            ))

    node._output_publication_gate = _RetiringGate()
    with pytest.raises(RuntimeError, match="retired"):
        node._test_output_result_delivery_checkpoint(reply)
    assert fixture.journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED


def test_delivery_revalidates_owner_death_after_the_gate_opens():
    fixture, node, _record, _complete, reply = _completed_node()

    class _OwnerDeathGate:
        def checkpoint(self, arrival, ensure_terminal=None):
            assert not node._state_lock._is_owned()
            assert not fixture.journal._lock._is_owned()
            assert arrival.publication_id == fixture.id
            # The Node lease check only consumes membership of an already-
            # installed owner fence; constructing death evidence is not this test.
            node._owner_death_fences[fixture.values.owner] = object()

    node._output_publication_gate = _OwnerDeathGate()
    with pytest.raises(ValueError, match="death-fenced"):
        node._test_output_result_delivery_checkpoint(reply)
    assert fixture.journal.snapshot(fixture.id).complete == fixture.values.witness
