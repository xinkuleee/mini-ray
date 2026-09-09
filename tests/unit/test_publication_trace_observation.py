"""Pure observation contracts around the existing publication authorities.

Each case uses one fixed tiny result, a 1-KiB store and an unstarted Node; Core
cases also use the existing threadless owner/recovery fixture. No user task
function, process, socket, timer or runtime thread executes. The typed
in-process bridges return actual reducer replies, then emit one explicitly simulated
rpc_reply_received observation with a real EventSink. This tests local causal
scope binding, not network delivery or cross-process transport causality.

Trace samples/calls are bounded at 32. Broken observation sinks fail only the
new observation names; their exceptions must not change publication, release,
payload retirement or owner GC. No business fault or recovery is introduced.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

from dataclasses import replace

import pytest

from miniray import control, core as core_module, node as node_module, output_protocol as wire, protocol, transport
from miniray.core import CoreWorker, _WAKE_COORDINATOR
from miniray.node import NodeServer
from miniray.output_publication_journal import OutputPublicationJournalState
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState, UnknownTaskError
from miniray.trace import MemoryEventSink, causal_scope, current_cause_id
from miniray.worker import WorkerServer
from tests.unit._pure_core import close_pure_core
from tests.unit.test_core_output_publication import _fixture as _core_fixture
from tests.unit.test_output_publication_node_server import _node


pytestmark = pytest.mark.unit
_LIMIT = 32
_OBSERVATIONS = frozenset((
    "output_lease_completed",
    "output_owner_ready", "output_payload_retired",
))


class ObservationOnlyFailure(BaseException):
    """Not an Exception: exercise the new observation-only containment."""


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    violations = []

    def forbidden(*_args, **_kwargs):
        if len(violations) < _LIMIT:
            violations.append(True)
        pytest.fail("publication observation attempted runtime or transport")

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "_execute"),
        (NodeServer, "__init__"),
        (WorkerServer, "__init__"), (control.GCSLite, "__init__"),
        (transport.TCPServer, "__init__"), (threading.Timer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (core_module, node_module, control):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    yield
    # Observation BaseException containment must not hide a tripwire failure.
    assert not violations


class _ObservedSink(MemoryEventSink):
    def __init__(self, probe, error_type):
        super().__init__()
        self.probe = probe
        self.error_type = error_type
        self.samples = []
        self.probe_errors = []
        self.overflow = False

    def _write(self, event):
        if len(self.events) >= _LIMIT:
            self.overflow = True
            return
        super()._write(event)

    def emit(self, name, *, component, **kwargs):
        if name in _OBSERVATIONS:
            if len(self.samples) >= _LIMIT:
                self.overflow = True
            else:
                try:
                    attributes = dict(kwargs.get("attributes") or {})
                    facts = self.probe(name, attributes)
                    self.samples.append((name, component, attributes, current_cause_id(), facts))
                except BaseException as exc:
                    if len(self.probe_errors) < _LIMIT:
                        self.probe_errors.append(exc)
                    else:
                        self.overflow = True
            if self.error_type is not None:
                raise self.error_type("observation sink only")
        return super().emit(name, component=component, **kwargs)


def _identity_fields(fixture):
    return {
        "task_id": str(fixture.id.task_id),
        "attempt_id": str(fixture.id.attempt_id),
        "lease_id": str(fixture.id.lease_id),
        "manifest_digest": fixture.manifest.manifest_digest,
    }


def _received(sink, component, handler, index):
    # No fake request/server trace: only the inert client-boundary observation
    # needed to test the production causal_scope -> acknowledgement edge.
    event = sink.emit(
        "rpc_reply_received", component=component,
        attributes={"handler": handler, "rpc_id": "inert-reply-{}".format(index), "ok": True},
    )
    assert event is not None
    return event


def _assert_sink(sink, error_type):
    assert not sink.overflow and not sink.probe_errors
    assert len(sink.events) <= _LIMIT and len(sink.samples) <= _LIMIT
    emitted = tuple(event for event in sink.events if event.name in _OBSERVATIONS)
    assert len(emitted) == (len(sink.samples) if error_type is None else 0)


@pytest.mark.parametrize("error_type", (None, RuntimeError, ObservationOnlyFailure),
                         ids=("records", "exception-sink", "base-exception-sink"))
def test_node_observation_preserves_prepare_local_release_and_terminal_outbox(error_type):
    fixture, node, record, complete = _node(refs=False, stored=True)
    stored_id = fixture.manifest.slots[0].object_id
    calls = []
    def probe(name, _attributes):
        if name != "output_lease_completed":
            return None
        return (node._state_lock._is_owned(), fixture.journal._lock._is_owned(),
                record.state, fixture.ledger.available, fixture.journal.snapshot(fixture.id).complete,
                fixture.handoffs.query(fixture.id).complete, fixture.store.get(stored_id))
    sink = _ObservedSink(probe, error_type)
    node.event_sink = sink
    record.request = replace(record.request, requester_owner_address=("owner.invalid", 1))
    def owner_rpc(address, handler, request):
        assert address == ("owner.invalid", 1) and len(calls) < 3
        assert not node._state_lock._is_owned()
        if handler == wire.REGISTER_OUTPUT_HANDOFF_HANDLER:
            snapshot = fixture.handoffs.register(request.manifest, fixture.id.attempt_id)
        else:
            assert handler == wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER
            snapshot = fixture.handoffs.record_complete(request.witness)
        reply = wire.OutputHandoffReply(request, True, snapshot)
        received = _received(sink, "node", handler, len(calls))
        calls.append((request, reply, received))
        return reply
    node._background_rpc = owner_rpc
    node._output_publications = node._make_output_publication_adapter()
    adapter = node._output_publications
    prior_cause = current_cause_id()
    with causal_scope("inert-prepare-handler"):
        prepared = node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads))
    assert prepared.accepted and current_cause_id() == prior_cause
    assert len(calls) == 1 and type(calls[0][0]) is wire.RegisterOutputHandoff
    assert record.state is protocol.LeaseExecutionState.RUNNING
    assert fixture.journal.snapshot(fixture.id).ready_to_complete
    assert fixture.store.capacity_bytes == 1024
    assert fixture.store.get(stored_id) == fixture.values.payloads[0]
    assert fixture.handoffs.query(fixture.id).complete is None
    with causal_scope("inert-complete-handler"):
        first = node._handle_complete_worker_lease(complete)
        replay = node._handle_complete_worker_lease(complete)
    assert current_cause_id() == prior_cause
    assert first.accepted and first.released and replay.accepted and not replay.released
    assert first.output_publication == replay.output_publication == fixture.values.envelope
    assert fixture.ledger.available == fixture.ledger.total
    assert tuple(node._leases) == (fixture.id.lease_id,) and len(calls) == 1
    assert adapter.pending_terminal_reports() == (fixture.values.witness,)
    assert adapter.pending_lease_completions() == ()
    assert fixture.handoffs.query(fixture.id).complete is None
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == (0,)
    assert [sample[2] for sample in sink.samples] == [
        dict(_identity_fields(fixture), released=released, status="SUCCEEDED", state="COMPLETED")
        for released in (True, False)
    ]
    assert all(sample[4] == (False, False, protocol.LeaseExecutionState.COMPLETED, fixture.ledger.total,
                            fixture.values.witness, None, fixture.values.payloads[0]) for sample in sink.samples)
    assert sink.samples[0][3] == "inert-complete-handler"
    with causal_scope("inert-terminal-parent"):
        assert adapter.report_terminal(fixture.id)
        assert current_cause_id() == calls[-1][2].event_id
    assert current_cause_id() == prior_cause
    assert not adapter.report_terminal(fixture.id) and len(calls) == 2
    assert adapter.pending_terminal_reports() == ()
    assert fixture.handoffs.query(fixture.id).complete == fixture.values.witness
    assert len(sink.samples) == 2
    _assert_sink(sink, error_type)
    assert fixture.store.get(stored_id) == fixture.values.payloads[0]


@pytest.mark.parametrize("error_type", (None, RuntimeError, ObservationOnlyFailure),
                         ids=("records", "exception-sink", "base-exception-sink"))
def test_core_observation_preserves_ready_adoption_payload_retirement_and_gc(error_type):
    fixture, node, core, pending, reply, calls, original_rpc = _core_fixture(refs=False, stored=True)
    stored_id = pending.object_id
    boundaries = []
    rpc_limit_exceeded = False
    def probe(name, _attributes):
        if name not in ("output_owner_ready", "output_payload_retired"):
            return None
        return (core._state_lock._is_owned(), core.owner_table._lock._is_owned(),
                node._state_lock._is_owned(), fixture.journal._lock._is_owned(),
                core.owner_table.snapshot(stored_id).state, core._objects[stored_id].event.is_set(),
                fixture.handoffs.query(fixture.id).adoption is not None,
                fixture.journal.snapshot(fixture.id).retained_result_slots, fixture.store.get(stored_id))
    sink = _ObservedSink(probe, error_type)
    core.event_sink = sink
    def rpc(address, handler, request):
        nonlocal rpc_limit_exceeded
        if len(boundaries) >= _LIMIT or len(calls) >= _LIMIT:
            rpc_limit_exceeded = True
            pytest.fail("publication observation exceeded its RPC bound")
        result = original_rpc(address, handler, request)
        received = _received(sink, "core_worker", handler, len(boundaries))
        boundaries.append((handler, request, result, received))
        return result
    core._rpc = rpc
    prior_cause = current_cause_id()
    try:
        assert fixture.store.capacity_bytes == 1024
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert current_cause_id() == prior_cause
        assert [handler for handler, _ in calls] == [wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER]
        assert [sample[0] for sample in sink.samples] == ["output_owner_ready", "output_payload_retired"]
        ready, retired = sink.samples
        assert ready[2] == dict(_identity_fields(fixture), return_count=1)
        assert retired[2] == _identity_fields(fixture)
        ready_facts = (False, False, False, False, ObjectState.READY_STORED, True, True, (0,), fixture.values.payloads[0])
        assert ready[4] == ready_facts
        assert retired[4] == (*ready_facts[:7], (), fixture.values.payloads[0])
        assert retired[3] == boundaries[0][3].event_id
        if error_type is None:
            actual = tuple(event for event in sink.events if event.name in _OBSERVATIONS)
            assert actual[1].cause_id == boundaries[0][3].event_id
        assert fixture.ledger.available == fixture.ledger.total
        assert fixture.journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
        assert fixture.store.get(stored_id) == fixture.values.payloads[0]
        assert not core._protocol_unresolved and core._stored_descriptors[stored_id] == reply.results[0]
        record = core._recovery.task_record(pending.task_id)
        assert record.state is TaskState.SUCCEEDED and record.current_attempt == pending.spec.attempt_id
        assert record.retries_started == 0 and core._recovery.active_recovery(pending.task_id) is None
        assert not fixture.adapter.report_terminal(fixture.id)
        assert fixture.adapter.pending_terminal_reports() == ()
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert core.owner_table.release_local_reference(stored_id, "outer0")
        core._reference_released(stored_id)
        assert core.owner_table.collection_state(stored_id) is ObjectCollectionState.COLLECTED
        core._reference_mailbox.drain()
        queued = core._submissions.qsize()
        assert queued <= _LIMIT
        for _ in range(queued):
            item = core._submissions.get_nowait()
            try:
                assert item is _WAKE_COORDINATOR
            finally:
                core._submissions.task_done()
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        assert len(calls) == len(boundaries) == 2
        assert fixture.store.used_bytes == 0 and not node._sealed_metadata
        assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
        assert core._recovery.lineage_for_object(stored_id) is None
        with pytest.raises(UnknownTaskError):
            core._recovery.task_record(pending.task_id)
        assert len(sink.samples) == 2
        _assert_sink(sink, error_type)
        assert current_cause_id() == prior_cause
    finally:
        core._rpc = original_rpc
        for object_id in tuple(core._objects):
            for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
                core.owner_table.release_local_reference(object_id, token)
        close_pure_core(core)
        assert not rpc_limit_exceeded
