"""Pure observation contracts around the existing publication authorities.

Each case uses two fixed tiny slots, a 1-KiB store and an unstarted Node; Core
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

import pytest

from miniray import control, core as core_module, node as node_module, output_protocol as wire, protocol, transport
from miniray.core import CoreWorker, _WAKE_COORDINATOR
from miniray.node import NodeServer
from miniray.output_publication_journal import OutputPublicationJournalState
from miniray.output_recovery import OutputRecoveryStage
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
    "output_publication_ack", "output_lease_completed",
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
    fixture, node, record, complete = _node(refs=False)
    stored_id = fixture.manifest.slots[1].object_id
    calls = []

    def probe(name, _attributes):
        if name != "output_lease_completed":
            return None
        return (
            node._state_lock._is_owned(), fixture.journal._lock._is_owned(),
            record.state, fixture.ledger.available,
            fixture.journal.snapshot(fixture.id).complete,
            fixture.recovery.snapshot(fixture.id).complete,
            fixture.store.get(stored_id),
        )

    sink = _ObservedSink(probe, error_type)
    node.event_sink = sink

    def stored_gcs_rpc(handler, request):
        assert handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER and len(calls) < 4
        if type(request) is wire.ReportOutputPublicationIntent:
            ack = fixture.recovery.report_intent(request.manifest)
        elif type(request) is wire.ArmOutputPublication:
            ack = fixture.recovery.arm_complete(request.publication_id, request.manifest_digest)
        else:
            assert type(request) is wire.ReportOutputPublicationTerminal
            ack = fixture.recovery.report_terminal(request.witness)
        reply = wire.OutputRecoveryReply(request, ack)
        received = _received(sink, "node", handler, len(calls))
        calls.append((request, reply, received))
        return reply

    node._stored_gcs_rpc = stored_gcs_rpc
    # Use the production Node adapter factory, not _Fixture's callback adapter.
    node._output_publications = node._make_output_publication_adapter()
    adapter = node._output_publications
    prior_cause = current_cause_id()
    prepared = node._handle_prepare_output_publication(
        wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads)
    )
    assert prepared.accepted and current_cause_id() == prior_cause
    assert [reply.ack.stage for _, reply, _ in calls] == [
        OutputRecoveryStage.INTENT, OutputRecoveryStage.ARM_COMPLETE,
    ]
    assert record.state is protocol.LeaseExecutionState.RUNNING
    assert fixture.journal.snapshot(fixture.id).ready_to_complete
    assert fixture.store.capacity_bytes == 1024
    assert fixture.store.get(stored_id) == fixture.values.payloads[1]
    assert fixture.recovery.snapshot(fixture.id).complete is None
    with causal_scope("inert-complete-handler"):
        first = node._handle_complete_worker_lease(complete)
        replay = node._handle_complete_worker_lease(complete)
    assert current_cause_id() == prior_cause
    assert first.accepted and first.released and replay.accepted and not replay.released
    assert first.output_publication == replay.output_publication == fixture.values.envelope
    assert fixture.ledger.available == fixture.ledger.total
    assert tuple(node._leases) == (fixture.id.lease_id,) and len(calls) == 2
    assert adapter.pending_terminal_reports() == (fixture.values.witness,)
    assert adapter.pending_lease_completions() == ()
    assert fixture.recovery.snapshot(fixture.id).complete is None
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == (0, 1)
    local = tuple(sample for sample in sink.samples if sample[0] == "output_lease_completed")
    assert [sample[2] for sample in local] == [
        dict(_identity_fields(fixture), released=released, status="SUCCEEDED", state="COMPLETED")
        for released in (True, False)
    ]
    assert all(sample[4] == (
        False, False, protocol.LeaseExecutionState.COMPLETED, fixture.ledger.total,
        fixture.values.witness, None, fixture.values.payloads[1],
    ) for sample in local)
    with causal_scope("inert-terminal-parent"):
        assert adapter.report_terminal(fixture.id)
        expected_cause = (sink.events[-1].event_id if error_type is None else calls[-1][2].event_id)
        assert current_cause_id() == expected_cause
    assert current_cause_id() == prior_cause
    assert not adapter.report_terminal(fixture.id) and len(calls) == 3
    assert adapter.pending_terminal_reports() == ()
    assert fixture.recovery.snapshot(fixture.id).complete == fixture.values.witness
    acknowledgements = tuple(sample for sample in sink.samples if sample[0] == "output_publication_ack")
    assert len(acknowledgements) == 3
    for sample, (_request, reply, received) in zip(acknowledgements, calls):
        assert sample[1] == "node" and sample[3] == received.event_id
        assert sample[2] == dict(_identity_fields(fixture), stage=reply.ack.stage.value, accepted=True)
    if error_type is None:
        actual_acks = tuple(event for event in sink.events if event.name == "output_publication_ack")
        assert tuple(event.cause_id for event in actual_acks) == tuple(event.event_id for _, _, event in calls)
    _assert_sink(sink, error_type)
    # No Core owner is constructed in this local-only case; stored bytes remain
    # deliberately live. The Core cases below perform actual owner collection.
    assert fixture.store.get(stored_id) == fixture.values.payloads[1]


@pytest.mark.parametrize("error_type", (None, RuntimeError, ObservationOnlyFailure),
                         ids=("records", "exception-sink", "base-exception-sink"))
def test_core_observation_preserves_ready_adoption_payload_retirement_and_gc(error_type):
    fixture, node, core, pending, reply, calls, original_rpc = _core_fixture(refs=False)
    stored_id = pending.output_ids[1]
    boundaries = []
    rpc_limit_exceeded = False

    def probe(name, _attributes):
        if name not in ("output_owner_ready", "output_payload_retired"):
            return None
        return (
            core._state_lock._is_owned(), core.owner_table._lock._is_owned(),
            node._state_lock._is_owned(),
            fixture.journal._lock._is_owned(),
            tuple(core.owner_table.snapshot(value).state for value in pending.output_ids),
            tuple(core._objects[value].event.is_set() for value in pending.output_ids),
            fixture.recovery.snapshot(fixture.id).adopted is not None,
            fixture.journal.snapshot(fixture.id).retained_result_slots,
            fixture.store.get(stored_id),
        )

    sink = _ObservedSink(probe, error_type)
    core.event_sink = sink

    def rpc(address, handler, request):
        nonlocal rpc_limit_exceeded
        if len(boundaries) >= _LIMIT or len(calls) >= _LIMIT:
            # Save the violation before any reducer effect: production GC or
            # observation containment must not turn a swallowed failure green.
            rpc_limit_exceeded = True
            pytest.fail("publication observation exceeded its RPC bound")
        # The pre-existing fixture dispatches to the original Node/GC/GCS
        # reducers. This wrapper supplies observation only after their reply.
        result = original_rpc(address, handler, request)
        received = _received(sink, "core_worker", handler, len(boundaries))
        boundaries.append((handler, request, result, received))
        return result

    core._rpc = rpc
    prior_cause = current_cause_id()
    try:
        assert fixture.store.capacity_bytes == 1024
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id,
                                   expected_lease_id=fixture.id.lease_id)
        assert current_cause_id() == prior_cause
        assert [handler for handler, _ in calls] == [
            wire.REPORT_OUTPUT_PUBLICATION_HANDLER, wire.REPORT_OUTPUT_PUBLICATION_HANDLER,
            wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER,
        ]
        assert [sample[0] for sample in sink.samples] == [
            "output_publication_ack", "output_owner_ready",
            "output_publication_ack", "output_payload_retired",
        ]
        ready = sink.samples[1]
        retired = sink.samples[3]
        assert ready[2] == dict(_identity_fields(fixture), return_count=2)
        assert retired[2] == _identity_fields(fixture)
        ready_facts = (
            False, False, False, False, (ObjectState.READY_INLINE, ObjectState.READY_STORED),
            (True, True), False, (0, 1), fixture.values.payloads[1],
        )
        assert ready[4] == ready_facts
        assert retired[4] == (*ready_facts[:6], True, (), fixture.values.payloads[1])
        for sample, boundary in ((sink.samples[0], boundaries[0]), (sink.samples[2], boundaries[1])):
            assert sample[1] == "core_worker" and sample[3] == boundary[3].event_id
            assert sample[2] == dict(_identity_fields(fixture), stage=boundary[2].ack.stage.value, accepted=True)
        assert sink.samples[0][2]["stage"] == "TERMINAL"
        assert sink.samples[2][2]["stage"] == "ADOPTED"
        assert retired[3] == boundaries[2][3].event_id
        if error_type is None:
            actual = tuple(event for event in sink.events if event.name in _OBSERVATIONS)
            assert actual[0].cause_id == boundaries[0][3].event_id
            assert actual[2].cause_id == boundaries[1][3].event_id
            assert actual[3].cause_id == boundaries[2][3].event_id
        assert tuple(node._leases) == (fixture.id.lease_id,)
        assert fixture.ledger.available == fixture.ledger.total
        assert fixture.journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
        assert fixture.store.get(stored_id) == fixture.values.payloads[1]
        assert not core._protocol_unresolved and not core._stored_descriptors.get(pending.output_ids[0])
        assert core._stored_descriptors[stored_id] == reply.results[1]
        record = core._recovery.task_record(pending.task_id)
        assert record.state is TaskState.SUCCEEDED and record.current_attempt == pending.spec.attempt_id
        assert record.retries_started == 0 and core._recovery.active_recovery(pending.task_id) is None
        assert all(core._objects[value].event.is_set() for value in pending.output_ids)
        # Late publication bookkeeping is still independent of local lease
        # release. It does not recreate bytes or another owner-ready event.
        assert fixture.adapter.report_terminal(fixture.id)
        assert fixture.adapter.pending_terminal_reports() == ()
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        for index, object_id in enumerate(pending.output_ids):
            assert core.owner_table.release_local_reference(object_id, "outer{}".format(index))
            core._reference_released(object_id)
            assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
            if index == 0:
                assert core.owner_table.contains(stored_id)
                assert fixture.store.get(stored_id) == fixture.values.payloads[1]
        core._reference_mailbox.drain()
        queued = core._submissions.qsize()
        assert queued <= _LIMIT
        for _ in range(queued):
            item = core._submissions.get_nowait()
            try:
                assert item is _WAKE_COORDINATOR  # no retry or fresh lease admission
            finally:
                core._submissions.task_done()
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        assert len(calls) == len(boundaries) == 6
        assert fixture.store.used_bytes == 0 and not node._sealed_metadata
        assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
        assert len(fixture.recovery.snapshot(fixture.id).slot_collections) == 2
        assert all(core._recovery.lineage_for_object(value) is None for value in pending.output_ids)
        with pytest.raises(UnknownTaskError):
            core._recovery.task_record(pending.task_id)
        assert len(sink.samples) == 4
        _assert_sink(sink, error_type)
        assert current_cause_id() == prior_cause
    finally:
        # Failure fencing only; do not invent a terminal result or claim GC.
        core._rpc = original_rpc
        for object_id in tuple(core._objects):
            for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
                core.owner_table.release_local_reference(object_id, token)
        close_pure_core(core)
        assert not rpc_limit_exceeded
