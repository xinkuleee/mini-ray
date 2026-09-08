"""Pure Worker discovery/custody contracts on the unified output backend.

Reuse the small in-memory backend from test_worker_unified_output rather than
maintaining INLINE/STORED projection stubs. Its autouse safety fixture forbids
runtime construction, sockets, subprocesses, threads and real waits.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import ObjectRef
from miniray.errors import ProtocolError
from miniray.ids import NodeID, ObjectID, TaskID, WorkerID
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication_journal import (
    OutputPublicationJournalState, OutputPublicationStage,
)
from miniray.transport import TransportError
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER,
    START_WORKER_LEASE_HANDLER,
)
from tests.unit.test_output_publication_node_server import _node
from tests.unit.test_worker_unified_output import (
    _Fixture, _borrowed_fixture, _completion, _envelope, _outcome,
    _no_runtime as _no_runtime,
)


pytestmark = pytest.mark.unit


@pytest.mark.parametrize("count", (1, 2))
@pytest.mark.parametrize("threshold", (0, 65536))
def test_plain_shapes_discover_every_slot_once_before_first_prepare(monkeypatch, count, threshold):
    reductions = []

    class _Value:
        def __init__(self, index):
            self.index = index

        def __reduce__(self):
            reductions.append(self.index)
            return bytes, (b"value" * (self.index + 1),)

    values = tuple(_Value(index) for index in range(count))
    f = _Fixture(monkeypatch, lambda: values[0] if count == 1 else values,
                 count=count, threshold=threshold)

    def prepare(request):
        assert reductions == list(range(count))
        assert f.pending.outputs.manifest == request.manifest
        assert f.pending.outputs.slot_payloads == request.slot_payloads
        assert request.manifest.header.node_incarnation == f.incarnation
        return wire.PreparedOutputPublicationReply(request.request_identity, True)

    f.on_prepare = prepare
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert reply.output_publication == f.complete_envelope
    assert len(reply.results) == count
    tier = protocol.ResultStorage.OBJECT_STORE if threshold == 0 else protocol.ResultStorage.INLINE
    assert all(result.storage is tier for result in reply.results)
    assert f.handlers == [START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER,
                          COMPLETE_WORKER_LEASE_HANDLER]
    assert reductions == list(range(count)) and f.executions == [True]
    assert f.worker._handle_push_task(f.push) is reply
    assert reductions == list(range(count)) and f.executions == [True]
    assert len(f.prepares) == 1 and len(f.calls) == 3


@pytest.mark.parametrize("target", (False, True))
def test_multi_contained_outputs_publish_once_after_all_selected_slots_discover(monkeypatch, target):
    returned = []
    reductions = []
    f = _Fixture(monkeypatch, lambda: tuple(returned), count=4 if target else 2,
                 threshold=1024, target=target)
    child = ObjectRef(ObjectID.for_task(TaskID.random()), f.worker.worker_id, f.worker.address)

    class _Selected:
        def __init__(self, index, padding):
            self.index = index
            self.padding = padding

        def __reduce__(self):
            reductions.append(self.index)
            return dict, ((("child", child), ("padding", self.padding)),)

    class _Unselected:
        def __reduce__(self):
            pytest.fail("unselected output was serialized")

    indices = (1, 3) if target else (0, 1)
    values = (_Selected(indices[0], b""), _Selected(indices[1], b"x" * 8192))
    returned.extend((_Unselected(), values[0], _Unselected(), values[1]) if target else values)

    def prepare(request):
        assert reductions == list(indices)
        assert f.pending.discovery.source_references == (child, child)
        assert f.pending.outputs.manifest == request.manifest
        assert tuple(slot.object_id.return_index for slot in request.manifest.slots) == indices
        first, second = request.manifest.slots
        assert first.transfers[0].contained_object_id == second.transfers[0].contained_object_id == child.object_id
        assert first.transfers[0].final_hold != second.transfers[0].final_hold
        return wire.PreparedOutputPublicationReply(request.request_identity, True)

    f.on_prepare = prepare
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert reply.output_publication == f.complete_envelope
    assert tuple(result.storage for result in reply.results) == (
        protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE,
    )
    assert not hasattr(reply, "inline_publication") and not hasattr(reply, "stored_publication")
    assert reductions == list(indices) and f.executions == [True]
    assert f.handlers == [START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER,
                          COMPLETE_WORKER_LEASE_HANDLER]
    assert not f.worker._prepared_output_replies
    assert f.worker._handle_push_task(f.push) is reply and len(f.calls) == 3
    assert reductions == list(indices)


def test_bad_later_selected_slot_releases_discovery_custody_without_publication_effect(monkeypatch):
    returned = []
    reductions = []
    sessions = []
    f = _Fixture(monkeypatch, lambda: tuple(returned), count=2, threshold=0)
    child = ObjectRef(ObjectID.for_task(TaskID.random()), f.worker.worker_id, f.worker.address)
    original_discover = OutputDiscoverySession.discover

    class _Good:
        def __reduce__(self):
            reductions.append("good")
            return dict, ((("child", child),),)

    class _Bad:
        def __reduce__(self):
            assert sessions[0].source_references == (child,)
            reductions.append("bad")
            raise ValueError("later selected slot failed serialization")

    def discover(session, values):
        sessions.append(session)
        return original_discover(session, values)

    monkeypatch.setattr(OutputDiscoverySession, "discover", discover)
    returned.extend((_Good(), _Bad()))
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert "later selected slot" in reply.error.message
    assert reply.results == () and reply.output_publication is None
    assert f.handlers == [START_WORKER_LEASE_HANDLER, COMPLETE_WORKER_LEASE_HANDLER]
    assert reductions == ["good", "bad"] and f.executions == [True]
    assert len(sessions) == 1 and sessions[0].source_references == ()
    assert sessions[0].discovered is None and not child.closed
    assert not getattr(f.worker, "_prepared_output_replies", {}) and f.prepares == []
    assert f.worker._handle_push_task(f.push) is reply and len(f.calls) == 2


@pytest.mark.parametrize("mode", ("missing", "wrong-node"))
def test_start_requires_exact_registered_incarnation_before_user_decode(monkeypatch, mode):
    f = _Fixture(monkeypatch, lambda: 1)
    value = None if mode == "missing" else replace(f.incarnation, node_id=NodeID.random())

    def start_only(_address, handler, request, **_options):
        assert handler == START_WORKER_LEASE_HANDLER
        f.calls.append((handler, request))
        return protocol.StartWorkerLeaseReply(
            request.lease_id, protocol.LeaseExecutionState.RUNNING, True,
            scheduling_key=request.scheduling_key, target_execution=request.target_execution,
            node_incarnation=value,
        )

    monkeypatch.setattr("miniray.worker.rpc_request", start_only)
    with pytest.raises(RuntimeError, match="incarnation|another publishing Node"):
        f.worker._handle_push_task(f.push)
    assert f.executions == [] and f.handlers == [START_WORKER_LEASE_HANDLER]
    assert f.worker._lease_bindings == {} and f.worker._replies == {}
    assert not getattr(f.worker, "_prepared_output_replies", {})


@pytest.mark.parametrize("threshold", (0, 65536))
def test_ambiguous_first_prepare_keeps_exact_bytes_and_attempt_borrower_until_promoted(monkeypatch, threshold):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=threshold)
    accept = [False]

    def prepare(request):
        assert request.manifest.header.node_incarnation == f.incarnation
        assert f.pending.nested_imports is imports
        assert f.pending.discovery.source_references == (child,)
        assert child.closes == 0 and not child.closed
        if not accept[0]:
            raise TransportError("prepare acknowledgement unavailable")
        return wire.PreparedOutputPublicationReply(request.request_identity, True)

    def complete(request):
        assert child.closed and child.closes == 1
        assert f.pending.nested_imports is None
        assert f.pending.discovery.source_references == ()
        return f.complete(request)

    f.on_prepare, f.on_complete = prepare, complete
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    outputs = pending.outputs
    assert not pending.prepare_acked and pending.failure_reply is None
    assert child.closes == 0 and not child.closed
    assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
    assert COMPLETE_WORKER_LEASE_HANDLER not in f.handlers
    accept[0] = True
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert all(request == f.prepares[0] for request in f.prepares)
    assert pending.outputs is outputs and reply.output_publication.manifest == outputs.manifest
    assert f.executions == reductions == [True] and child.closes == 1
    assert not f.worker._prepared_output_replies
    rpc_count = len(f.calls)
    assert f.worker._handle_push_task(f.push) is reply
    assert len(f.calls) == rpc_count and child.closes == 1


@pytest.mark.parametrize("threshold", (0, 65536))
@pytest.mark.parametrize("phase", ("source", "imports"))
def test_prepare_ack_retries_only_local_source_or_import_drain_before_complete(monkeypatch, threshold, phase):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=threshold)
    source_calls, close_calls = [], []
    original_release = OutputDiscoverySession.release_sources_after_promotions
    original_close = imports.close

    def release_then_raise(discovery):
        source_calls.append(discovery)
        original_release(discovery)
        if phase == "source" and len(source_calls) == 1:
            raise RuntimeError("source release took effect before local failure")

    def close_then_raise():
        close_calls.append(True)
        original_close()
        if phase == "imports" and len(close_calls) == 1:
            raise RuntimeError("borrower close took effect before local failure")

    def complete(request):
        assert request.status is protocol.TaskReplyStatus.SUCCEEDED
        assert f.pending.nested_imports is None
        assert f.pending.discovery.source_references == ()
        assert child.closed and child.closes == 1
        return f.complete(request)

    monkeypatch.setattr(OutputDiscoverySession, "release_sources_after_promotions", release_then_raise)
    monkeypatch.setattr(imports, "close", close_then_raise)
    f.on_complete = complete
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    outputs = pending.outputs
    assert pending.prepare_acked and pending.failure_reply is None
    assert pending.complete_envelope is None and pending.nested_imports is imports
    assert pending.discovery.source_references == ()
    assert child.closes == int(phase == "imports")
    assert f.executions == reductions == [True]
    assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
    assert COMPLETE_WORKER_LEASE_HANDLER not in f.handlers
    before_resume = tuple(f.calls)
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert tuple(f.calls[:-1]) == before_resume and f.handlers[-1] == COMPLETE_WORKER_LEASE_HANDLER
    assert len(f.prepares) == 1 and len(source_calls) == 2
    assert source_calls[0] is source_calls[1]
    assert len(close_calls) == (2 if phase == "imports" else 1)
    assert child.closed and child.closes == 1 and pending.nested_imports is None
    assert pending.outputs is outputs and not f.worker._prepared_output_replies
    assert f.executions == reductions == [True]
    assert f.worker._handle_push_task(f.push) is reply
    assert len(source_calls) == 2 and child.closes == 1


@pytest.mark.parametrize("threshold", (0, 65536))
def test_rejected_prepare_freezes_failure_complete_until_exact_rollback_ack(monkeypatch, threshold):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=threshold)
    completions, outcomes = [], []
    f.on_prepare = lambda request: wire.PreparedOutputPublicationReply(
        request.request_identity, False, wire.OutputPublicationRPCErrorKind.INVALID_STATE,
        "step declined after unknown effect",
    )

    def complete(request):
        completions.append(request)
        assert request.status is protocol.TaskReplyStatus.SYSTEM_ERROR
        assert child.closes == 0 and not child.closed
        assert f.pending.nested_imports is imports
        return _completion(request, accepted=len(completions) > 1)

    def outcome(request):
        outcomes.append(request)
        return replace(_outcome(f, request), state=protocol.LeaseExecutionState.COMPLETED,
                       completion_status=protocol.TaskReplyStatus.SYSTEM_ERROR)

    f.on_complete, f.on_outcome = complete, outcome
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    failure = pending.failure_reply
    assert failure.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
    assert f.executions == reductions == [True] and not child.closed
    reply = f.worker._handle_push_task(f.push)
    assert reply is failure and reply.output_publication is None
    assert len(f.prepares) == 1 and len(completions) == 2 and len(outcomes) == 1
    assert completions[0] == completions[1]
    assert child.closed and child.closes == 1 and pending.nested_imports is None
    assert not f.worker._prepared_output_replies
    assert f.worker._cached_output_manifests[f.key] == pending.outputs.manifest
    assert f.worker._handle_push_task(f.push) is failure
    assert f.executions == reductions == [True]


# These parameterized cases replace the two legacy tier-specific witness tests.
# Exact successful outcome is already Complete: replay must issue no extra
# success-Complete RPC, unlike the obsolete single-return projection path.
@pytest.mark.parametrize("threshold", (0, 65536))
@pytest.mark.parametrize("metadata_only", (False, True))
def test_successful_complete_witness_overrides_abort_without_forward_or_complete_replay(monkeypatch, threshold, metadata_only):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=threshold)
    successful, completions, outcomes = [], [], []
    f.on_prepare = lambda request: wire.PreparedOutputPublicationReply(
        request.request_identity, False, wire.OutputPublicationRPCErrorKind.CONFLICT,
        "lease already completed",
    )

    def complete(request):
        completions.append(request)
        assert request.status is protocol.TaskReplyStatus.SYSTEM_ERROR
        assert not child.closed and f.pending.nested_imports is imports
        return _completion(request, accepted=False)

    def outcome(request):
        outcomes.append(request)
        assert not child.closed
        successful.append(_envelope(f.prepared_request))
        return (_outcome(f, request, witness=successful[0].complete) if metadata_only
                else _outcome(f, request, successful[0]))

    f.on_complete, f.on_outcome = complete, outcome
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert reply.output_publication == successful[0]
    assert not hasattr(reply, "inline_publication") and not hasattr(reply, "stored_publication")
    assert f.executions == reductions == [True] and child.closes == 1
    assert f.handlers == [START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER,
                          COMPLETE_WORKER_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER]
    assert len(completions) == len(outcomes) == 1
    assert f.key in f.worker._completion_acked and not f.worker._prepared_output_replies
    assert f.worker._handle_push_task(f.push) is reply and len(f.calls) == 4


@pytest.mark.parametrize("threshold", (0, 65536))
@pytest.mark.parametrize("metadata_only", (False, True))
def test_known_success_retries_import_close_effect_then_error_without_abort_or_rpc(monkeypatch, threshold, metadata_only):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=threshold)
    f.on_prepare = lambda request: wire.PreparedOutputPublicationReply(
        request.request_identity, False, wire.OutputPublicationRPCErrorKind.CONFLICT,
        "lease already completed",
    )
    f.on_complete = lambda request: _completion(request, accepted=False)
    queries, close_calls = [], []
    original_close = imports.close

    def outcome(request):
        queries.append(request)
        assert not child.closed
        envelope = _envelope(f.prepared_request)
        return (_outcome(f, request, witness=envelope.complete) if metadata_only
                else _outcome(f, request, envelope))

    def close_then_raise():
        close_calls.append(True)
        original_close()
        if len(close_calls) == 1:
            raise RuntimeError("borrower close took effect before local failure")

    f.on_outcome = outcome
    monkeypatch.setattr(imports, "close", close_then_raise)
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    outputs, envelope = pending.outputs, pending.complete_envelope
    assert envelope is not None and pending.prepare_acked and pending.failure_reply is None
    assert pending.nested_imports is imports and pending.discovery.source_references == ()
    assert child.closed and child.closes == 1 and close_calls == [True]
    assert f.key in f.worker._completion_acked and f.key not in f.worker._replies
    assert f.executions == reductions == [True]
    rpc_count = len(f.calls)
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED and reply.output_publication == envelope
    assert len(f.calls) == rpc_count and len(f.prepares) == len(queries) == 1
    assert close_calls == [True, True] and child.closes == 1
    assert pending.nested_imports is None and pending.outputs is outputs
    assert not f.worker._prepared_output_replies
    assert f.worker._handle_push_task(f.push) is reply and len(f.calls) == rpc_count
    assert close_calls == [True, True] and f.executions == reductions == [True]


@pytest.mark.parametrize("failure", ("lost", "rebound", "rejected"))
def test_node_failed_complete_withholds_ack_after_release_effect_until_exact_retry(monkeypatch, failure):
    fixture, node, record, complete = _node()
    fixture.fault = "prepare"
    with pytest.raises(TimeoutError, match="lost-ACK"):
        node._handle_prepare_output_publication(wire.PrepareOutputPublication(
            fixture.manifest, fixture.values.payloads,
        ))
    journal = fixture.journal
    before = journal.snapshot(fixture.id)
    assert sum(effect.stage is OutputPublicationStage.PREPARE for effect in before.intents) == 1
    assert not any(ack.effect.stage is OutputPublicationStage.PREPARE for ack in before.acknowledgements)
    release_calls, allocation_releases = [], []
    actual_child_release = fixture.adapter._release_child
    actual_allocation_release = node._release_record_locked

    def release_child(address, request):
        release_calls.append(request)
        reply = actual_child_release(address, request)
        if len(release_calls) == 1:
            assert reply.released
            if failure == "lost":
                raise TransportError("child release applied; ACK lost")
            if failure == "rebound":
                return replace(reply, hold=replace(request.hold, transfer_token="other-effect"))
            return replace(reply, accepted=False, released=False, error="release ACK rejected")
        assert not reply.released and reply.accepted
        return reply

    def release_allocation(current, state):
        allocation_releases.append(current.request.lease_id)
        return actual_allocation_release(current, state)

    monkeypatch.setattr(fixture.adapter, "_release_child", release_child)
    monkeypatch.setattr(node, "_release_record_locked", release_allocation)
    request = replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    expected_error = TransportError if failure == "lost" else ValueError
    with pytest.raises(expected_error, match="ACK lost|identity|rejected"):
        node._handle_complete_worker_lease_inner(request)
    assert record.state is protocol.LeaseExecutionState.COMPLETED
    assert record.completion == request and len(allocation_releases) == 1
    assert fixture.ledger.available == fixture.ledger.total
    snapshot = journal.snapshot(fixture.id)
    assert snapshot.state is OutputPublicationJournalState.ROLLING_BACK
    assert snapshot.complete is None and snapshot.rollback_tombstone is None
    pending = journal.next_rollback_effect(fixture.id)
    assert pending.stage is OutputPublicationStage.PROVISIONAL_RELEASE
    assert fixture.recovery.snapshot(fixture.id).rollback is None
    reply = node._handle_complete_worker_lease_inner(request)
    assert reply.accepted and not reply.released
    assert len(allocation_releases) == 1 and len(release_calls) == 2
    assert release_calls[0] == release_calls[1]
    assert journal.next_rollback_effect(fixture.id) is None
    snapshot = journal.snapshot(fixture.id)
    assert snapshot.state is OutputPublicationJournalState.RETIRED
    assert snapshot.rollback_tombstone is not None
    assert fixture.recovery.snapshot(fixture.id).rollback == snapshot.rollback_tombstone
    replay = node._handle_complete_worker_lease_inner(request)
    assert replay.accepted and not replay.released
    assert len(allocation_releases) == 1 and len(release_calls) == 2
    fixture.assert_no_pins_or_bytes()


def test_protocol_start_incarnation_rejects_tampered_pid_epoch_and_wrong_id_kind(monkeypatch):
    f = _Fixture(monkeypatch, lambda: 1)
    incarnation = f.incarnation
    reply = protocol.StartWorkerLeaseReply(
        f.push.lease_id, protocol.LeaseExecutionState.RUNNING, True,
        node_incarnation=incarnation,
    )
    assert reply.node_incarnation == incarnation and reply.node_incarnation is not incarnation
    for name, value in (("node_pid", 0), ("registration_epoch", 0), ("node_id", WorkerID.random())):
        damaged = replace(incarnation)
        object.__setattr__(damaged, name, value)
        with pytest.raises((ValueError, TypeError, ProtocolError)):
            replace(reply, node_incarnation=damaged)
