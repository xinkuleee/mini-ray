"""Bounded Core -> Node -> child-owner single-output publication.

One accepted Task, one 1 KiB store, at most two child transfers, and one
explicit replay. Real owner/handoff/recovery/journal/GC transitions run without
constructors, sockets, threads, sleeps, blocking waits, or user execution.
"""

from copy import deepcopy
from dataclasses import replace
import socket
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import _HomeRoute
from miniray.core import CoreWorker, _DelayedReadyTask, _ObjectWaiter, _PendingTask, _PushRequestState, _WAKE_COORDINATOR
from miniray.errors import ProtocolError, SystemTaskError
from miniray.output_handoff import OutputHandoffPhase
from miniray.ownership import ObjectCollectionState, ObjectState, OutputOwnerPublicationPlan
from miniray.resources import ResourceVector
from tests.unit._pure_core import make_pure_core, close_pure_core
from tests.unit.test_output_publication_node_server import _node


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure Core output test attempted infrastructure")
    monkeypatch.setattr(CoreWorker, "__init__", forbidden)
    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Event, "wait", forbidden)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _fixture(*, refs=True, stored=True, report_complete=True):
    fixture, node, record, complete = _node(refs=refs, stored=stored)
    values = fixture.values
    core = make_pure_core()
    core.job_id, core.worker_id, core.node_id = values.job, values.owner, values.node
    core.node_address, core.owner_address = ("node.invalid", 1), ("owner.invalid", 1)
    core._home_route = _HomeRoute(core.node_id, core.node_address, core._membership_epoch)
    core.gcs_address = None
    spec = protocol.TaskSpec(
        values.job, values.task, values.attempt,
        protocol.FunctionKey(values.job, __name__, "producer", "v1"),
        (), 1, ResourceVector({"CPU": 1}), values.owner, max_retries=3,
    )
    core.owner_table.register_task_outputs(spec, local_tokens=("outer0",))
    core._recovery.register_task(spec, max_retries=3)
    core._objects = {spec.return_ids()[0]: _ObjectWaiter(threading.Event())}
    pending = _PendingTask(spec.return_ids()[0], spec)
    with core._state_lock:
        core._install_task_finish_barrier_locked(pending)
        core._accepted_task_count += 1
        core._enqueue_reconstruction_task(pending)
    assert core._submissions.get_nowait() is pending
    core._submissions.task_done()
    core._registered_functions = set()
    core._resolve_node_address = lambda node_id, *, home_route=None: core.node_address if node_id == values.node else pytest.fail("other node")
    calls, owner_calls = [], []

    def rpc(address, handler, request):
        assert not core._state_lock._is_owned()
        calls.append((handler, request))
        assert address == core.node_address
        if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
            return node._handle_ack_output_publication_adopted(request)
        if handler == "drop_object_replica":
            return node._handle_drop_object_replica(request)
        if handler == "get_worker_lease_outcome":
            return node._handle_get_worker_lease_outcome(request)
        pytest.fail("unexpected Core RPC: " + handler)

    def borrow_rpc(address, handler, request):
        assert not core._state_lock._is_owned()
        assert handler == "release_contained_reference"
        calls.append((handler, request))
        return fixture.release_child(address, request)

    def background_rpc(address, handler, request):
        assert not node._state_lock._is_owned()
        assert not fixture.journal._lock._is_owned()
        owner_handlers = {
            wire.REGISTER_OUTPUT_HANDOFF_HANDLER: core.register_output_handoff,
            wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER: core.report_output_handoff_complete,
            wire.REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER: core.report_output_handoff_rollback,
        }
        if handler in owner_handlers:
            assert address == core.owner_address
            owner_calls.append((handler, request))
            return owner_handlers[handler](request)
        if handler == "prepare_stored_contained_pin":
            return fixture.prepare_child(address, request)
        if handler == "promote_stored_contained_pin":
            return fixture.promote_child(address, request)
        if handler == "release_contained_reference":
            return fixture.release_child(address, request)
        pytest.fail("unexpected Node RPC: " + handler)

    core._rpc, core._borrow_rpc = rpc, borrow_rpc
    record.request = replace(record.request, requester_owner_address=core.owner_address)
    node._background_rpc = background_rpc
    node._output_publications = fixture.adapter = node._make_output_publication_adapter()
    fixture.handoffs, fixture.owner_calls = core._output_handoff_table(), owner_calls
    prepared = node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (values.payload)))
    assert prepared.accepted
    assert fixture.handoffs.query(fixture.id).manifest == fixture.manifest
    completed = node._handle_complete_worker_lease_inner(complete)
    assert completed.accepted and completed.released
    envelope = completed.output_publication
    assert envelope == values.envelope
    if report_complete:
        assert node._drive_output_publications()
        assert fixture.handoffs.query(fixture.id).complete == envelope.complete
    else:
        assert fixture.handoffs.query(fixture.id).complete is None
    reply = protocol.TaskReply(spec.task_id, spec.attempt_id, values.executor, protocol.TaskReplyStatus.SUCCEEDED,
                               ((envelope.result,)), output_publication=envelope)
    return fixture, node, core, pending, reply, calls, rpc


def _close(core):
    for object_id in tuple(core._objects):
        for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
            assert core.owner_table.release_local_reference(object_id, token)
    close_pure_core(core)


def _take_adoption(core):
    queued = core._submissions.qsize()
    assert queued <= 8
    delayed = []
    for _ in range(queued):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if isinstance(item, _DelayedReadyTask):
            delayed.append(item)
        else:
            assert item is _WAKE_COORDINATOR
    assert len(delayed) == 1 and delayed[0].ready.output_adoption is not None
    return delayed[0].ready.output_adoption


@pytest.mark.parametrize("refs", (False, True))
@pytest.mark.parametrize("stored", (False, True))
def test_core_adopts_single_output_and_gc_releases_exact_children(refs, stored):
    fixture, node, core, pending, reply, calls, _rpc = _fixture(refs=refs, stored=stored)
    try:
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert not core._protocol_unresolved
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is (ObjectState.READY_STORED if stored else ObjectState.READY_INLINE)
        assert snapshot.output_publication.manifest == fixture.manifest
        assert fixture.handoffs.query(fixture.id).phase is OutputHandoffPhase.ADOPTED
        assert not fixture.journal.snapshot(fixture.id).result_retained
        assert fixture.store.used_bytes == (len((fixture.values.payload)) if stored else 0)
        for transfer in (fixture.manifest.value).transfers:
            child = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
            assert transfer.final_hold in child.contained_holds
            assert transfer.provisional_hold not in child.contained_holds
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert core.owner_table.release_local_reference(pending.object_id, "outer0")
        core._reference_released(pending.object_id)
        assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
        assert not core.owner_table.contains(pending.object_id)
        assert core._recovery.lineage_for_object(pending.object_id) is None
        fixture.assert_no_pins_or_bytes()
        for transfer in (fixture.manifest.value).transfers:
            assert fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id).local_tokens == frozenset(("source-live",))
        assert not core._objects and not core._stored_descriptors and not core._object_gc_obligations
        assert sum(handler == "drop_object_replica" for handler, _ in calls) == int(stored)
        assert sum(handler == "release_contained_reference" for handler, _ in calls) == (2 if refs else 0)
    finally:
        _close(core)


@pytest.mark.parametrize("refs", (False, True))
@pytest.mark.parametrize("fault", ("recovery-preflight", "owner-final-tamper"))
def test_final_owner_validation_fences_failures_without_early_recovery_commit(monkeypatch, refs, fault):
    fixture, node, core, pending, reply, calls, _rpc = _fixture(refs=refs, stored=False)
    try:
        owner_before = core.owner_table.snapshot(pending.object_id)
        recovery_before = replace(core._recovery.task_record(pending.task_id))
        active_before = core._recovery.active_recovery(pending.task_id)
        physical_before = fixture.journal.snapshot(fixture.id), fixture.store.used_bytes
        child_before = {
            transfer.contained_object_id: fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
            for transfer in fixture.manifest.value.transfers
        }
        original_reply = deepcopy(reply)
        validate = core._recovery.validate_task_success
        commit = core.owner_table.commit_output_publication
        events = []

        def recovery_preflight(task_id, attempt_id):
            transition = validate(task_id, attempt_id)
            events.append("recovery-preflight")
            assert transition.decision.action.value == "ACCEPT_SUCCESS"
            assert core.owner_table.snapshot(pending.object_id) == owner_before
            assert core._recovery.task_record(pending.task_id) == recovery_before
            if fault == "recovery-preflight":
                raise RuntimeError("recovery copy preflight failed before owner commit")
            return transition

        def owner_final(plan):
            assert events == ["recovery-preflight"]
            assert core._recovery.task_record(pending.task_id) == recovery_before
            payload = plan.envelope.result.inline_data
            assert payload
            changed = bytes((payload[0] ^ 1,)) + payload[1:]
            object.__setattr__(plan.envelope.result, "inline_data", changed)
            with pytest.raises(ProtocolError):
                commit(plan)
            events.append("owner-final-rejected")
            raise RuntimeError("owner final validation rejected same-size corrupted payload")

        with monkeypatch.context() as patch:
            patch.setattr(core._recovery, "validate_task_success", recovery_preflight)
            patch.setattr(core._recovery, "commit_validated_transition", lambda *_: pytest.fail("recovery committed before owner success"))
            patch.setattr(core.owner_table, "validate_output_publication", lambda *_: pytest.fail("redundant owner preflight still called"))
            patch.setattr(core.owner_table, "commit_output_publication", owner_final if fault == "owner-final-tamper" else lambda *_: pytest.fail("owner commit ran after recovery preflight failure"))
            assert not core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert events == (["recovery-preflight"] if fault == "recovery-preflight" else ["recovery-preflight", "owner-final-rejected"])
        assert core.owner_table.snapshot(pending.object_id) == owner_before
        assert core._recovery.task_record(pending.task_id) == recovery_before
        assert core._recovery.active_recovery(pending.task_id) == active_before
        assert not core._stored_descriptors and not core.owner_table._output_publication_receipts
        assert (fixture.journal.snapshot(fixture.id), fixture.store.used_bytes) == physical_before
        assert {
            transfer.contained_object_id: fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
            for transfer in fixture.manifest.value.transfers
        } == child_before
        assert reply == original_reply and not calls
        assert not core._objects[pending.object_id].event.is_set()
        assert not core._finish_pending_task(pending)
        obligation = _take_adoption(core)
        assert obligation.envelope == original_reply.output_publication
        assert core._execute(pending, pending.spec, output_adoption=obligation)
        assert core._recovery.task_record(pending.task_id).state.value == "SUCCEEDED"
        assert core._finish_pending_task(pending)
        assert core.owner_table.release_local_reference(pending.object_id, "outer0")
        core._reference_released(pending.object_id)
        fixture.assert_no_pins_or_bytes()
    finally:
        _close(core)


def test_lost_adoption_ack_keeps_finish_barrier_and_replays_without_second_cas(monkeypatch):
    fixture, node, core, pending, reply, calls, rpc = _fixture()
    original_commit = core.owner_table.commit_output_publication
    commits, requests = [], []

    def commit(plan):
        commits.append(plan)
        return original_commit(plan)

    def lose_ack(address, handler, request):
        result = rpc(address, handler, request)
        if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
            requests.append(request)
            if len(requests) == 1:
                raise TimeoutError("Node retired payload; ACK lost")
        return result

    monkeypatch.setattr(core.owner_table, "commit_output_publication", commit)
    core._rpc = lose_ack
    try:
        assert not core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.READY_STORED
        assert not fixture.journal.snapshot(fixture.id).result_retained
        assert not core._finish_pending_task(pending)
        assert core._task_finish_barriers == {pending.object_id: pending}
        assert core._accepted_task_count == 1 and len(commits) == 1
        assert core._execute(pending, pending.spec, output_adoption=_take_adoption(core))
        assert requests == [requests[0]] * 2 and len(commits) == 1
        assert core._finish_pending_task(pending)
        assert not core._protocol_unresolved and not core._task_finish_barriers
        assert core._accepted_task_count == 0
    finally:
        _close(core)


def test_owner_commit_effect_then_error_repairs_recovery_and_routes_on_exact_replay(monkeypatch):
    fixture, node, core, pending, reply, _calls, _rpc = _fixture()
    original = core.owner_table.commit_output_publication
    applied = []

    def commit(plan):
        result = original(plan)
        assert result.committed
        applied.append(plan)
        raise RuntimeError("owner CAS applied before local error")

    monkeypatch.setattr(core.owner_table, "commit_output_publication", commit)
    try:
        assert not core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.READY_STORED
        assert core._recovery.task_record(pending.task_id).state.value != "SUCCEEDED"
        assert pending.object_id not in core._stored_descriptors
        assert not core._objects[pending.object_id].event.is_set()
        assert core._execute(pending, pending.spec, output_adoption=_take_adoption(core))
        assert len(applied) == 1
        assert core._recovery.task_record(pending.task_id).state.value == "SUCCEEDED"
        assert core._stored_descriptors[pending.object_id] == reply.results[0]
        assert core._objects[pending.object_id].event.is_set()
        assert core._finish_pending_task(pending)
    finally:
        _close(core)


@pytest.mark.parametrize("source", ("custody", "owner", "missing"))
@pytest.mark.parametrize("stored", (False, True))
def test_metadata_only_successful_outcome_never_falls_back_to_system_retry(monkeypatch, source, stored):
    fixture, node, core, pending, reply, calls, rpc = _fixture(refs=False, stored=stored)
    identity = fixture.id
    record = node._leases[identity.lease_id]
    push = protocol.PushTask(identity.lease_id, fixture.values.executor, pending.spec)
    state = _PushRequestState(push, record.grant, core.node_address, record.grant.worker_address)
    witness = reply.output_publication.complete
    core._mark_protocol_unresolved(pending, "push-outcome", output_candidate=identity)
    if source == "custody":
        core._output_result_custody = {identity: reply.output_publication}
    elif source == "owner":
        assert core.owner_table.commit_output_publication(OutputOwnerPublicationPlan(pending.execution, reply.output_publication)).committed

    def outcome_rpc(address, handler, request):
        result = rpc(address, handler, request)
        if handler == "get_worker_lease_outcome":
            assert result.output_publication.complete == witness
            # Fault only the transport's payload delivery. The real Node
            # terminal witness remains successful; metadata cannot supply bytes.
            return replace(result, output_publication=None, output_completion=witness, descriptors=())
        return result

    core._rpc = outcome_rpc
    monkeypatch.setattr(core, "_retry_system_failure", lambda *_: pytest.fail("known Complete became ordinary retry"))
    try:
        complete = core._resolve_ambiguous_push_outcome(pending, state)
        assert complete is (source != "missing")
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        if source == "missing":
            assert pending.task_key in core._protocol_unresolved
            assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
            assert any(isinstance(item, _DelayedReadyTask) for item in tuple(core._submissions.queue))
            assert not core._finish_pending_task(pending)
        else:
            result = core.owner_table.snapshot(pending.object_id)
            assert result.state is (ObjectState.READY_STORED if stored else ObjectState.READY_INLINE)
            if stored:
                assert core._stored_descriptors[pending.object_id] == reply.results[0]
                assert fixture.store.get(pending.object_id) == (fixture.values.payload)
            else:
                assert result.inline_data == (fixture.values.payload)
            assert core._finish_pending_task(pending)
    finally:
        _close(core)


def test_descriptor_only_ordinary_success_cannot_publish_or_clear_push_fence():
    fixture, node, core, pending, reply, _calls, _rpc = _fixture(refs=False)
    legacy_shape = replace(reply, output_publication=None)
    core._mark_protocol_unresolved(pending, "push-send", output_candidate=fixture.id)
    before = core.owner_table.snapshot(pending.object_id)
    try:
        with pytest.raises(SystemTaskError, match="single-output"):
            core._validate_ordinary_reply_domain(legacy_shape)
        with pytest.raises(SystemTaskError, match="single-output"):
            core._publish_reply(pending, legacy_shape, expected_node_id=node.node_id)
        assert core.owner_table.snapshot(pending.object_id) == before
        assert pending.task_key in core._protocol_unresolved
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert core._finish_pending_task(pending)
    finally:
        _close(core)
