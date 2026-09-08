"""Pure submitting Core -> Node -> graph/owner unified publication loop."""

from dataclasses import replace
import socket
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import CoreWorker, _DelayedReadyTask, _PendingTask, _PushRequestState
from miniray.ownership import ObjectState
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecutionKey
from tests.unit._pure_core import make_pure_core, close_pure_core
from tests.unit.test_output_publication_node_server import _node


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure Core output test attempted infrastructure")
    monkeypatch.setattr(CoreWorker, "__init__", forbidden)
    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _fixture(*, refs=True):
    fixture, node, record, complete = _node(refs=refs)
    values = fixture.values
    core = make_pure_core()
    core.job_id, core.worker_id, core.node_id = values.job, values.owner, values.node
    core.node_address, core.gcs_address = ("node.invalid", 1), ("gcs.invalid", 1)
    spec = protocol.TaskSpec(
        values.job, values.task, values.attempt,
        protocol.FunctionKey(values.job, __name__, "producer", "v1"),
        (), 2, ResourceVector({"CPU": 1}), values.owner, max_retries=3,
    )
    core._owner_table.register_task_outputs(spec, local_tokens=("outer0", "outer1"))
    core._recovery.register_task(spec, max_retries=3)
    from miniray.core import _ObjectWaiter
    core._objects = {object_id: _ObjectWaiter(threading.Event()) for object_id in spec.return_ids()}
    pending = _PendingTask(spec.return_ids()[0], spec)
    core._accepted_task_count = 1
    core._install_task_finish_barrier_locked(pending)
    core._registered_functions = set()
    core._resolve_node_address = lambda node_id: core.node_address if node_id == values.node else pytest.fail("other node")
    calls = []

    def rpc(address, handler, request):
        calls.append((handler, request))
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            if isinstance(request, wire.ReportOutputPublicationTerminal):
                ack = fixture.recovery.report_terminal(request.witness)
            elif isinstance(request, wire.ReportOutputPublicationAdopted):
                ack = fixture.recovery.report_adopted(request.proof)
            elif isinstance(request, wire.ReportOutputPublicationSlotCollected):
                ack = fixture.recovery.report_slot_collected(request.proof)
            else:
                pytest.fail("unexpected report")
            return wire.OutputRecoveryReply(request, ack)
        if handler == "commit_contained_graph":
            return protocol.ContainedGraphReply(request, fixture.graph.commit_manifest(request.manifest))
        if handler == "release_contained_graph_container":
            return protocol.ContainedGraphReply(request, fixture.graph.release_manifest_container(request.manifest, request.container_object_id))
        if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
            return node._handle_ack_output_publication_adopted(request)
        if handler == "drop_object_replica":
            return node._handle_drop_object_replica(request)
        pytest.fail("unexpected Core RPC: " + handler)

    core._rpc = rpc
    core._borrow_rpc = lambda address, handler, request: fixture.release_child(address, request)
    node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, values.payloads))
    completed = node._handle_complete_worker_lease_inner(complete)
    envelope = completed.output_publication
    reply = protocol.TaskReply(spec.task_id, spec.attempt_id, values.executor, protocol.TaskReplyStatus.SUCCEEDED,
                               envelope.results, output_publication=envelope)
    return fixture, node, core, pending, reply, calls, rpc


@pytest.mark.parametrize("refs", (False, True))
def test_core_adopts_all_slots_and_gc_releases_only_each_slots_children(refs):
    fixture, node, core, pending, reply, calls, _rpc = _fixture(refs=refs)
    try:
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert not core._protocol_unresolved
        assert core._owner_table.snapshot(pending.output_ids[0]).state is ObjectState.READY_INLINE
        assert core._owner_table.snapshot(pending.output_ids[1]).state is ObjectState.READY_STORED
        assert not fixture.journal.snapshot(fixture.id).retained_result_slots
        assert fixture.store.used_bytes > 0
        assert core._finish_pending_task(pending)
        for index, object_id in enumerate(pending.output_ids):
            core.owner_table.release_local_reference(object_id, "outer{}".format(index))
            core._reference_released(object_id)
            assert not core.owner_table.contains(object_id)
            if index == 0:
                assert core.owner_table.contains(pending.output_ids[1])
        assert fixture.store.used_bytes == 0
        assert not fixture.graph.snapshot().committed_edges
        assert not core._object_gc_obligations
    finally:
        for object_id in tuple(core._objects):
            for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
                core.owner_table.release_local_reference(object_id, token)
        close_pure_core(core)


def test_lost_adoption_ack_keeps_finish_barrier_and_replays_without_second_cas():
    fixture, node, core, pending, reply, calls, rpc = _fixture()
    lost = [False]

    def lose_ack(address, handler, request):
        result = rpc(address, handler, request)
        if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER and not lost[0]:
            lost[0] = True
            raise TimeoutError("Node retired payload; ACK lost")
        return result

    core._rpc = lose_ack
    try:
        assert not core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert core.owner_table.snapshot(pending.output_ids[0]).state is ObjectState.READY_INLINE
        assert not core._finish_pending_task(pending)
        delayed = None
        for _ in range(16):
            item = core._submissions.get_nowait()
            if isinstance(item, _DelayedReadyTask):
                delayed = item
                break
        assert delayed is not None and delayed.ready.output_adoption is not None
        assert core._execute(pending, pending.spec, output_adoption=delayed.ready.output_adoption)
        assert core._finish_pending_task(pending)
        assert not core._protocol_unresolved
    finally:
        for object_id in tuple(core._objects):
            for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
                core.owner_table.release_local_reference(object_id, token)
        close_pure_core(core)


def test_owner_commit_effect_then_error_repairs_recovery_and_routes_on_exact_replay(monkeypatch):
    fixture, node, core, pending, reply, _calls, _rpc = _fixture()
    original = core.owner_table.commit_output_publication
    applied = []

    def commit(plan):
        result = original(plan)
        applied.append(True)
        raise RuntimeError("owner CAS applied before local error")

    monkeypatch.setattr(core.owner_table, "commit_output_publication", commit)
    try:
        assert not core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert core.owner_table.snapshot(pending.output_ids[0]).state is ObjectState.READY_INLINE
        assert core._recovery.task_record(pending.task_id).state.value != "SUCCEEDED"
        delayed = next(item for item in tuple(core._submissions.queue) if isinstance(item, _DelayedReadyTask))
        assert core._execute(pending, pending.spec, output_adoption=delayed.ready.output_adoption)
        assert applied == [True]
        assert core._recovery.task_record(pending.task_id).state.value == "SUCCEEDED"
        assert pending.output_ids[1] in core._stored_descriptors
        assert core._finish_pending_task(pending)
    finally:
        for object_id in tuple(core._objects):
            for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
                core.owner_table.release_local_reference(object_id, token)
        close_pure_core(core)


@pytest.mark.parametrize("source", ("custody", "owner-slots", "missing"))
def test_metadata_only_successful_outcome_never_falls_back_to_system_retry(monkeypatch, source):
    fixture, node, core, pending, reply, calls, rpc = _fixture(refs=False)
    identity = fixture.id
    record = node._leases[identity.lease_id]
    push = protocol.PushTask(identity.lease_id, fixture.values.executor, pending.spec)
    state = _PushRequestState(push, record.grant, core.node_address, record.grant.worker_address)
    witness = reply.output_publication.complete
    core._mark_protocol_unresolved(pending, "push-outcome", output_candidate=identity)
    if source == "custody":
        core._output_result_custody = {identity: reply.output_publication}
    elif source == "owner-slots":
        from miniray.ownership import OutputOwnerPublicationPlan
        core.owner_table.commit_output_publication(OutputOwnerPublicationPlan(pending.execution, reply.output_publication))
    def outcome_rpc(address, handler, request):
        if handler == "get_worker_lease_outcome":
            return protocol.GetWorkerLeaseOutcomeReply(
                request.lease_id, request.task_id, request.attempt_id, request.executor_worker_id,
                request.owner_worker_id, request.object_ids, node.node_id, True, True,
                state=protocol.LeaseExecutionState.COMPLETED, completion_status=protocol.TaskReplyStatus.SUCCEEDED,
                output_completion=witness,
            )
        return rpc(address, handler, request)
    core._rpc = outcome_rpc
    monkeypatch.setattr(core, "_retry_system_failure", lambda *_: pytest.fail("known Complete became ordinary retry"))
    try:
        complete = core._resolve_ambiguous_push_outcome(pending, state)
        assert complete is (source != "missing")
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        if source == "missing":
            assert pending.task_key in core._protocol_unresolved
            assert all(core.owner_table.snapshot(output).state is ObjectState.PENDING for output in pending.output_ids)
            assert any(isinstance(item, _DelayedReadyTask) for item in tuple(core._submissions.queue))
        else:
            assert core.owner_table.snapshot(pending.output_ids[0]).inline_data == fixture.values.payloads[0]
            assert core.owner_table.snapshot(pending.output_ids[1]).state is ObjectState.READY_STORED
            assert core._finish_pending_task(pending)
    finally:
        for object_id in tuple(core._objects):
            for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
                core.owner_table.release_local_reference(object_id, token)
        close_pure_core(core)


def test_descriptor_only_ordinary_success_cannot_publish_or_clear_push_fence():
    fixture, node, core, pending, reply, _calls, _rpc = _fixture(refs=False)
    from miniray.errors import SystemTaskError
    legacy_shape = replace(reply, output_publication=None)
    core._mark_protocol_unresolved(pending, "push-send", output_candidate=fixture.id)
    before = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
    try:
        with pytest.raises(SystemTaskError, match="selected-output"):
            core._validate_ordinary_reply_domain(legacy_shape)
        with pytest.raises(SystemTaskError, match="selected-output"):
            core._publish_reply(pending, legacy_shape, expected_node_id=node.node_id)
        assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before
        assert pending.task_key in core._protocol_unresolved
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert core._finish_pending_task(pending)
    finally:
        for object_id in tuple(core._objects):
            for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
                core.owner_table.release_local_reference(object_id, token)
        close_pure_core(core)
