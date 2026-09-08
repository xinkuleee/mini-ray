"""Pure proof that pre-Push Node loss uses the unified publication domain.

One task, at most four result identities, fake lease RPCs and an in-memory
owner/recovery Core. The targeted cases select noncontiguous slots 1/3, use
four one-byte descriptor payloads, and spend at most two retry-budget entries
(initial reconstruction START plus one system retry). No function execution,
thread, process, socket, timer, polling, or wait. Finalizers run synchronously.
"""

from dataclasses import replace
import hashlib
import queue

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import (
    _HomeRoute, _LocationReportState, _LeaseCancellationState, _WAKE_COORDINATOR,
)
from miniray.ids import LeaseID, NodeID, WorkerID
from miniray.output_publication import OutputPublicationID
from miniray.ownership import ObjectState
from miniray.recovery import TaskState
from miniray.resources import AllocationToken, ResourceVector
from miniray.targeted_reconstruction import TargetedSessionPhase
from miniray.task_outputs import TargetExecutionKey
from tests.unit._pure_core import make_pure_core, close_pure_core
from tests.unit.test_core_output_node_loss import _no_runtime


pytestmark = pytest.mark.unit


def _fixture(count, *, target=False):
    assert count in (1, 4) and (not target or count == 4)
    core = make_pure_core()
    core.gcs_address = ("gcs.invalid", 1)
    core._ready_tasks = queue.Queue()
    core._registered_functions = set()
    core._home_route = _HomeRoute(core.node_id, core.node_address, 1)
    definition = core.define_remote_function(lambda: 7)
    pending, handles = core._register_submission(
        definition, (), {}, ResourceVector({"CPU": 1}),
        num_returns=count, max_retries=2 if target else 1,
    )
    refs = handles if isinstance(handles, tuple) else (handles,)
    if target:
        # Initial publication and completion go through real owner/recovery
        # authorities; only the physical stored bytes are descriptor metadata.
        original = pending
        surviving_node = NodeID(bytes.fromhex("e2" * 16))
        descriptors = tuple(
            protocol.ResultDescriptor(
                output_id, protocol.ResultStorage.OBJECT_STORE, 1,
                core.worker_id, surviving_node,
                hashlib.sha256(bytes((index,))).hexdigest(),
            )
            for index, output_id in enumerate(original.output_ids)
        )
        assert core.owner_table.publish_task_outputs(original.execution, descriptors)
        core._stored_descriptors.update({item.object_id: item for item in descriptors})
        core._recovery.record_task_success(original.task_id, original.spec.attempt_id)
        for output in original.output_ids:
            core._wake_object(output)
        for _ in original.output_ids:
            assert core._submissions.get_nowait() is _WAKE_COORDINATOR
            core._submissions.task_done()
        # _register_submission returns the value; submit(), not this pure
        # registration helper, owns enqueueing it for dispatch.
        assert core._submissions.empty()
        assert core._finish_pending_task(original)
        assert core._submissions.get_nowait() is _WAKE_COORDINATOR
        core._submissions.task_done()
        assert core._submissions.empty()
        coordinator = core._targeted_reconstruction_coordinator()
        for output_id in (original.output_ids[1], original.output_ids[3]):
            assert core.owner_table.mark_lost(output_id, original.spec.attempt_id)
            coordinator.request(output_id, original.spec.attempt_id)
        assert core._recovery.task_record(original.task_id).retries_started == 0
        core._start_open_targeted_reconstruction(original.task_id)
        pending = core._submissions.get_nowait()
        core._submissions.task_done()
        assert core._submissions.empty()
        assert pending.output_ids == (refs[1].object_id, refs[3].object_id)
        assert pending.full_output_ids == original.output_ids
        assert pending.spec.attempt_id == original.spec.attempt_id.next()
        assert coordinator.current_session(original.task_id).execution == pending.execution
    death = protocol.NodeDeathRecord(
        "pre-push-exit", core.node_id, 1201, 1, 2, 5,
        protocol.NodeDeathReason.PROCESS_EXIT, "confirmed Node exit",
    )
    return core, pending, refs, death


def _finish_fixture(core, refs):
    for ref in refs:
        ref._closed = True
        ref._finalizer()
        assert ref._release_done.is_set()
    close_pure_core(core)


@pytest.mark.parametrize("count", (1, 4))
@pytest.mark.parametrize("phase", ("lease-send", "grant-before-push", "location-replay", "cancel"))
def test_pre_push_loss_queries_one_output_domain_before_budgeted_retry(count, phase):
    _assert_pre_push_loss(count, phase)


@pytest.mark.parametrize("phase", ("lease-send", "grant-before-push", "location-replay", "cancel"))
def test_targeted_pre_push_loss_preserves_exact_subset_and_healthy_siblings(phase):
    _assert_pre_push_loss(4, phase, target=True)


def _assert_pre_push_loss(count, phase, *, target=False):
    core, pending, refs, death = _fixture(count, target=target)
    calls = []
    healthy = tuple(ref.object_id for ref in refs if ref.object_id not in pending.output_ids)
    healthy_before = tuple(core.owner_table.snapshot(output) for output in healthy)
    healthy_routes = tuple(core._stored_descriptors[output] for output in healthy)
    before_owner = tuple(core.owner_table.snapshot(ref.object_id) for ref in refs)
    before_record = replace(core._recovery.task_record(pending.task_id))
    coordinator = core._targeted_reconstruction_coordinator()
    before_session = coordinator.current_session(pending.task_id)
    if target:
        assert isinstance(pending.execution, TargetExecutionKey)
        assert pending.target_execution == pending.execution
        assert pending.execution.target_output_ids == (refs[1].object_id, refs[3].object_id)
        assert pending.execution.full_output_ids == tuple(ref.object_id for ref in refs)
        assert before_record.state is TaskState.RETRY_PENDING and before_record.retries_started == 1
        assert before_record.retries_remaining == 1
        assert before_session.phase is TargetedSessionPhase.STARTED
    lease = LeaseID.random()
    worker = WorkerID.random()
    request = protocol.RequestWorkerLease(
        lease, pending.task_id, pending.spec.attempt_id, pending.spec.resources,
        core.node_id, core.worker_id, return_ids=pending.output_ids,
        target_execution=pending.target_execution,
    )
    grant = protocol.GrantWorkerLease(
        lease, pending.task_id, pending.spec.attempt_id, core.node_id, worker,
        ("worker.invalid", 1), AllocationToken("pre-push-test-grant"),
        target_execution=pending.target_execution,
    )
    assert request.return_ids == pending.output_ids
    assert request.target_execution == grant.target_execution == pending.target_execution

    def rpc(_address, handler, message):
        calls.append((handler, message))
        assert _address == core.gcs_address
        assert handler == wire.GET_OUTPUT_NODE_LOSS_HANDLER
        assert type(message) is wire.GetOutputNodeLoss
        assert message.owner_worker_id == core.worker_id and message.node_death == death
        expected_lease = received_lease[0].lease_id if received_lease else lease
        assert message.publication_id == OutputPublicationID(expected_lease, pending.execution)
        assert message.publication_id.execution == pending.execution
        assert message.publication_id.output_ids == pending.output_ids
        assert message.publication_id.full_output_ids == tuple(ref.object_id for ref in refs)
        assert tuple(core.owner_table.snapshot(ref.object_id) for ref in refs) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        assert coordinator.current_session(pending.task_id) == before_session
        # No Worker was pushed, so the frozen registry is precisely absent.
        return wire.GetOutputNodeLossReply(message, False)

    core._rpc = rpc
    received_lease = []

    def lease_rpc(state):
        received_lease.append(state.request)
        assert type(state.request) is protocol.RequestWorkerLease
        assert state.request.return_ids == pending.output_ids
        assert state.request.target_execution == pending.target_execution
        assert state.request.task_id == pending.task_id
        assert state.request.attempt_id == pending.spec.attempt_id
        if target:
            assert state.request.target_execution.full_output_ids == tuple(ref.object_id for ref in refs)
            assert state.request.target_execution.target_output_ids == pending.output_ids
        marker = core._protocol_unresolved[pending.task_key]
        assert marker.output_candidate == OutputPublicationID(state.request.lease_id, pending.execution)
        assert not hasattr(marker, "inline_candidate") and not hasattr(marker, "stored_candidate")
        core._dead_nodes[core.node_id] = death
        if phase == "lease-send":
            raise TimeoutError("lease reply lost after Node exit")
        reply = replace(grant, lease_id=state.request.lease_id)
        assert reply.target_execution == state.request.target_execution
        return reply

    core._request_lease_hop = lease_rpc
    location = None
    if phase in ("location-replay", "cancel"):
        core._dead_nodes[core.node_id] = death
        location = _LocationReportState(grant, core.node_address, (), lease_request=request)
    try:
        if phase == "cancel":
            cancellation = _LeaseCancellationState(
                protocol.CancelWorkerLease(lease, pending.task_id, pending.spec.attempt_id, core.node_id, core.worker_id),
                core.node_address, RuntimeError("unresolved lease"), target_node_id=core.node_id,
                lease_request=request, known_grant=grant,
            )
            # Cancellation has already selected a terminal error. Node death
            # discharges that exact pre-Push obligation; it is not permission
            # to start a new attempt merely because no cancel ACK could arrive.
            assert core._resolve_lease_cancellation(pending, pending.spec, (), cancellation)
            assert not calls and not core._protocol_unresolved and core._ready_tasks.empty()
            record = core._recovery.task_record(pending.task_id)
            assert record.current_attempt == before_record.current_attempt
            assert record.retries_started == before_record.retries_started
            for output in pending.output_ids:
                snapshot = core.owner_table.snapshot(output)
                assert snapshot.current_attempt == pending.spec.attempt_id
                assert snapshot.state is ObjectState.ERROR and snapshot.error is cancellation.terminal_error
            assert tuple(core.owner_table.snapshot(output) for output in healthy) == healthy_before
            assert tuple(core._stored_descriptors[output] for output in healthy) == healthy_routes
            assert not received_lease
            return
        else:
            assert not core._execute(pending, pending.spec, location_state=location)
        marker = core._protocol_unresolved[pending.task_key]
        expected_lease = received_lease[0].lease_id if received_lease else lease
        assert marker.output_candidate == OutputPublicationID(expected_lease, pending.execution)
        assert not hasattr(marker, "inline_candidate") and not hasattr(marker, "stored_candidate")
        ready = core._ready_tasks.get_nowait()
        core._ready_tasks.task_done()
        assert ready.output_node_loss is not None
        assert ready.output_node_loss.publication_id == marker.output_candidate
        assert ready.output_node_loss.node_death == death
        assert not hasattr(ready, "inline_node_loss") and not hasattr(ready, "stored_node_loss")
        assert calls == []
        assert tuple(core.owner_table.snapshot(ref.object_id) for ref in refs) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        assert not core._execute(pending, pending.spec, output_node_loss=ready.output_node_loss)
        assert [handler for handler, _request in calls] == [wire.GET_OUTPUT_NODE_LOSS_HANDLER]
        assert not core._protocol_unresolved
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == pending.spec.attempt_id.next()
        assert record.retries_started == before_record.retries_started + 1
        assert record.retries_remaining == 0
        assert record.state is TaskState.RETRY_PENDING
        assert all(core.owner_table.snapshot(output).current_attempt == record.current_attempt for output in pending.output_ids)
        assert tuple(core.owner_table.snapshot(output) for output in healthy) == healthy_before
        assert tuple(core._stored_descriptors[output] for output in healthy) == healthy_routes
        if target:
            assert tuple(item.current_attempt for item in healthy_before) == (
                before_session.losses[0].expected_attempt,
            ) * 2
            assert all(item.state is ObjectState.READY_STORED for item in healthy_before)
            assert all(core._objects[output].event.is_set() for output in healthy)
            retry_session = coordinator.current_session(pending.task_id)
            assert retry_session.phase is TargetedSessionPhase.STARTED
            assert retry_session.losses == before_session.losses
            assert retry_session.execution == pending.execution.for_attempt(record.current_attempt)
            assert coordinator.queued_losses(pending.task_id) == ()
            assert core._recovery.active_recovery(pending.task_id) == record.current_attempt
            retried = core._submissions.get_nowait()
            core._submissions.task_done()
            assert core._submissions.empty()
            assert retried.target_execution == retry_session.execution
            assert retried.output_ids == pending.output_ids
            assert retried.full_output_ids == pending.full_output_ids
            assert retried.task_key == pending.task_key
            assert retried.reconstruction_origin_attempt == pending.reconstruction_origin_attempt
            assert all(core._task_finish_barriers[output] is retried for output in pending.output_ids)
            assert all(output not in core._task_finish_barriers for output in healthy)
            assert all(core.owner_table.snapshot(output).state is ObjectState.PENDING for output in pending.output_ids)
            assert all(output not in core._stored_descriptors for output in pending.output_ids)
            assert retried.execution.manifest == marker.output_candidate.execution.manifest
            assert retried.execution.attempt_id != marker.output_candidate.attempt_id
            before_replay = replace(record)
            before_replay_owner = tuple(core.owner_table.snapshot(ref.object_id) for ref in refs)
            before_calls = tuple(calls)
            # Duplicate old pre-Push loss is now obsolete: no second query or
            # budget charge may disturb the already queued targeted retry.
            assert core._execute(pending, pending.spec, output_node_loss=ready.output_node_loss)
            assert tuple(calls) == before_calls
            assert core._recovery.task_record(pending.task_id) == before_replay
            assert tuple(core.owner_table.snapshot(ref.object_id) for ref in refs) == before_replay_owner
            assert coordinator.current_session(pending.task_id) is retry_session
            assert not core._protocol_unresolved and core._submissions.empty()
        assert bool(received_lease) is (phase not in ("location-replay", "cancel"))
    finally:
        _finish_fixture(core, refs)
