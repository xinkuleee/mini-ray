"""Pure owner-routed targeted admission across retryable preparation waits.

Resource review: one threadless Core/Node, one two-slot producer publication,
at most two retained requester credentials, no output-contained refs, and at
most a 4 KiB in-memory ObjectStore. One renewal WAIT or one physical-drop ACK
loss, bounded admission requests, and cached reply replay per case. No
producer, real thread/process/socket, timer, polling or blocking wait runs.
Owner/recovery/publication/retirement CASes are real; only transport and the
foreign-lineage renewal service are synchronous doubles.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
import multiprocessing as mp
import os
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import CoreWorker, _PendingTask, _WAKE_COORDINATOR
from miniray.foreign_lineage_runtime import (
    ForeignLineageRenewalDisposition, ForeignLineageRenewalResult,
)
from miniray.ids import AttemptID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.ownership import ObjectState
from miniray.recovery import TaskState
from miniray.targeted_reconstruction import TargetedSessionPhase
from tests.unit._pure_core import close_pure_core
from tests.unit.test_core_output_publication import _fixture


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    violations = []

    def forbidden(*_args, **_kwargs):
        violations.append("runtime")
        pytest.fail("pure owner-routed deferral attempted runtime work")

    for owner, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (socket, "socket"),
        (socket, "create_connection"), (subprocess, "Popen"),
        (mp.Process, "start"), (time, "sleep"),
    ):
        monkeypatch.setattr(owner, method, forbidden)
    if hasattr(os, "fork"):
        monkeypatch.setattr(os, "fork", forbidden)
    yield
    assert not violations


@contextmanager
def _partial_publication():
    backend, node, core, pending, reply, calls, rpc = _fixture(refs=False)
    request = None
    try:
        assert core._publish_reply(
            pending, reply, expected_node_id=node.node_id,
            expected_lease_id=backend.id.lease_id,
        )
        assert core._finish_pending_task(pending)
        queued = core._submissions.qsize()
        assert queued <= 3
        for _ in range(queued):
            assert core._submissions.get_nowait() is _WAKE_COORDINATOR
            core._submissions.task_done()
        assert core._submissions.empty()
        lost = pending.output_ids[1]
        assert core.owner_table.snapshot(lost).output_publication is not None
        # The owner has lost the advertised location. The Node may retain an
        # old physical replica, which real targeted retirement must delete.
        assert core.owner_table.mark_lost(lost, pending.spec.attempt_id)
        requester = WorkerID(bytes.fromhex("c1" * 16))
        consumer_task = TaskID(bytes.fromhex("c2" * 16))
        borrower = (requester, "owner-defer-source")
        assert core.owner_table.add_borrowed_reference(lost, borrower)
        hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, requester, consumer_task,
            AttemptID(consumer_task, 0),
        )
        assert core.owner_table.retain_borrowed_reference_for_task(lost, borrower, hold)
        assert core.owner_table.release_borrowed_reference(lost, borrower)
        request = protocol.RequestOwnedObjectReconstruction(
            lost, core.worker_id, requester, protocol.RetainedCredential(hold),
            None, pending.spec.attempt_id,
        )
        assert core.owner_table.snapshot(lost).retained_tokens == frozenset((hold,))
        yield SimpleNamespace(
            backend=backend, node=node, core=core, pending=pending, reply=reply,
            calls=calls, rpc=rpc, request=request, lost=lost,
            healthy=core.owner_table.snapshot(pending.output_ids[0]),
        )
    finally:
        if request is not None:
            core.owner_table.release_retained_reference_for_task(
                request.object_id, request.credential.hold,
            )
        for output in tuple(core._objects):
            for token in tuple(core.owner_table.snapshot(output).local_tokens):
                assert core.owner_table.release_local_reference(output, token)
        close_pure_core(core)


def _assert_echo(reply, request):
    assert type(reply) is protocol.RequestOwnedObjectReconstructionReply
    assert (
        reply.object_id, reply.owner_worker_id, reply.requester_worker_id,
        reply.credential, reply.borrower_token, reply.expected_owner_attempt,
    ) == (
        request.object_id, request.owner_worker_id, request.requester_worker_id,
        request.credential, request.borrower_token, request.expected_owner_attempt,
    )


def _retained_request(core, object_id, expected_attempt, *, tag):
    requester = WorkerID(bytes((tag,)) * 16)
    consumer_task = TaskID(bytes((tag + 1,)) * 16)
    borrower = (requester, "owner-defer-{}".format(tag))
    assert core.owner_table.add_borrowed_reference(object_id, borrower)
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, requester, consumer_task,
        AttemptID(consumer_task, 0),
    )
    assert core.owner_table.retain_borrowed_reference_for_task(object_id, borrower, hold)
    assert core.owner_table.release_borrowed_reference(object_id, borrower)
    return protocol.RequestOwnedObjectReconstruction(
        object_id, core.worker_id, requester, protocol.RetainedCredential(hold),
        None, expected_attempt,
    )


def _assert_deferred(values, reply):
    _assert_echo(reply, values.request)
    assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.FAILED
    # NOT_LOST is the existing retryable projection of ReconstructionDeferred,
    # unlike the permanent AUTHORITY_REJECTED raised for a broken authority.
    assert reply.failure is protocol.OwnedObjectReconstructionFailure.NOT_LOST
    assert reply.reconstruction_attempt is None and reply.detail
    core, pending = values.core, values.pending
    record = core._recovery.task_record(pending.task_id)
    assert record.current_attempt == pending.spec.attempt_id
    assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
    assert core._recovery.active_recovery(pending.task_id) is None
    session = core._targeted_reconstruction.current_session(pending.task_id)
    assert session.phase is TargetedSessionPhase.OPEN and session.execution is None
    assert session.target_output_ids == (values.lost,)
    assert session.expected_attempts == {values.lost: pending.spec.attempt_id}
    assert core.owner_table.snapshot(values.lost).state is ObjectState.LOST
    assert core.owner_table.snapshot(values.lost).current_attempt == pending.spec.attempt_id
    assert core.owner_table.snapshot(pending.output_ids[0]) == values.healthy
    assert not core._task_finish_barriers and core._accepted_task_count == 0
    queued = tuple(core._submissions.queue)
    assert len(queued) <= 2 and not any(isinstance(item, _PendingTask) for item in queued)


def _assert_started_once(values, reply):
    _assert_echo(reply, values.request)
    core, pending = values.core, values.pending
    assert reply.disposition is protocol.OwnedObjectReconstructionDisposition.STARTED
    assert reply.failure is None
    assert reply.reconstruction_attempt == pending.spec.attempt_id.next()
    record = core._recovery.task_record(pending.task_id)
    assert record.current_attempt == reply.reconstruction_attempt and record.retries_started == 1
    assert record.retries_remaining == pending.spec.max_retries - 1
    assert core._recovery.active_recovery(pending.task_id) == record.current_attempt
    session = core._targeted_reconstruction.current_session(pending.task_id)
    assert session.phase is TargetedSessionPhase.STARTED
    assert session.execution.attempt_id == record.current_attempt
    assert session.execution.target_output_ids == (values.lost,)
    assert session.execution.full_output_ids == pending.full_output_ids
    target = core.owner_table.snapshot(values.lost)
    assert target.state is ObjectState.PENDING and target.current_attempt == record.current_attempt
    assert target.output_publication is None and target.output_retirement_id is None
    assert target.retained_tokens == frozenset((values.request.credential.hold,))
    assert core.owner_table.snapshot(pending.output_ids[0]) == values.healthy
    queued = tuple(core._submissions.queue)
    assert len(queued) <= 4
    admitted = tuple(item for item in queued if isinstance(item, _PendingTask))
    assert len(admitted) == 1 and admitted[0].target_execution == session.execution
    assert core._task_finish_barriers == {values.lost: admitted[0]}
    assert core._accepted_task_count == 1
    assert not getattr(core, "_output_retirement_work", {})
    assert not core.owner_table.has_active_output_retirements()
    assert not core._protocol_unresolved
    before_calls = tuple(values.calls)
    before_record = replace(record)
    # Only accepted START is cached. Repeat cannot start again or repeat the
    # original physical drop, renewal or owner CAS.
    assert core.request_owned_object_reconstruction(values.request) is reply
    assert tuple(values.calls) == before_calls
    assert tuple(core._submissions.queue) == queued
    assert core._recovery.task_record(pending.task_id) == before_record


def test_owner_targeted_renewal_wait_defers_then_same_request_starts():
    with _partial_publication() as values:
        core, pending = values.core, values.pending
        before_owner = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
        before_record = replace(core._recovery.task_record(pending.task_id))
        before_calls = tuple(values.calls)
        before_bytes = values.backend.store.get(values.lost)
        renewal_calls, validated, completed = [], [], []
        ready = False

        def renew(task_id, attempt_id):
            assert not core._state_lock._is_owned()
            assert (task_id, attempt_id) == (pending.task_id, pending.spec.attempt_id.next())
            renewal_calls.append((task_id, attempt_id))
            assert len(renewal_calls) <= 2
            return ForeignLineageRenewalResult(
                task_id, attempt_id, ForeignLineageRenewalDisposition.READY if ready
                else ForeignLineageRenewalDisposition.WAITING, (), 0,
            )

        def validate(task_id, attempt_id):
            assert ready
            validated.append((task_id, attempt_id))

        def complete(task_id, attempt_id):
            assert ready and validated
            completed.append((task_id, attempt_id))

        core._foreign_lineage_registry = SimpleNamespace(
            snapshot=lambda task_id: SimpleNamespace(edges=()) if task_id == pending.task_id else None,
        )
        core._foreign_lineage_runtime = SimpleNamespace(
            drive_renewal=renew, validate_renewal_ready=validate, complete_renewal=complete,
        )
        deferred = core.request_owned_object_reconstruction(values.request)
        _assert_deferred(values, deferred)
        assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        assert tuple(values.calls) == before_calls
        assert values.backend.store.get(values.lost) == before_bytes
        assert len(renewal_calls) == 1 and not validated and not completed
        ready = True
        accepted = core.request_owned_object_reconstruction(values.request)
        _assert_started_once(values, accepted)
        assert renewal_calls == [(pending.task_id, pending.spec.attempt_id.next())] * 2
        assert completed == [(pending.task_id, pending.spec.attempt_id.next())]
        assert validated and all(item == completed[0] for item in validated)
        assert not values.backend.store.contains(values.lost, sealed_only=False)


def test_owner_targeted_retirement_drop_ack_loss_defers_then_exact_request_starts():
    with _partial_publication() as values:
        core, pending = values.core, values.pending
        old_target = core.owner_table.snapshot(values.lost)
        before_record = replace(core._recovery.task_record(pending.task_id))
        drop_requests, drop_statuses = [], []
        before_calls = len(values.calls)

        def lose_drop_ack(address, handler, request):
            assert not core._state_lock._is_owned(), handler
            result = values.rpc(address, handler, request)
            assert len(values.calls) - before_calls <= 3
            if handler == "drop_object_replica":
                assert request.object_id == values.lost
                assert request.producer_attempt_id == pending.spec.attempt_id
                assert request.owner_worker_id == core.worker_id
                assert request.checksum == old_target.output_publication.slot.checksum
                drop_requests.append(request)
                drop_statuses.append(result.status)
                assert not values.backend.store.contains(values.lost, sealed_only=False)
                if len(drop_requests) == 1:
                    assert result.status is protocol.DropObjectReplicaStatus.DROPPED
                    raise TimeoutError("physical retirement applied; drop ACK lost")
            return result

        core._rpc = lose_drop_ack
        deferred = core.request_owned_object_reconstruction(values.request)
        _assert_deferred(values, deferred)
        target = core.owner_table.snapshot(values.lost)
        assert target.output_retirement_id is not None
        assert replace(target, output_retirement_id=None) == old_target
        assert core._recovery.task_record(pending.task_id) == before_record
        assert len(drop_requests) == 1
        work = core._output_retirement_work[values.lost]
        assert work["plan"].retirement_id == target.output_retirement_id
        assert work["plan"].memberships == (old_target.output_publication,)
        assert not work["replica"]  # effect is real, its ACK is still missing
        assert values.backend.recovery.snapshot(values.backend.id).slot_collections == ()
        accepted = core.request_owned_object_reconstruction(values.request)
        _assert_started_once(values, accepted)
        assert drop_requests == [drop_requests[0]] * 2
        assert drop_statuses == [
            protocol.DropObjectReplicaStatus.DROPPED,
            protocol.DropObjectReplicaStatus.ALREADY_DROPPED,
        ]
        assert [handler for handler, _ in values.calls[before_calls:]] == [
            "drop_object_replica", "drop_object_replica",
            wire.REPORT_OUTPUT_PUBLICATION_HANDLER,
        ]
        (cleanup,) = values.backend.recovery.snapshot(values.backend.id).slot_collections
        assert cleanup.object_id == values.lost
        assert cleanup.cleanup_id == target.output_retirement_id
        assert cleanup.complete.publication_id == old_target.output_publication.publication_id


def test_new_owner_credential_joins_started_target_despite_finish_barrier():
    with _partial_publication() as values:
        core, pending = values.core, values.pending
        started = core.request_owned_object_reconstruction(values.request)
        _assert_started_once(values, started)
        request = _retained_request(
            core, values.lost, pending.spec.attempt_id, tag=0xc3,
        )
        try:
            assert request != values.request
            assert request.credential != values.request.credential
            session = core._targeted_reconstruction.current_session(pending.task_id)
            assert session.phase is TargetedSessionPhase.STARTED
            assert core.owner_table.snapshot(values.lost).state is ObjectState.PENDING
            assert values.lost in core._task_finish_barriers
            before_owner = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
            before_record = replace(core._recovery.task_record(pending.task_id))
            before_calls = tuple(values.calls)
            before_queue = tuple(core._submissions.queue)
            before_barriers = dict(core._task_finish_barriers)

            joined = core.request_owned_object_reconstruction(request)

            _assert_echo(joined, request)
            assert joined.disposition is protocol.OwnedObjectReconstructionDisposition.JOINED
            assert joined.reconstruction_attempt == started.reconstruction_attempt
            assert joined.failure is None
            assert core._recovery.task_record(pending.task_id) == before_record
            assert before_record.retries_started == 1
            assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before_owner
            assert core._targeted_reconstruction.current_session(pending.task_id) is session
            assert core._targeted_reconstruction.queued_losses(pending.task_id) == ()
            assert core._task_finish_barriers == before_barriers
            assert core._accepted_task_count == 1
            assert tuple(values.calls) == before_calls
            assert tuple(core._submissions.queue) == before_queue
            assert core.request_owned_object_reconstruction(request) is joined
            assert tuple(values.calls) == before_calls
            assert tuple(core._submissions.queue) == before_queue
        finally:
            assert core.owner_table.release_retained_reference_for_task(
                request.object_id, request.credential.hold,
            )


def test_new_lost_sibling_is_queued_next_not_joined_to_unrelated_started_target():
    from tests.unit.test_task_finish_barrier import _Fixture as StoredFixture

    # This helper uses real discovery -> Node adapter/journal -> Core, with
    # two stored slots. Neither loss is a forged READY_INLINE -> LOST change.
    values = StoredFixture()
    core = values.core
    pending = None
    requests = []
    try:
        pending, _refs = values.submit(num_returns=2)
        values.succeed(pending, stored=True)
        assert core._finish_pending_task(pending)
        assert not values.queued()
        selected, sibling = pending.output_ids

        def lose_slot(output):
            owner = core.owner_table.snapshot(output)
            assert owner.state is ObjectState.READY_STORED
            drop = protocol.DropObjectReplica(
                output, owner.current_attempt, core.worker_id, core.node_id,
                owner.canonical_stored_result.checksum,
            )
            reply = values.backend.node._handle_drop_object_replica(drop)
            assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
            assert not values.backend.store.contains(output, sealed_only=False)
            assert core.owner_table.mark_lost(output, owner.current_attempt)
            core._stored_descriptors.pop(output, None)

        lose_slot(selected)
        first = _retained_request(core, selected, pending.spec.attempt_id, tag=0xd1)
        requests.append(first)
        started = core.request_owned_object_reconstruction(first)
        assert started.disposition is protocol.OwnedObjectReconstructionDisposition.STARTED
        coordinator = core._targeted_reconstruction
        session = coordinator.current_session(pending.task_id)
        assert session.phase is TargetedSessionPhase.STARTED
        assert session.target_output_ids == (selected,)
        assert session.execution.attempt_id == started.reconstruction_attempt
        assert selected in core._task_finish_barriers and sibling not in core._task_finish_barriers
        assert core.owner_table.snapshot(sibling).state is ObjectState.READY_STORED

        lose_slot(sibling)
        second = _retained_request(core, sibling, pending.spec.attempt_id, tag=0xd3)
        requests.append(second)
        before_owner = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
        before_record = replace(core._recovery.task_record(pending.task_id))
        before_calls = tuple(values.backend.calls)
        before_queue = tuple(core._submissions.queue)
        before_barriers = dict(core._task_finish_barriers)

        for _ in range(2):
            deferred = core.request_owned_object_reconstruction(second)
            _assert_echo(deferred, second)
            assert deferred.disposition is protocol.OwnedObjectReconstructionDisposition.FAILED
            assert deferred.failure is protocol.OwnedObjectReconstructionFailure.NOT_LOST
            assert deferred.reconstruction_attempt is None
            assert coordinator.current_session(pending.task_id) is session
            queued = coordinator.queued_losses(pending.task_id)
            assert tuple((loss.object_id, loss.expected_attempt) for loss in queued) == (
                (sibling, pending.spec.attempt_id),
            )
            assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before_owner
            assert core._recovery.task_record(pending.task_id) == before_record
            assert before_record.retries_started == 1
            assert core._task_finish_barriers == before_barriers
            assert core._accepted_task_count == 1
            assert tuple(values.backend.calls) == before_calls
            assert tuple(core._submissions.queue) == before_queue
        assert core.owner_table.snapshot(sibling).output_publication is not None
        assert core.owner_table.snapshot(sibling).output_retirement_id is None
        assert not core._state_lock.waits
    finally:
        for request in requests:
            assert core.owner_table.release_retained_reference_for_task(
                request.object_id, request.credential.hold,
            )
        if pending is not None:
            values.close_refs(pending)
