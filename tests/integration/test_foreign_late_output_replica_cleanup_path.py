"""F6: a foreign Worker owner retains and collects a late retired replica.

A factory holding A's CPU submits one stored-output producer that spills to B.
Its embedded Core pauses before the actual Node adoption ACK request, after
its real owner ADOPTED CAS, on one bounded control socket. It returns the handle,
freeing A's sole Worker. A Driver-submitted consumer really pulls/seals/pins
the stored return on A, but its existing dispatch lane pauses before the first
foreign location report. The test crashes B, resumes the real request into the
dead Node, and observes A's committed loss receipt before releasing the report.

The live original owner must answer RETIRED with deletion custody; the consumer
must cancel its actual grant without Push, and the owner's existing cleanup
mailbox must delete the sealed secondary. Only after observed physical absence
does the test replay the exact old Drop. One public get then reconstructs the
same stored ObjectID on A; old owner-commit/report/Drop replay must leave its
new epoch and reference holds untouched. The same socket carries the foreign
owner's actual typed loss receipt; the Driver never substitutes its own Core.
No owner history or ACK is fabricated.

Five startup children, two 1 MiB stores, one tiny Driver put, one factory, one
producer (two physical attempts), and one never-executed consumer. One Node
crash plus one explicit reconstruction, 8 KiB padding, one listener/socket, no
new test thread, Actor/PG or tracing. The two semantic gates each allow at most
8 s inside an 18 s work deadline. Finally releases them and shares 3 s for ref
cleanup before shutdown. Run this exact ID alone through the 30 s tree runner.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import multiprocessing as mp
import os
import signal
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import output_protocol as wire, protocol
from miniray.api import _get_runtime, _test_crash_node
from miniray.control import GET_NODES_HANDLER, GET_WORKER_STATE_HANDLER
from miniray.errors import SystemTaskError
from miniray.ids import AttemptID, TaskID
from miniray.node import (
    CANCEL_LEASE_HANDLER, DROP_OBJECT_REPLICA_HANDLER, GET_OBJECT_HANDLER,
    GET_WORKER_LEASE_OUTCOME_HANDLER, SHUTDOWN_STATUS_HANDLER,
)
from miniray.output_publication import OutputPublicationID
from miniray.output_handoff import NodeLostOutputResolution, OutputHandoffPhase, OutputHandoffSnapshot
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_sources import BorrowedContainedSource
from miniray.resources import ResourceVector
from miniray.runtime_binding import current_core_worker, current_execution_context
from miniray.task_outputs import TaskExecution
from miniray.recovery import TaskState
from miniray.transport import _receive, _send
from miniray.worker import GET_OWNED_OBJECT_HANDLER, REPORT_RETAINED_OBJECT_LOCATION_HANDLER
from tests.support._legacy_reference_cleanup import _close_local
from tests.integration.test_stored_outer_node_loss_path import (
    _assert_metadata_only, _close_reference, _handoff, _pid_exists, _poll_until,
    _query, _recv_exact, _release_connection, _remaining,
)


pytestmark = pytest.mark.multiprocess_smoke
_OWNER_RESOURCE = "foreign_late_owner"
_PADDING = b"F" * (8 * 1024)
_SOURCE_VALUE = ("foreign-late-live-source", 42)
_RESUME_OWNER = b"R"
_OWNER_RESUMED = b"A"
_GATE_SECONDS = 8.0
_WORK_SECONDS = 18.0
_MAX_OBSERVATION_BYTES = 64 * 1024
_GATE_KIND = "foreign-owner-before-node-adoption-ack"
_LOSS_KIND = "foreign-owner-committed-node-loss"
_REPLAY_KIND = "foreign-owner-old-loss-commit-replayed"


@ray.remote(num_cpus=1, max_retries=1)
def _stored_foreign_owned_output(container):
    child, = container
    assert isinstance(child, ray.ObjectRef) and child.borrower_token is not None
    execution = current_execution_context()
    assert execution is not None and execution.blocking_notifier is not None
    return {
        "child": child, "padding": _PADDING, "producer_pid": os.getpid(),
        "attempt": execution.parent_attempt_id, "lease_id": execution.blocking_notifier.identity.lease_id,
    }


@ray.remote(num_cpus=1, resources={_OWNER_RESOURCE: 1}, max_retries=0)
def _return_worker_owned_output_before_node_adoption_ack(container, control_address, deadline):
    core = current_core_worker()
    assert core is not None and core._dispatch_lane_count == 1
    original, original_loss = core._rpc, core._drive_output_node_loss_once
    reached = threading.Event()
    selected = []
    selection_lock = threading.Lock()
    connection = None
    old_manifest = old_resolution = None
    loss_sent = replay_sent = False

    def observe_owner_loss(pending, obligation):
        nonlocal old_resolution, loss_sent
        result = original_loss(pending, obligation)
        with core._state_lock:
            identity = obligation.publication_id
            receipt = core.owner_table._output_loss_receipts.get(identity)
            if (not selected or identity != selected[0] or receipt is None or loss_sent
                    or identity not in getattr(core, "_output_loss_completed", set())):
                return result
            old_resolution = replace(receipt)
            handoff = core._output_handoff_table().query(identity)
            owner = core.owner_table.snapshot(((identity.object_id,))[0])
            record = core._recovery.task_record(identity.task_id)
            assert owner.state is ObjectState.LOST and owner.current_attempt == identity.attempt_id
            assert owner.inline_data is None and owner.canonical_stored_result is None
            assert owner.output_publication is None and not owner.locations
            observation = (_LOSS_KIND, old_resolution, handoff, record.state,
                           record.current_attempt, record.retries_started)
            loss_sent = True
        _assert_metadata_only(observation)
        connection.settimeout(_remaining(deadline))
        _send(connection, observation, _MAX_OBSERVATION_BYTES)
        return result

    def hold_before_actual_adopted_ack(address, handler, request):
        nonlocal connection, old_manifest, replay_sent
        take_gate = False
        adoption = (handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
                    and type(request) is wire.AckOutputPublicationAdopted)
        if adoption:
            identity = request.proof.complete.publication_id
            if request.proof.owner_worker_id == core.worker_id and identity.attempt_id.attempt_number == 0:
                with selection_lock:
                    if not selected:
                        selected.append(identity)
                        take_gate = True
        if take_gate:
            with core._state_lock:
                handoff = core._output_handoff_table().query(identity)
                assert handoff.phase is OutputHandoffPhase.ADOPTED and handoff.adoption == request.proof
                old_manifest = handoff.manifest
                assert old_manifest.header.owner_worker_id == core.worker_id
                assert len(((identity.object_id,))) == 1 and ((identity.object_id,))[0].return_index == 0
            gate_deadline = min(deadline, time.monotonic() + _GATE_SECONDS)
            assert not core._state_lock._is_owned()
            try:
                connection = socket.create_connection(control_address, timeout=_remaining(gate_deadline))
                connection.settimeout(_remaining(gate_deadline))
                _send(connection, (_GATE_KIND, os.getpid(), request, handoff), _MAX_OBSERVATION_BYTES)
                reached.set()
                connection.settimeout(_remaining(gate_deadline))
                if connection.recv(1) != _RESUME_OWNER:
                    raise RuntimeError("Driver did not resume the exact Node adoption request")
                connection.settimeout(_remaining(gate_deadline))
                connection.sendall(_OWNER_RESUMED)
            except BaseException:
                core._rpc, core._drive_output_node_loss_once = original, original_loss
                if connection is not None:
                    connection.close()
                raise
            # The publisher is now dead. The real transport exception drives
            # the normal existing adoption-replay / Node-loss takeover path.
        reply = original(address, handler, request)
        if (adoption and selected and identity.task_id == selected[0].task_id
                and identity.attempt_id == selected[0].attempt_id.next() and not replay_sent):
            assert type(reply) is wire.AckOutputPublicationAdoptedReply
            assert reply.accepted and reply.request == request
            with core._state_lock:
                assert old_resolution is not None and loss_sent
                owner_before = core.owner_table.snapshot(((identity.object_id,))[0])
                assert owner_before.state is ObjectState.READY_STORED and owner_before.current_attempt == identity.attempt_id
                # Replay the real prior owner commit; it must return its
                # existing receipt without mutating the reconstructed epoch.
                committed = core.owner_table.resolve_output_node_loss(old_manifest, old_resolution)
                assert committed is False
                assert core.owner_table.snapshot(((identity.object_id,))[0]) == owner_before
                assert core.owner_table._output_loss_receipts[selected[0]] == old_resolution
                record = core._recovery.task_record(identity.task_id)
                observation = (_REPLAY_KIND, identity, old_resolution, committed,
                               core._output_handoff_table().query(identity), record.state,
                               record.current_attempt, record.retries_started)
                replay_sent = True
            _assert_metadata_only(observation)
            try:
                connection.settimeout(_remaining(deadline))
                _send(connection, observation, _MAX_OBSERVATION_BYTES)
            finally:
                connection.close()
                core._rpc, core._drive_output_node_loss_once = original, original_loss
        return reply

    core._rpc, core._drive_output_node_loss_once = hold_before_actual_adopted_ack, observe_owner_loss
    try:
        ref = _stored_foreign_owned_output.remote(container)
        assert reached.wait(min(_GATE_SECONDS, _remaining(deadline)))
        # This thread returns; its dispatch lane, not this Worker slot, waits.
        return [ref]
    except BaseException:
        core._rpc, core._drive_output_node_loss_once = original, original_loss
        if connection is not None:
            connection.close()
        raise


@ray.remote(num_cpus=1, resources={_OWNER_RESOURCE: 1}, max_retries=0)
def _foreign_late_consumer_must_not_run(_value):
    raise AssertionError("retired foreign dependency reached consumer user code")


def _physical(node, object_id, deadline):
    reply = _query(node.node_address, GET_OBJECT_HANDLER, protocol.GetObject(object_id, node.node_id), deadline)
    assert type(reply) is protocol.GetObjectReply and reply.object_id == object_id and reply.node_id == node.node_id
    return reply


def _owned(reference, borrower, deadline):
    request = protocol.GetOwnedObject(reference.object_id, reference.owner_worker_id, borrower, reference.borrower_token)
    reply = _query(reference.owner_address, GET_OWNED_OBJECT_HANDLER, request, deadline)
    assert type(reply) is protocol.GetOwnedObjectReply and reply.accepted
    assert (reply.object_id, reply.owner_worker_id, reply.borrower_worker_id, reply.borrower_token) == (
        request.object_id, request.owner_worker_id, request.borrower_worker_id, request.borrower_token,
    )
    return reply


def _assert_drop(reply, request):
    assert type(reply) is protocol.DropObjectReplicaReply
    assert reply.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED and reply.accepted and not reply.dropped
    assert protocol.DropObjectReplica(reply.object_id, reply.producer_attempt_id, reply.owner_worker_id,
                                      reply.node_id, reply.checksum) == request


def _wait(core, predicate, deadline):
    with core._completion:
        while True:
            result = predicate()
            if result:
                return result
            core._completion.wait(_remaining(deadline))


def test_foreign_late_replica_is_collected_and_old_messages_preserve_reconstructed_epoch():
    listener = owner_connection = None
    context = core = report = death = None
    source = outer = consumer = None
    refs, restored = (), []
    original_rpc = original_borrow = original_build = original_push = None
    publication = None
    pids, addresses = set(), set()
    localized = threading.Event()
    release_report = threading.Event()
    gate_expired = threading.Event()
    observation_lock = threading.Lock()
    held, reports, cancellations, custody_acks = [], [], [], []
    before_cancel, push_counts = [], {}
    overflow = threading.Event()
    close_errors = []
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        control_address = listener.getsockname()
        addresses.add(control_address)
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _OWNER_RESOURCE: 1}, {"CPU": 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        target, publisher = context.nodes
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, runtime.owner_service.address))
        for node in context.nodes:
            assert len(node.worker_ids) == 1
            addresses.update((node.node_address, node.worker_address))
        assert len(pids) == 5 and len(addresses) == 7 and context.trace_address is None
        assert core.node_id == target.node_id and os.getpid() not in pids
        source = ray.put(_SOURCE_VALUE)
        source_id = source.object_id
        source_before = core.owner_table.snapshot(source_id)
        original_rpc, original_borrow = core._rpc, core._borrow_rpc
        original_build, original_push = core._build_location_reports, core._push_task_rpc

        def record_borrow(address, handler, request):
            reply = original_borrow(address, handler, request)
            if handler == REPORT_RETAINED_OBJECT_LOCATION_HANDLER:
                with observation_lock:
                    if len(reports) < 8:
                        reports.append((address, request, reply))
                    else:
                        overflow.set()
            return reply

        def record_rpc(address, handler, request):
            if handler == CANCEL_LEASE_HANDLER:
                with observation_lock:
                    assert reports and reports[-1][2].status is protocol.RetainedLocationReportStatus.RETIRED
                    assert reports[-1][2].custody_transferred
                    if len(before_cancel) < 4:
                        before_cancel.append(reports[-1][1])
                    else:
                        overflow.set()
            reply = original_rpc(address, handler, request)
            if handler in (CANCEL_LEASE_HANDLER, protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER):
                with observation_lock:
                    values = cancellations if handler == CANCEL_LEASE_HANDLER else custody_acks
                    if len(values) < 4:
                        values.append((request, reply))
                    else:
                        overflow.set()
            return reply

        def hold_real_foreign_grant(requested, grant, foreign_guards=()):
            result = original_build(requested, grant, foreign_guards)
            if publication is not None and grant.dependencies and grant.dependencies[0].object_id == ((publication.object_id,))[0]:
                assert len(result) == len(foreign_guards) == len(grant.dependencies) == 1
                assert grant.node_id == target.node_id and grant.worker_id == target.worker_id
                with observation_lock:
                    assert not held, "late-report gate was entered twice"
                    held.append((requested, grant, result[0]))
                localized.set()
                if not release_report.wait(min(_GATE_SECONDS, _remaining(deadline))):
                    gate_expired.set()
                    raise TimeoutError("foreign late-report gate exceeded its bound")
            return result

        def record_push(address, handler, request):
            with observation_lock:
                key = request.spec.task_id
                push_counts[key] = push_counts.get(key, 0) + 1
                if len(push_counts) > 2 or sum(push_counts.values()) > 8:
                    overflow.set()
            return original_push(address, handler, request)

        core._rpc, core._borrow_rpc = record_rpc, record_borrow
        core._build_location_reports, core._push_task_rpc = hold_real_foreign_grant, record_push
        outer = _return_worker_owned_output_before_node_adoption_ack.remote([source], control_address, deadline)
        listener.settimeout(_remaining(deadline))
        owner_connection, _ = listener.accept()
        frame = _receive(owner_connection, _MAX_OBSERVATION_BYTES, deadline=deadline)
        _assert_metadata_only(frame)
        assert type(frame) is tuple and len(frame) == 4 and frame[0] == _GATE_KIND
        _, owner_pid, adoption_request, before = frame
        assert owner_pid == target.worker_pid
        assert type(adoption_request) is wire.AckOutputPublicationAdopted
        assert type(before) is OutputHandoffSnapshot and before.phase is OutputHandoffPhase.ADOPTED
        publication = adoption_request.proof.complete.publication_id
        task_id = publication.task_id
        assert task_id == TaskID.derive(core.job_id, TaskID.derive(core.job_id, outer.object_id.task_id, 0), 0)
        assert publication.execution == (TaskExecution(AttemptID(task_id, 0)))
        owner_gate_deadline = min(deadline, time.monotonic() + _GATE_SECONDS)
        refs = tuple(ray.get(outer, timeout=_remaining(owner_gate_deadline)))
        assert len(refs) == 1 and tuple(ref.object_id for ref in refs) == ((publication.object_id,))
        assert all(isinstance(ref, ray.ObjectRef) and ref.owner_worker_id == target.worker_id
                   and ref.owner_address == target.worker_address and ref.borrower_token for ref in refs)
        assert all(not core.owner_table.contains(ref.object_id) for ref in refs)
        _wait(core, lambda: outer.object_id not in core._task_finish_barriers, owner_gate_deadline)
        assert _handoff(target.worker_address, publication, owner_gate_deadline) == before
        manifest = before.manifest
        assert before.complete is not None and before.adoption == adoption_request.proof
        assert before.adoption.complete == before.complete and before.abort_reason is None
        assert manifest.header.owner_worker_id == target.worker_id != core.worker_id
        assert manifest.header.executor_worker_id == publisher.worker_id and manifest.header.node_incarnation.node_id == publisher.node_id
        assert ((manifest.value.tier,)) == (protocol.ResultStorage.OBJECT_STORE,)
        assert len(_PADDING) < (manifest.value).size_bytes < 32 * 1024
        assert (len(manifest.value.transfers) == 1 and manifest.value.transfers[0].contained_object_id == source_id)
        assert (type(manifest.value.transfers[0].source) is BorrowedContainedSource and manifest.value.transfers[0].contained_owner_worker_id == core.worker_id)
        stored_before = _owned(refs[0], core.worker_id, owner_gate_deadline)
        assert stored_before.state is protocol.OwnedObjectState.READY_STORED and stored_before.descriptor.node_id == publisher.node_id
        assert stored_before.current_attempt == publication.attempt_id
        _close_local(outer, owner_gate_deadline)
        _wait(core, lambda: core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED, owner_gate_deadline)

        consumer = _foreign_late_consumer_must_not_run.remote(refs[0])
        assert localized.wait(min(_GATE_SECONDS, _remaining(owner_gate_deadline)))
        with observation_lock:
            ((requested, grant, location),) = held
        assert grant.task_id == consumer.object_id.task_id and grant.attempt_id == AttemptID(grant.task_id, 0)
        assert requested == (stored_before.descriptor,)
        replica, = grant.dependencies
        assert replica == replace(stored_before.descriptor, node_id=target.node_id)
        original_report = location.request
        assert original_report.descriptor == replica and original_report.owner_worker_id == target.worker_id
        assert original_report.borrower_worker_id == core.worker_id and original_report.hold.kind is protocol.TaskReferenceHoldKind.RETAINED
        assert original_report.hold.task_id == consumer.object_id.task_id
        assert location.guard.owner_address == target.worker_address
        old_drop = protocol.DropObjectReplica(replica.object_id, replica.producer_attempt_id, replica.owner_worker_id, replica.node_id, replica.checksum)
        sealed = _physical(target, refs[0].object_id, owner_gate_deadline)
        assert sealed.found and sealed.sealed and sealed.producer_attempt_id == publication.attempt_id
        assert sealed.owner_worker_id == target.worker_id and sealed.size_bytes == replica.size_bytes
        assert sealed.checksum == replica.checksum == hashlib.sha256(sealed.data).hexdigest()
        with observation_lock:
            assert not reports and not cancellations
        owner_before = _query(context.gcs_address, GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(target.worker_id), owner_gate_deadline)
        assert type(owner_before) is protocol.GetWorkerStateReply and owner_before.state is protocol.WorkerMembershipState.ALIVE

        death = _test_crash_node(publisher.node_id, timeout=_remaining(owner_gate_deadline))
        assert death.node_pid == publisher.node_pid and death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        assert death.registration_epoch == manifest.header.node_incarnation.registration_epoch
        assert manifest.header.node_incarnation.node_pid == publisher.node_pid
        owner_connection.settimeout(_remaining(owner_gate_deadline))
        owner_connection.sendall(_RESUME_OWNER)
        assert _recv_exact(owner_connection, 1, owner_gate_deadline) == _OWNER_RESUMED
        loss_frame = _receive(owner_connection, _MAX_OBSERVATION_BYTES, deadline=owner_gate_deadline)
        _assert_metadata_only(loss_frame)
        assert type(loss_frame) is tuple and len(loss_frame) == 6 and loss_frame[0] == _LOSS_KIND
        _, resolution, resolved, task_state, attempt, retries = loss_frame
        assert type(resolution) is NodeLostOutputResolution and type(resolved) is OutputHandoffSnapshot
        assert resolution.publication_id == publication and resolution.node_death == death
        assert resolution.manifest_digest == manifest.manifest_digest
        assert resolution.owner_worker_id == target.worker_id != core.worker_id
        assert resolution.complete == before.complete and not resolution.keep
        resolution.validate_manifest(manifest)
        transfer, = (manifest.value).transfers
        assert all(type(reply) is protocol.ReleaseContainedReferenceReply and reply.accepted
                   for reply in resolution.cleanup)
        assert {(reply.object_id, reply.owner_worker_id, reply.hold) for reply in resolution.cleanup} == {
            (source_id, core.worker_id, transfer.final_hold),
            (source_id, core.worker_id, transfer.provisional_hold),
        }
        assert resolved == before and resolved.adoption is not None
        assert task_state is TaskState.SUCCEEDED and attempt == publication.attempt_id and retries == 0
        assert _handoff(target.worker_address, publication, deadline) == resolved
        release_report.set()
        with pytest.raises(SystemTaskError, match="retired.*replica"):
            ray.get(consumer, timeout=_remaining(deadline))
        _wait(core, lambda: consumer.object_id not in core._task_finish_barriers, deadline)
        assert not gate_expired.is_set() and not overflow.is_set()
        with observation_lock:
            assert reports and cancellations and before_cancel and custody_acks
            assert push_counts == {outer.object_id.task_id: 1}
            assert all(request == original_report for _, request, _ in reports)
            assert all(request == original_report for request in before_cancel)
            for address, request, reply in reports:
                assert address == target.worker_address and type(reply) is protocol.ReportRetainedObjectLocationReply
                assert reply.status is protocol.RetainedLocationReportStatus.RETIRED and not reply.accepted and reply.custody_transferred
                assert (reply.object_id, reply.owner_worker_id, reply.borrower_worker_id, reply.hold, reply.descriptor) == (
                    request.object_id, request.owner_worker_id, request.borrower_worker_id, request.hold, request.descriptor,
                )
            assert any(reply.released for _, reply in cancellations)
            for request, reply in cancellations:
                assert request.lease_id == grant.lease_id and request.attempt_id == grant.attempt_id
                assert request.requester_worker_id == core.worker_id and request.lease_request.return_ids == (consumer.object_id,)
                assert reply.accepted and reply.cancelled and reply.state is protocol.LeaseExecutionState.ABANDONED
                assert reply.dependency_inventory.descriptors == grant.dependencies
            assert all(reply.accepted and reply.request == request and request.inventory.descriptors == grant.dependencies
                       for request, reply in custody_acks)
        terminal_consumer = core.owner_table.snapshot(consumer.object_id)
        assert terminal_consumer.state is ObjectState.ERROR
        assert core._recovery.task_record(consumer.object_id.task_id).retries_started == 0

        def old_absent():
            reply = _physical(target, refs[0].object_id, deadline)
            return reply if not reply.found else None

        assert _poll_until(old_absent, deadline, "foreign owner did not autonomously collect its retired secondary").data is None
        # This replay occurs only after observed absence, so it cannot be the
        # destructive action which makes the owner's cleanup assertion pass.
        old_receipt = _query(target.node_address, DROP_OBJECT_REPLICA_HANDLER, old_drop, deadline)
        _assert_drop(old_receipt, old_drop)
        outcome_request = protocol.GetWorkerLeaseOutcome(grant.lease_id, grant.task_id, grant.attempt_id,
            grant.worker_id, core.worker_id, (consumer.object_id,))
        outcome = _query(target.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, outcome_request, deadline)
        assert type(outcome) is protocol.GetWorkerLeaseOutcomeReply and outcome.found and outcome.worker_alive
        assert outcome.state is protocol.LeaseExecutionState.ABANDONED and outcome.completion_status is None
        assert not outcome.descriptors and not outcome.orphan_descriptors and not outcome.cleanup_pending

        def owner_lost():
            reply = _owned(refs[0], core.worker_id, deadline)
            assert reply.current_attempt == publication.attempt_id
            return reply if reply.state is protocol.OwnedObjectState.LOST else None

        lost = _poll_until(owner_lost, deadline, "known-successful foreign output did not settle as LOST")
        assert lost.descriptor is lost.data is None
        child_at_loss = core.owner_table.snapshot(source_id)
        assert not child_at_loss.contained_holds
        for hold in (transfer.final_hold, transfer.provisional_hold):
            assert core.owner_table.contained_release_was_seen(source_id, hold)

        # Only this public get opens whole-task reconstruction after old GC.
        rebuilt = ray.get(refs[0], timeout=_remaining(deadline))
        restored.append(rebuilt["child"])
        assert rebuilt["padding"] == _PADDING and rebuilt["producer_pid"] == target.worker_pid
        new_attempt = publication.attempt_id.next()
        assert rebuilt["attempt"] == new_attempt
        new_publication = OutputPublicationID(rebuilt["lease_id"], (TaskExecution(new_attempt)))
        assert ((new_publication.object_id,)) == ((publication.object_id,)) and new_publication.lease_id != publication.lease_id
        new_owned = _owned(refs[0], core.worker_id, deadline)
        assert new_owned.state is protocol.OwnedObjectState.READY_STORED and new_owned.current_attempt == new_attempt
        assert new_owned.descriptor.node_id == target.node_id and new_owned.owner_worker_id == target.worker_id

        replay_frame = _receive(owner_connection, _MAX_OBSERVATION_BYTES, deadline=deadline)
        _assert_metadata_only(replay_frame)
        assert type(replay_frame) is tuple and len(replay_frame) == 8 and replay_frame[0] == _REPLAY_KIND
        _, observed_new, prior_receipt, committed, new_history, state, current_attempt, retries = replay_frame
        assert observed_new == new_publication and prior_receipt == resolution and committed is False
        assert type(new_history) is OutputHandoffSnapshot and new_history.phase is OutputHandoffPhase.ADOPTED
        assert state is TaskState.SUCCEEDED and current_attempt == new_attempt and retries == 1
        assert _handoff(target.worker_address, new_publication, deadline) == new_history
        owner_connection.close()
        owner_connection = None
        assert new_history.complete is not None and new_history.adoption is not None
        assert new_history.manifest.header.owner_worker_id == new_history.manifest.header.executor_worker_id == target.worker_id
        assert (new_history.manifest.publication_id).object_id == refs[0].object_id
        new_transfer, = (new_history.manifest.value).transfers
        assert new_transfer.contained_object_id == source_id and new_transfer.contained_owner_worker_id == core.worker_id
        assert new_transfer.final_hold != transfer.final_hold
        new_replica = _physical(target, refs[0].object_id, deadline)
        assert new_replica.found and new_replica.producer_attempt_id == new_attempt
        assert new_replica.checksum == new_owned.descriptor.checksum == hashlib.sha256(new_replica.data).hexdigest()
        expected_retained = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, target.worker_id, task_id, new_attempt,
        )
        assert type(new_transfer.source) is BorrowedContainedSource
        assert new_transfer.source.original_source == protocol.TaskHoldSource(expected_retained)
        assert rebuilt["child"]._local_token is not None
        expected_local = source_before.local_tokens | frozenset((rebuilt["child"]._local_token,))
        expected_contained = frozenset((new_transfer.final_hold,))

        def source_lifetime_settled():
            snapshot = core.owner_table.snapshot(source_id)
            return snapshot if (snapshot.contained_holds == expected_contained
                                and not snapshot.borrowed_tokens and not snapshot.submitted_tokens
                                and snapshot.retained_tokens == frozenset((expected_retained,))
                                and snapshot.local_tokens == expected_local
                                and snapshot.lineage_tokens == source_before.lineage_tokens) else None

        # Node adoption ACK precedes the execution/finalizer tail. Wait for the real
        # source credentials to settle before comparing the complete snapshot;
        # ordinary borrower releases must not masquerade as stale-replay damage.
        source_with_new = _wait(core, source_lifetime_settled, deadline)
        replay_report = _query(target.worker_address, REPORT_RETAINED_OBJECT_LOCATION_HANDLER, original_report, deadline)
        assert type(replay_report) is protocol.ReportRetainedObjectLocationReply
        assert replay_report.status is protocol.RetainedLocationReportStatus.RETIRED and replay_report.custody_transferred and not replay_report.accepted
        assert (replay_report.object_id, replay_report.owner_worker_id, replay_report.borrower_worker_id, replay_report.hold, replay_report.descriptor) == (
            original_report.object_id, original_report.owner_worker_id, original_report.borrower_worker_id, original_report.hold, original_report.descriptor,
        )
        replay_drop = _query(target.node_address, DROP_OBJECT_REPLICA_HANDLER, old_drop, deadline)
        _assert_drop(replay_drop, old_drop)
        assert replay_drop == old_receipt
        assert _physical(target, refs[0].object_id, deadline) == new_replica
        assert _owned(refs[0], core.worker_id, deadline) == new_owned
        assert core.owner_table.snapshot(source_id) == source_with_new
        assert _handoff(target.worker_address, publication, deadline) == resolved
        for reference in restored:
            assert isinstance(reference, ray.ObjectRef) and reference.object_id == source_id and reference.borrower_token is None
            assert ray.get(reference, timeout=_remaining(deadline)) == _SOURCE_VALUE
            _close_local(reference, deadline)

        _close_local(consumer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _close_reference(refs[0], deadline)
        _poll_until(lambda: value if not (value := _physical(target, refs[0].object_id, deadline)).found else None,
                    deadline, "reconstructed foreign stored slot did not collect")
        _wait(core, lambda: not (snapshot := core.owner_table.snapshot(source_id)).contained_holds
              and not snapshot.borrowed_tokens and not snapshot.retained_tokens and not snapshot.submitted_tokens, deadline)
        assert core.owner_table.snapshot(source_id).local_tokens == source_before.local_tokens
        _close_local(source, deadline)
        _wait(core, lambda: core.owner_table.collection_state(source_id) is ObjectCollectionState.COLLECTED, deadline)
        owner_after = _query(context.gcs_address, GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(target.worker_id), deadline)
        assert owner_after.state is protocol.WorkerMembershipState.ALIVE and owner_after.incarnation == owner_before.incarnation
        def gcs_resources_restored():
            nodes = _query(context.gcs_address, GET_NODES_HANDLER, protocol.GetNodes(), deadline)
            assert type(nodes) is protocol.GetNodesReply and len(nodes.nodes) == 1 and nodes.nodes[0].node_id == target.node_id
            assert nodes.nodes[0].total_resources == ResourceVector({"CPU": 1, _OWNER_RESOURCE: 1})
            return nodes.nodes[0].available_resources == nodes.nodes[0].total_resources

        _poll_until(gcs_resources_restored, deadline, "survivor availability report did not converge")
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations

        def survivor_clean():
            status = _query(target.node_address, SHUTDOWN_STATUS_HANDLER, protocol.ShutdownStatusRequest("inspect-foreign-late-cleanup"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested
            return status if status.resources_clean else None

        assert _poll_until(survivor_clean, deadline, "foreign owner survivor did not drain local cleanup").child_pids == (target.worker_pid,)
        assert not gate_expired.is_set() and not overflow.is_set()
    finally:
        release_report.set()
        cleanup_deadline = time.monotonic() + 3.0
        try:
            _release_connection(owner_connection, _RESUME_OWNER, cleanup_deadline)
            if listener is not None:
                listener.close()
            for reference in (*restored, consumer, *refs, outer, source):
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            if core is not None and original_rpc is not None:
                core._rpc, core._borrow_rpc = original_rpc, original_borrow
                core._build_location_reports, core._push_task_rpc = original_build, original_push
            report = ray.shutdown()

    assert not close_errors and context is not None and report is not None and death is not None
    assert report.core_stopped and report.gcs_clean and not report.forced
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.node_exitcodes == (0, death.exit_code) and report.worker_exitcodes == (0, None)
    assert report.node_cleans == report.node_finalized == report.worker_cleans == (True, False)
    assert report.node_forced == report.worker_forced == (False, False) and report.node_deaths[1] == death
    assert not ray.is_initialized()
    _poll_until(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0, "foreign late cleanup leaked a managed process")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
