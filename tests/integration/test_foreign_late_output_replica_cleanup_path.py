"""F6: a foreign Worker owner retains and collects a late retired replica.

A factory holding A's CPU submits a mixed two-return producer that spills to B.
Its embedded Core forwards the real GCS Adopted RPC, then holds that exact ACK
on one bounded control socket. The factory returns both Worker-owned handles,
freeing A's sole Worker. A Driver-submitted consumer really pulls/seals/pins
the stored return on A, but its existing dispatch lane pauses before the first
foreign location report. The test crashes B, releases the owner's Adopted ACK,
and waits for A's real KEEP/DROP decision before releasing the late report.

The live original owner must answer RETIRED with deletion custody; the consumer
must cancel its actual grant without Push, and the owner's existing cleanup
mailbox must delete the sealed secondary. Only after observed physical absence
does the test replay the exact old Drop. One public get then reconstructs the
same stored ObjectID on A; old report/Drop replay must leave its new epoch and
the healthy INLINE sibling untouched. No owner history or ACK is fabricated.

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
from miniray.ids import AttemptID, LeaseID, TaskID
from miniray.node import (
    CANCEL_LEASE_HANDLER, DROP_OBJECT_REPLICA_HANDLER, GET_OBJECT_HANDLER,
    GET_WORKER_LEASE_OUTCOME_HANDLER, SHUTDOWN_STATUS_HANDLER,
)
from miniray.output_publication import OutputPublicationID
from miniray.output_recovery import OutputRecoveryAction, OutputRecoveryOwnerDecision
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_sources import BorrowedContainedSource
from miniray.resources import ResourceVector
from miniray.runtime_binding import current_core_worker, current_execution_context
from miniray.task_outputs import TargetExecutionKey, TargetOutputManifest, TaskExecutionKey, TaskOutputManifest
from miniray.worker import GET_OWNED_OBJECT_HANDLER, REPORT_RETAINED_OBJECT_LOCATION_HANDLER
from tests.integration.test_borrowed_output_unknown_path import _wait
from tests.support._legacy_reference_cleanup import _close_local
from tests.integration.test_stored_outer_node_loss_path import (
    _node_loss, _pid_exists, _poll_until, _query, _recovery, _recv_exact,
    _release_connection, _remaining,
)
from tests.integration.test_stored_outer_publication_path import _close_reference


pytestmark = pytest.mark.multiprocess_smoke
_OWNER_RESOURCE = "foreign_late_owner"
_PADDING = b"F" * (8 * 1024)
_SOURCE_VALUE = ("foreign-late-live-source", 42)
_RESUME_OWNER = b"R"
_OWNER_RESUMED = b"A"
_GATE_SECONDS = 8.0
_WORK_SECONDS = 18.0


@ray.remote(num_cpus=1, num_returns=2, max_retries=1)
def _mixed_foreign_owned_outputs(container):
    child, = container
    assert isinstance(child, ray.ObjectRef) and child.borrower_token is not None
    execution = current_execution_context()
    assert execution is not None and execution.blocking_notifier is not None
    return {"child": child, "slot": 0, "producer_pid": os.getpid()}, {
        "child": child, "slot": 1, "padding": _PADDING, "producer_pid": os.getpid(),
        "attempt": execution.parent_attempt_id, "lease_id": execution.blocking_notifier.identity.lease_id,
    }


@ray.remote(num_cpus=1, resources={_OWNER_RESOURCE: 1}, max_retries=0)
def _return_worker_owned_outputs_while_adopted_ack_is_held(container, control_address, deadline):
    core = current_core_worker()
    assert core is not None and core._dispatch_lane_count == 1
    original = core._rpc
    reached = threading.Event()
    selected = []
    selection_lock = threading.Lock()

    def hold_actual_adopted_ack(address, handler, request):
        reply = original(address, handler, request)
        take_gate = False
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER and type(request) is wire.ReportOutputPublicationAdopted:
            identity = request.proof.complete.publication_id
            if request.proof.owner_worker_id == core.worker_id and len(identity.output_ids) == 2:
                with selection_lock:
                    if not selected:
                        selected.append(identity)
                        take_gate = True
        if not take_gate:
            return reply
        assert type(reply) is wire.OutputRecoveryReply and reply.accepted and reply.request == request
        assert reply.ack.snapshot.adopted == request.proof
        gate_deadline = min(deadline, time.monotonic() + _GATE_SECONDS)
        try:
            # No Core/owner lock is held while the existing dispatch lane waits.
            assert not core._state_lock._is_owned()
            with socket.create_connection(control_address, timeout=_remaining(gate_deadline)) as connection:
                connection.settimeout(_remaining(gate_deadline))
                connection.sendall(os.getpid().to_bytes(8, "big") + bytes(identity.lease_id) + bytes(identity.task_id))
                reached.set()
                connection.settimeout(_remaining(gate_deadline))
                if connection.recv(1) != _RESUME_OWNER:
                    raise RuntimeError("Driver did not resume the exact owner Adopted ACK")
                core._rpc = original
                connection.settimeout(_remaining(gate_deadline))
                connection.sendall(_OWNER_RESUMED)
        finally:
            core._rpc = original
        return reply

    core._rpc = hold_actual_adopted_ack
    try:
        refs = _mixed_foreign_owned_outputs.remote(container)
        assert reached.wait(min(_GATE_SECONDS, _remaining(deadline)))
        assert len(refs) == 2
        # This thread returns; its dispatch lane, not this Worker slot, waits.
        return refs
    except BaseException:
        core._rpc = original
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
            if publication is not None and grant.dependencies and grant.dependencies[0].object_id == publication.output_ids[1]:
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
        outer = _return_worker_owned_outputs_while_adopted_ack_is_held.remote([source], control_address, deadline)
        listener.settimeout(_remaining(deadline))
        owner_connection, _ = listener.accept()
        frame = _recv_exact(owner_connection, 40, deadline)
        assert int.from_bytes(frame[:8], "big") == target.worker_pid
        task_id = TaskID(frame[24:40])
        assert task_id == TaskID.derive(core.job_id, TaskID.derive(core.job_id, outer.object_id.task_id, 0), 0)
        publication = OutputPublicationID(LeaseID(frame[8:24]), TaskExecutionKey(TaskOutputManifest.for_task(task_id, 2), AttemptID(task_id, 0)))
        owner_gate_deadline = min(deadline, time.monotonic() + _GATE_SECONDS)
        refs = tuple(ray.get(outer, timeout=_remaining(owner_gate_deadline)))
        assert len(refs) == 2 and tuple(ref.object_id for ref in refs) == publication.output_ids
        assert all(isinstance(ref, ray.ObjectRef) and ref.owner_worker_id == target.worker_id
                   and ref.owner_address == target.worker_address and ref.borrower_token for ref in refs)
        assert all(not core.owner_table.contains(ref.object_id) for ref in refs)
        _wait(core, lambda: outer.object_id not in core._task_finish_barriers, owner_gate_deadline)
        before = _recovery(context, publication, owner_gate_deadline)
        manifest = before.manifest
        assert before.complete is not None and before.adopted is not None and before.adopted.complete == before.complete
        assert before.frozen_node_death is before.owner_decision is before.resolution is None
        assert manifest.header.owner_worker_id == target.worker_id != core.worker_id
        assert manifest.header.executor_worker_id == publisher.worker_id and manifest.header.node_incarnation.node_id == publisher.node_id
        assert tuple(slot.tier for slot in manifest.slots) == (protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE)
        assert len(_PADDING) < manifest.slots[1].size_bytes < 32 * 1024
        assert all(len(slot.transfers) == 1 and slot.transfers[0].contained_object_id == source_id for slot in manifest.slots)
        assert all(type(slot.transfers[0].source) is BorrowedContainedSource
                   and slot.transfers[0].contained_owner_worker_id == core.worker_id for slot in manifest.slots)
        healthy_before = _owned(refs[0], core.worker_id, owner_gate_deadline)
        stored_before = _owned(refs[1], core.worker_id, owner_gate_deadline)
        assert healthy_before.state is protocol.OwnedObjectState.READY_INLINE
        assert stored_before.state is protocol.OwnedObjectState.READY_STORED and stored_before.descriptor.node_id == publisher.node_id
        assert healthy_before.current_attempt == stored_before.current_attempt == publication.attempt_id
        _close_local(outer, owner_gate_deadline)
        _wait(core, lambda: core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED, owner_gate_deadline)

        consumer = _foreign_late_consumer_must_not_run.remote(refs[1])
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
        sealed = _physical(target, refs[1].object_id, owner_gate_deadline)
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
        owner_connection.settimeout(_remaining(owner_gate_deadline))
        owner_connection.sendall(_RESUME_OWNER)
        assert _recv_exact(owner_connection, 1, owner_gate_deadline) == _OWNER_RESUMED
        owner_connection.close()
        owner_connection = None

        def drop_is_latched():
            snapshot = _recovery(context, publication, deadline)
            if snapshot.owner_decision is None:
                return None
            assert tuple(slot.decision for slot in snapshot.owner_decision.slots) == (OutputRecoveryOwnerDecision.KEEP, OutputRecoveryOwnerDecision.DROP)
            return snapshot

        _poll_until(drop_is_latched, deadline, "foreign Worker owner did not latch DROP")
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
            reply = _physical(target, refs[1].object_id, deadline)
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

        def resolved_loss():
            value = _node_loss(context, publication, target.worker_id, death, deadline)
            return value if value.snapshot.resolution is not None else None

        terminal = _poll_until(resolved_loss, deadline, "foreign owner did not resolve original output cleanup")
        resolved = terminal.snapshot
        assert terminal.work.action is OutputRecoveryAction.POSTCOMPLETE_RESOLVE
        assert terminal.work.snapshot == replace(before, frozen_node_death=death)
        assert resolved.resolution.kept_slots == (0,) and resolved.resolution.complete == before.complete
        assert resolved.resolution.publication_id == publication and resolved.resolution.node_death == death
        assert resolved.resolution.manifest_digest == manifest.manifest_digest
        assert resolved.owner_death is None and resolved.resolution.owner_worker_id == target.worker_id
        assert _owned(refs[0], core.worker_id, deadline) == healthy_before

        def owner_lost():
            reply = _owned(refs[1], core.worker_id, deadline)
            assert reply.current_attempt == publication.attempt_id
            return reply if reply.state is protocol.OwnedObjectState.LOST else None

        lost = _poll_until(owner_lost, deadline, "known-successful foreign output did not settle as LOST")
        assert lost.descriptor is lost.data is None
        child_at_loss = core.owner_table.snapshot(source_id)
        assert child_at_loss.contained_holds == frozenset((manifest.slots[0].transfers[0].final_hold,))
        assert core.owner_table.contained_release_was_seen(source_id, manifest.slots[1].transfers[0].final_hold)

        # Only this public get opens targeted reconstruction after old GC.
        rebuilt = ray.get(refs[1], timeout=_remaining(deadline))
        restored.append(rebuilt["child"])
        assert rebuilt["slot"] == 1 and rebuilt["padding"] == _PADDING and rebuilt["producer_pid"] == target.worker_pid
        new_attempt = publication.attempt_id.next()
        assert rebuilt["attempt"] == new_attempt
        new_publication = OutputPublicationID(rebuilt["lease_id"], TargetExecutionKey(
            TargetOutputManifest(TaskOutputManifest.for_task(task_id, 2), (refs[1].object_id,)), new_attempt,
        ))
        new_owned = _owned(refs[1], core.worker_id, deadline)
        assert new_owned.state is protocol.OwnedObjectState.READY_STORED and new_owned.current_attempt == new_attempt
        assert new_owned.descriptor.node_id == target.node_id and new_owned.owner_worker_id == target.worker_id

        def adopted_retry():
            value = _recovery(context, new_publication, deadline)
            return value if value.adopted is not None else None

        new_history = _poll_until(adopted_retry, deadline, "targeted reconstruction was not adopted by its original owner")
        assert new_history.complete is not None and new_history.owner_death is None
        assert new_history.manifest.header.owner_worker_id == new_history.manifest.header.executor_worker_id == target.worker_id
        assert new_history.manifest.slots[0].object_id == refs[1].object_id
        new_transfer, = new_history.manifest.slots[0].transfers
        assert new_transfer.contained_object_id == source_id and new_transfer.contained_owner_worker_id == core.worker_id
        assert new_transfer.final_hold != manifest.slots[1].transfers[0].final_hold
        new_replica = _physical(target, refs[1].object_id, deadline)
        assert new_replica.found and new_replica.producer_attempt_id == new_attempt
        assert new_replica.checksum == new_owned.descriptor.checksum == hashlib.sha256(new_replica.data).hexdigest()
        expected_retained = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, target.worker_id, task_id, new_attempt,
        )
        assert type(new_transfer.source) is BorrowedContainedSource
        assert new_transfer.source.original_source == protocol.TaskHoldSource(expected_retained)
        assert rebuilt["child"]._local_token is not None
        expected_local = source_before.local_tokens | frozenset((rebuilt["child"]._local_token,))
        expected_contained = frozenset((manifest.slots[0].transfers[0].final_hold, new_transfer.final_hold))

        def source_lifetime_settled():
            snapshot = core.owner_table.snapshot(source_id)
            return snapshot if (snapshot.contained_holds == expected_contained
                                and not snapshot.borrowed_tokens and not snapshot.submitted_tokens
                                and snapshot.retained_tokens == frozenset((expected_retained,))
                                and snapshot.local_tokens == expected_local
                                and snapshot.lineage_tokens == source_before.lineage_tokens) else None

        # GCS Adopted precedes the execution/finalizer tail. Wait for the real
        # source credentials to settle before comparing the complete snapshot;
        # ordinary borrower releases must not masquerade as stale-replay damage.
        source_with_new = _wait(core, source_lifetime_settled, deadline)
        assert _owned(refs[0], core.worker_id, deadline) == healthy_before
        replay_report = _query(target.worker_address, REPORT_RETAINED_OBJECT_LOCATION_HANDLER, original_report, deadline)
        assert type(replay_report) is protocol.ReportRetainedObjectLocationReply
        assert replay_report.status is protocol.RetainedLocationReportStatus.RETIRED and replay_report.custody_transferred and not replay_report.accepted
        assert (replay_report.object_id, replay_report.owner_worker_id, replay_report.borrower_worker_id, replay_report.hold, replay_report.descriptor) == (
            original_report.object_id, original_report.owner_worker_id, original_report.borrower_worker_id, original_report.hold, original_report.descriptor,
        )
        replay_drop = _query(target.node_address, DROP_OBJECT_REPLICA_HANDLER, old_drop, deadline)
        _assert_drop(replay_drop, old_drop)
        assert replay_drop == old_receipt
        assert _physical(target, refs[1].object_id, deadline) == new_replica
        assert _owned(refs[1], core.worker_id, deadline) == new_owned
        assert _owned(refs[0], core.worker_id, deadline) == healthy_before
        assert core.owner_table.snapshot(source_id) == source_with_new
        assert _recovery(context, publication, deadline) == resolved
        first = ray.get(refs[0], timeout=_remaining(deadline))
        restored.append(first["child"])
        assert first["slot"] == 0 and first["producer_pid"] == publisher.worker_pid
        for reference in restored:
            assert isinstance(reference, ray.ObjectRef) and reference.object_id == source_id and reference.borrower_token is None
            assert ray.get(reference, timeout=_remaining(deadline)) == _SOURCE_VALUE
            _close_local(reference, deadline)

        _close_local(consumer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(consumer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _close_reference(refs[1], deadline)
        _poll_until(lambda: value if not (value := _physical(target, refs[1].object_id, deadline)).found else None,
                    deadline, "reconstructed foreign stored slot did not collect")
        _wait(core, lambda: core.owner_table.snapshot(source_id).contained_holds == frozenset((manifest.slots[0].transfers[0].final_hold,)), deadline)
        _close_reference(refs[0], deadline)
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
