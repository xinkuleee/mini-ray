"""ARM-UNKNOWN publisher loss releases a live owner's contained holds.

Five startup children: one GCS, two Nodes and one ordinary Worker per Node.
A survivor blocker occupies the only other CPU while a producer returns one
stored slot containing a borrowed Driver-owned child. The existing AFTER_ARM
gate proves real promotions and physical bytes but no Complete. Exactly one
managed Node crash and at most one SYSTEM retry occur; no synthetic death,
child-release ACK, recovery resolution or test-side cleanup is used.

The actual Core RPC/ retry boundary records both old contained holds retired
before retry. Dead-executor borrower cleanup is a distinct lifetime: it must
converge through the ordinary death consumer before the blocker is released,
not satisfy an invented ordering against SYSTEM-retry admission.

One tiny Driver put, two logical tasks, 8 KiB output padding, two 1 MiB stores,
one test listener and two connections; no new test thread, Actor/PG or tracing.
All post-init work shares 10 seconds, final reference/gate cleanup 3 seconds.
Run only this exact test through the 30-second process-tree runner; it supplies
the external execution bound and bounded TERM/KILL/reap grace.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
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
from miniray.control import GET_WORKER_STATE_HANDLER
from miniray.core import _worker_death_reference_id
from miniray.ids import AttemptID
from miniray.node import GET_OBJECT_HANDLER, REQUEST_LEASE_HANDLER, SHUTDOWN_STATUS_HANDLER
from miniray.output_recovery import OutputRecoveryAction, OutputRecoveryOwnerDecision
from miniray.ownership import ObjectCollectionState, ObjectOwnerSnapshot, ObjectState
from miniray.publication_gate import (
    OUTPUT_PUBLICATION_GATE_RELEASE, OutputPublicationGateConfig,
    OutputPublicationGatePhase, recv_output_publication_gate_arrival,
)
from miniray.publication_sources import BorrowedContainedSource
from miniray.recovery import TaskState
from tests.integration.test_multi_contained_output_path import _close_local
from tests.integration.test_stored_outer_node_loss_path import (
    _BLOCKER_RELEASE, _SURVIVOR_RESOURCE, _assert_metadata_only, _graph,
    _node_loss, _occupy_survivor, _pid_exists, _poll_until, _query,
    _recovery, _recv_exact, _release_connection, _remaining,
)


pytestmark = pytest.mark.multiprocess_smoke
_WORK_SECONDS = 10.0
_SOURCE_VALUE = ("armed-borrowed-driver-child", 42)
_PADDING = b"U" * (8 * 1024)


@ray.remote(num_cpus=1, max_retries=1)
def _return_stored_borrowed_child(container):
    child, = container
    assert isinstance(child, ray.ObjectRef) and child.borrower_token is not None
    return {"child": child, "padding": _PADDING, "executor_pid": os.getpid()}


@dataclass(frozen=True)
class _RetryCut:
    resolution: object
    current_attempt: AttemptID
    retries_started: int
    task_state: TaskState
    outer: ObjectOwnerSnapshot
    source: ObjectOwnerSnapshot
    dependency_hold: protocol.TaskReferenceHold
    release_seen: tuple[bool, bool]
    executor_death: object


def _wait(core, predicate, deadline):
    with core._completion:
        while True:
            result = predicate()
            if result:
                return result
            core._completion.wait(_remaining(deadline))


def _physical(node, object_id, deadline):
    reply = _query(node.node_address, GET_OBJECT_HANDLER, protocol.GetObject(object_id, node.node_id), deadline)
    assert type(reply) is protocol.GetObjectReply and reply.object_id == object_id and reply.node_id == node.node_id
    return reply


def test_armed_unknown_borrowed_output_releases_old_holds_before_retrying_same_live_child():
    listener = blocker_connection = publication_connection = None
    context = core = report = death = None
    source = blocker = outer = restored_child = None
    transfer = publication = None
    original_rpc = original_retry = None
    managed_pids, managed_addresses = set(), set()
    observation_lock = threading.Lock()
    requests, grants, cleanup_acks, retry_cuts = {}, {}, {}, {}
    conflicts = set()
    overflow = threading.Event()
    close_errors = []
    phase = OutputPublicationGatePhase.AFTER_ARM_ACK_BEFORE_COMPLETE
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        address = listener.getsockname()
        managed_addresses.add(address)
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _SURVIVOR_RESOURCE: 1}, {"CPU": 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024,
            _test_output_publication_gate=OutputPublicationGateConfig(1, address, phase, _WORK_SECONDS),
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        survivor, victim = context.nodes
        assert core.node_id == survivor.node_id and context.trace_address is None
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, runtime.owner_service.address))
        for node in context.nodes:
            assert len(node.worker_ids) == 1
            managed_addresses.update((node.node_address, node.worker_address))
        assert len(managed_pids) == 5 and len(managed_addresses) == 7 and os.getpid() not in managed_pids
        source = ray.put(_SOURCE_VALUE)
        source_id = source.object_id
        initial_source = core.owner_table.snapshot(source_id)
        assert source.owner_worker_id == core.worker_id and source.borrower_token is None
        assert initial_source.state is ObjectState.READY_INLINE and initial_source.inline_data is not None
        original_rpc, original_retry = core._rpc, core._retry_system_failure

        def inspect_rpc(rpc_address, handler, message):
            reply = original_rpc(rpc_address, handler, message)
            with observation_lock:
                if handler == REQUEST_LEASE_HANDLER and type(message) is protocol.RequestWorkerLease:
                    key = message.task_id, message.attempt_id.attempt_number
                    if key not in requests and len(requests) >= 4:
                        overflow.set()
                    else:
                        requests[key] = message
                        if type(reply) is protocol.GrantWorkerLease:
                            if grants.setdefault(key, (message, reply)) != (message, reply):
                                conflicts.add(key)
                snapshot = (reply.snapshot if type(reply) is wire.OutputNodeLossReply
                            else reply.snapshot if type(reply) is wire.GetOutputNodeLossReply and reply.found else None)
                if snapshot is not None and snapshot.resolution is not None:
                    identity = snapshot.publication_id
                    if identity not in cleanup_acks and len(cleanup_acks) >= 2:
                        overflow.set()
                    else:
                        prior = cleanup_acks.setdefault(identity, snapshot.resolution)
                        if prior != snapshot.resolution:
                            conflicts.add((identity.task_id, identity.attempt_id.attempt_number))
            return reply

        def inspect_retry(pending, error, **kwargs):
            if transfer is not None and pending.task_id == transfer.final_hold.container_object_id.task_id:
                with core._completion:
                    record = core._recovery.task_record(pending.task_id)
                    owner = core.owner_table.snapshot(pending.output_ids[0])
                    child = core.owner_table.snapshot(source_id)
                    released = tuple(core.owner_table.contained_release_was_seen(source_id, hold)
                                     for hold in (transfer.final_hold, transfer.provisional_hold))
                    executor_death = core.owner_table.dead_worker_record(victim.worker_id)
                    with observation_lock:
                        key = pending.task_id, pending.spec.attempt_id
                        if key not in retry_cuts and len(retry_cuts) >= 2:
                            overflow.set()
                        else:
                            retry_cuts.setdefault(key, _RetryCut(
                                cleanup_acks.get(publication), record.current_attempt, record.retries_started,
                                record.state, owner, child, pending.dependency_hold, released, executor_death,
                            ))
            return original_retry(pending, error, **kwargs)

        core._rpc, core._retry_system_failure = inspect_rpc, inspect_retry
        blocker = _occupy_survivor.remote(address, deadline)
        listener.settimeout(_remaining(deadline))
        blocker_connection, _ = listener.accept()
        assert int.from_bytes(_recv_exact(blocker_connection, 8, deadline), "big") == survivor.worker_pid
        outer = _return_stored_borrowed_child.remote([source])
        object_id = outer.object_id
        listener.settimeout(_remaining(deadline))
        publication_connection, _ = listener.accept()
        publication_connection.settimeout(_remaining(deadline))
        arrival = recv_output_publication_gate_arrival(publication_connection)
        publication = arrival.publication_id
        assert arrival.phase is phase
        assert (arrival.node_id, arrival.node_pid, arrival.registration_epoch) == (
            victim.node_id, victim.node_pid, runtime.nodes[1].registration_epoch,
        )
        assert publication.task_id == object_id.task_id and publication.output_ids == (object_id,)
        assert publication.full_output_ids == (object_id,) and publication.attempt_id == AttemptID(object_id.task_id, 0)
        _assert_metadata_only(arrival)
        before = _recovery(context, publication, deadline)
        manifest = before.manifest
        assert manifest.manifest_digest == arrival.manifest_digest
        assert manifest.header.owner_worker_id == core.worker_id
        assert manifest.header.executor_worker_id == victim.worker_id
        assert before.armed and before.complete is None
        assert before.recovery_action is OutputRecoveryAction.COMPLETION_UNKNOWN
        assert before.adopted is before.rollback is before.owner_decision is before.resolution is None
        assert before.frozen_node_death is before.owner_death is None and before.slot_collections == ()
        slot, = manifest.slots
        assert slot.object_id == object_id and slot.tier is protocol.ResultStorage.OBJECT_STORE
        assert len(_PADDING) < slot.size_bytes < 32 * 1024
        transfer, = slot.transfers
        assert type(transfer.source) is BorrowedContainedSource
        assert transfer.contained_object_id == source_id
        assert transfer.contained_owner_worker_id == core.worker_id
        assert transfer.contained_owner_address == runtime.owner_service.address
        assert transfer.source.borrower_worker_id == victim.worker_id
        assert type(transfer.source.original_source) is protocol.TaskHoldSource
        hold = transfer.source.original_source.hold
        assert hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert (hold.submitting_worker_id, hold.task_id, hold.origin_attempt_id) == (
            core.worker_id, object_id.task_id, publication.attempt_id,
        )
        assert transfer.provisional_hold.container_owner_worker_id == victim.worker_id
        assert transfer.final_hold.container_owner_worker_id == core.worker_id
        assert transfer.final_hold.container_object_id == transfer.provisional_hold.container_object_id == object_id
        old_borrower = transfer.source.owner_table_token
        at_arm = core.owner_table.snapshot(source_id)
        assert at_arm.state is ObjectState.READY_INLINE and at_arm.inline_data == initial_source.inline_data
        assert at_arm.current_attempt == initial_source.current_attempt and at_arm.local_tokens == initial_source.local_tokens
        assert old_borrower in at_arm.borrowed_tokens
        assert dict(at_arm.borrowed_sources)[old_borrower] == transfer.source.original_source
        assert at_arm.submitted_tokens == frozenset((hold,)) and at_arm.lineage_tokens
        assert not at_arm.retained_tokens and at_arm.contained_holds == frozenset((transfer.final_hold,))
        # Promotions have already removed the provisional hold; final survives
        # executor death because its container owner is the live Driver.
        assert core.owner_table.contained_release_was_seen(source_id, transfer.provisional_hold)
        assert not core.owner_table.contained_release_was_seen(source_id, transfer.final_hold)
        assert _graph(context, publication, deadline).manifest == manifest.to_graph_manifest()
        physical = _physical(victim, object_id, deadline)
        assert physical.found and physical.sealed and physical.producer_attempt_id == publication.attempt_id
        assert physical.owner_worker_id == core.worker_id and physical.size_bytes == slot.size_bytes
        assert physical.checksum == slot.checksum == hashlib.sha256(physical.data).hexdigest()
        assert len(physical.data) == slot.size_bytes
        pending_owner = core.owner_table.snapshot(object_id)
        assert pending_owner.state is ObjectState.PENDING and pending_owner.output_publication is None
        assert pending_owner.current_attempt == publication.attempt_id and not pending_owner.locations
        assert publication not in getattr(core, "_output_result_custody", {})

        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert (death.node_id, death.node_pid, death.registration_epoch) == (
            arrival.node_id, arrival.node_pid, arrival.registration_epoch,
        )
        assert death.exit_code == -signal.SIGKILL and death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        publication_connection.close()
        publication_connection = None

        def observed_retry():
            with observation_lock:
                return retry_cuts.get((object_id.task_id, publication.attempt_id))

        cut = _poll_until(observed_retry, deadline, "live-child cleanup never reached SYSTEM retry")
        assert cut.current_attempt == publication.attempt_id and cut.retries_started == 0
        assert cut.outer.state is ObjectState.PENDING and cut.outer.current_attempt == publication.attempt_id
        assert cut.outer.output_publication is None and not cut.outer.locations
        assert cut.resolution is not None and cut.resolution.publication_id == publication
        assert cut.resolution.complete is None and cut.resolution.kept_slots == ()
        assert cut.resolution.manifest_digest == manifest.manifest_digest and cut.resolution.owner_worker_id == core.worker_id
        assert cut.resolution.node_death == death and cut.release_seen == (True, True)
        assert cut.dependency_hold == hold and cut.source.submitted_tokens == at_arm.submitted_tokens
        assert cut.source.lineage_tokens == at_arm.lineage_tokens
        assert cut.source.state is ObjectState.READY_INLINE and cut.source.inline_data == at_arm.inline_data
        assert transfer.final_hold not in cut.source.contained_holds
        assert transfer.provisional_hold not in cut.source.contained_holds
        # No assertion couples cut.executor_death / cut.source.borrowed_tokens
        # to retry admission: the independent ordered consumer owns that tail.

        terminal = _node_loss(context, publication, core.worker_id, death, deadline)
        assert terminal.work.action is OutputRecoveryAction.COMPLETION_UNKNOWN
        assert terminal.work.snapshot == replace(before, frozen_node_death=death)
        resolved = terminal.snapshot
        assert resolved.manifest == manifest and resolved.resolution == cut.resolution
        assert resolved.complete is None and resolved.armed and resolved.frozen_node_death == death
        assert resolved.owner_decision.complete is None
        decision, = resolved.owner_decision.slots
        assert decision.object_id == object_id and decision.slot_index == 0
        assert decision.decision is OutputRecoveryOwnerDecision.DROP
        assert resolved.adopted is resolved.rollback is resolved.owner_death is resolved.owner_cleaned is None
        assert not resolved.terminal_report_allowed
        assert _recovery(context, publication, deadline) == resolved

        def retry_admitted():
            record = core._recovery.task_record(object_id.task_id)
            return (record.current_attempt == publication.attempt_id.next()
                    and record.retries_started == 1 and record.state is TaskState.RETRY_PENDING)

        _wait(core, retry_admitted, deadline)

        def old_borrower_retired():
            child = core.owner_table.snapshot(source_id)
            installed = core.owner_table.dead_worker_record(victim.worker_id)
            return child if (installed is not None and old_borrower not in child.borrowed_tokens
                             and old_borrower in child.released_borrowed_tokens) else None

        retired_source = _wait(core, old_borrower_retired, deadline)
        worker = _query(context.gcs_address, GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(victim.worker_id), deadline)
        assert type(worker) is protocol.GetWorkerStateReply and worker.found and worker.worker_id == victim.worker_id
        assert worker.state is protocol.WorkerMembershipState.DEAD and worker.death.reason is protocol.WorkerDeathReason.NODE_EXIT
        assert (worker.death.node_id, worker.death.node_pid, worker.death.node_registration_epoch, worker.death.worker_pid) == (
            victim.node_id, victim.node_pid, arrival.registration_epoch, victim.worker_pid,
        )
        assert core.owner_table.dead_worker_record(victim.worker_id).death_id == _worker_death_reference_id(worker.death)
        assert not core.owner_table.dead_worker_record(core.worker_id)
        assert retired_source.submitted_tokens == at_arm.submitted_tokens
        assert retired_source.lineage_tokens == at_arm.lineage_tokens and not retired_source.contained_holds
        assert retired_source.local_tokens == initial_source.local_tokens
        assert ray.get(source, timeout=_remaining(deadline)) == _SOURCE_VALUE
        with observation_lock:
            assert {attempt for task, attempt in grants if task == object_id.task_id} == {0}
            assert len(retry_cuts) == 1 and not conflicts and not overflow.is_set()

        blocker_connection.settimeout(_remaining(deadline))
        blocker_connection.sendall(_BLOCKER_RELEASE)
        assert ray.get(blocker, timeout=_remaining(deadline)) == survivor.worker_pid
        blocker_connection.close()
        blocker_connection = None
        value = ray.get(outer, timeout=_remaining(deadline))
        restored_child = value["child"]
        assert value["padding"] == _PADDING and value["executor_pid"] == survivor.worker_pid
        assert isinstance(restored_child, ray.ObjectRef)
        assert restored_child.object_id == source_id and restored_child.owner_worker_id == core.worker_id
        assert restored_child.borrower_token is None
        assert ray.get(restored_child, timeout=_remaining(deadline)) == _SOURCE_VALUE
        _wait(core, lambda: object_id not in core._task_finish_barriers, deadline)
        completed = core.owner_table.snapshot(object_id)
        assert completed.state is ObjectState.READY_STORED and completed.current_attempt == publication.attempt_id.next()
        next_member = completed.output_publication
        assert next_member is not None and next_member.publication_id != publication
        assert next_member.manifest.header.executor_worker_id == survivor.worker_id
        assert next_member.manifest.header.owner_worker_id == core.worker_id
        assert next_member.manifest.header.node_incarnation.node_id == survivor.node_id
        next_transfer, = next_member.slot.transfers
        assert next_transfer.contained_object_id == source_id
        assert next_transfer.contained_owner_worker_id == core.worker_id
        assert type(next_transfer.source) is BorrowedContainedSource
        assert next_transfer.source.borrower_worker_id == survivor.worker_id
        assert next_transfer.source.original_source == transfer.source.original_source
        assert next_transfer.final_hold != transfer.final_hold and next_transfer.provisional_hold != transfer.provisional_hold
        assert next_transfer.final_hold.container_object_id == transfer.final_hold.container_object_id == object_id
        source_after_retry = core.owner_table.snapshot(source_id)
        assert source_after_retry.contained_holds == frozenset((next_transfer.final_hold,))
        assert not source_after_retry.submitted_tokens and not source_after_retry.borrowed_tokens
        assert source_after_retry.lineage_tokens == at_arm.lineage_tokens
        assert next_transfer.source.owner_table_token in source_after_retry.released_borrowed_tokens
        assert core.owner_table.contained_release_was_seen(source_id, transfer.final_hold)
        record = core._recovery.task_record(object_id.task_id)
        assert record.state is TaskState.SUCCEEDED and record.retries_started == 1 and record.retries_remaining == 0
        assert record.current_attempt == completed.current_attempt and core._recovery.active_recovery(object_id.task_id) is None
        assert _recovery(context, publication, deadline) == resolved
        assert _node_loss(context, publication, core.worker_id, death, deadline).work == terminal.work
        with observation_lock:
            task_grants = {attempt: pair for (task, attempt), pair in grants.items() if task == object_id.task_id}
            assert set(task_grants) == {0, 1} and len(retry_cuts) == 1
            assert not conflicts and not overflow.is_set()
        first_request, first_grant = task_grants[0]
        next_request, next_grant = task_grants[1]
        assert first_request.return_ids == next_request.return_ids == (object_id,)
        assert first_request.lease_id != next_request.lease_id
        assert (first_grant.node_id, next_grant.node_id) == (victim.node_id, survivor.node_id)

        _close_local(restored_child, deadline)
        _close_local(outer, deadline)
        _close_local(blocker, deadline)
        _wait(core, lambda: all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
                               for ref in (outer, blocker)), deadline)
        assert not _physical(survivor, object_id, deadline).found
        assert core.owner_table.contained_release_was_seen(source_id, next_transfer.final_hold)
        assert core.owner_table.contained_release_was_seen(source_id, next_transfer.provisional_hold)
        assert ray.get(source, timeout=_remaining(deadline)) == _SOURCE_VALUE
        _close_local(source, deadline)
        _wait(core, lambda: core.owner_table.collection_state(source_id) is ObjectCollectionState.COLLECTED, deadline)
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations
        def survivor_clean():
            status = _query(survivor.node_address, SHUTDOWN_STATUS_HANDLER,
                            protocol.ShutdownStatusRequest("inspect-borrowed-unknown-cleanup"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested
            return status if status.resources_clean else None

        status = _poll_until(survivor_clean, deadline, "survivor cleanup did not settle")
        assert status.child_pids == (survivor.worker_pid,)
        assert not _pid_exists(victim.node_pid) and not _pid_exists(victim.worker_pid)
    finally:
        cleanup_deadline = time.monotonic() + 3.0
        try:
            _release_connection(publication_connection, OUTPUT_PUBLICATION_GATE_RELEASE, cleanup_deadline)
            _release_connection(blocker_connection, _BLOCKER_RELEASE, cleanup_deadline)
            if listener is not None:
                listener.close()
            for reference in (restored_child, outer, blocker, source):
                try:
                    _close_local(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            if core is not None and original_rpc is not None:
                core._rpc, core._retry_system_failure = original_rpc, original_retry
            report = ray.shutdown()

    assert not close_errors and context is not None and death is not None and report is not None
    assert not ray.is_initialized() and report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert not report.gcs_forced and not report.forced
    assert report.node_exitcodes == (0, death.exit_code) and report.node_cleans == (True, False)
    assert report.node_forced == (False, False) and report.node_finalized == (True, False)
    assert report.worker_exitcodes == (0, None) and report.worker_cleans == (True, False)
    assert report.worker_forced == (False, False) and report.node_deaths[1] == death
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert not report.node_clean and not report.worker_clean
    assert not report.finalized and not report.shutdown_ack_clean and not report.resources_clean
    _poll_until(lambda: all(not _pid_exists(pid) for pid in managed_pids),
                time.monotonic() + 2.0, "managed process survived borrowed-unknown shutdown")
    assert all(process.pid not in managed_pids for process in mp.active_children())
    for address in managed_addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
