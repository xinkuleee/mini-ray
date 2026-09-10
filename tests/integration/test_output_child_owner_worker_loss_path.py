"""Bounded Worker-only loss after child promotion on one surviving Node.

One logical task, at most two attempts and two tiny executor-owned puts.  One
GCS, one Node, one ordinary Worker slot and one replacement (four distinct
child PIDs over the run, at most three live).  Each outer has 16 KiB padding
and stays below 64 KiB; Node ObjectStore is 1 MiB.  One test-owned loopback
listener, no blocker, extra test thread, tracing, Actor, PG or fake RPC.

The existing AFTER_PROMOTIONS_ACK gate selects only attempt zero.  Registered
Worker identity is cross-checked before SIGKILL; Node remains live to report
PROCESS_EXIT, roll back the dead owner's contained hold and replace its Worker.
Gate release may finish acknowledged preparation, never a dead Worker Complete.  Exact compensation
must precede retry.  Observers forward real Core RPCs and never change replies.

Work shares a 10 s monotonic deadline.  Reference/gate cleanup shares 3 s;
runtime shutdown always runs, including partial init.  Run this exact node ID
only through scripts/run_baseline.py --case with its external 30 s process bound.
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
from miniray.api import _get_runtime
from miniray.control import GET_NODE_STATE_HANDLER, GET_WORKER_STATE_HANDLER
from miniray.core import CoreWorker
from miniray.output_handoff import OutputHandoffPhase
from miniray.ids import AttemptID
from miniray.node import GET_OBJECT_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER, REQUEST_LEASE_HANDLER
from miniray.output_publication import OutputPublicationID
from miniray.output_publication_journal import OutputPublicationStage as Stage
from miniray.publication_gate import (
    OUTPUT_PUBLICATION_GATE_RELEASE, OutputPublicationGateConfig,
    OutputPublicationGatePhase, recv_output_publication_gate_arrival,
)
from miniray.publication_sources import OwnedContainedSource
from miniray.ownership import ObjectState
from miniray.recovery import TaskState
from miniray.transport import request as rpc_request


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_INLINE_THRESHOLD = 1024
_STORE_BYTES = 1024 * 1024
_PADDING = b"W" * (16 * 1024)


@ray.remote(num_cpus=1, max_retries=1)
def _owned_child_outer():
    child = ray.put(("dead-child-replacement", os.getpid()))
    return child, _PADDING, os.getpid()


def _remaining(deadline, detail="Worker-only output smoke exceeded its deadline"):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError(detail)
    return remaining


def _query(address, handler, message, deadline):
    remaining = _remaining(deadline)
    return rpc_request(address, handler, message, connect_timeout=min(0.5, remaining),
                       request_timeout=min(2.0, remaining), deadline=deadline)


def _poll_until(predicate, deadline, detail):
    wake = threading.Event()
    while True:
        _remaining(deadline, detail)
        value = predicate()
        if value:
            return value
        remaining = _remaining(deadline, detail)
        wake.wait(min(0.01, remaining))


def _worker_state(context, worker_id, deadline):
    reply = _query(context.gcs_address, GET_WORKER_STATE_HANDLER,
                   protocol.GetWorkerState(worker_id), deadline)
    assert type(reply) is protocol.GetWorkerStateReply and reply.worker_id == worker_id and reply.found
    return replace(reply)


def _handoff(owner_address, publication_id, deadline):
    request = wire.GetOutputHandoff(publication_id)
    reply = _query(owner_address, wire.GET_OUTPUT_HANDOFF_HANDLER, request, deadline)
    assert type(reply) is wire.OutputHandoffReply and reply.request == request
    assert reply.accepted and reply.snapshot is not None
    return replace(reply.snapshot)


def _node_alive(context, node, epoch, deadline):
    reply = _query(context.gcs_address, GET_NODE_STATE_HANDLER, protocol.GetNodeState(node.node_id), deadline)
    assert type(reply) is protocol.GetNodeStateReply and reply.found
    assert reply.node_id == node.node_id and reply.node_pid == node.node_pid
    assert reply.registration_epoch == epoch and reply.state is protocol.NodeMembershipState.ALIVE
    assert reply.death is None and _pid_exists(node.node_pid)


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _close_reference(reference, deadline):
    if reference is None or reference.closed:
        return
    reference._closed = True
    if reference._finalizer is not None:
        reference._finalizer()
    if reference._release_done is not None:
        assert reference._release_done.wait(max(0.0, deadline - time.monotonic())), "reference cleanup timed out"


def test_dead_executor_child_cleanup_precedes_retry_on_same_live_node(monkeypatch):
    listener = connection = None
    context = runtime = core = report = death = None
    outer = child = None
    original_rpc = original_retry = None
    replacement_pid = None
    managed_pids, managed_addresses = set(), set()
    close_errors = []
    observations = threading.Condition()
    grants, node_outcomes, retry_samples = {}, [], []
    grant_conflicts = []
    target = {}
    rollback_reports = []
    actual_report = CoreWorker.report_output_handoff_rollback

    def report_rollback(owner, request):
        reply = actual_report(owner, request)
        if core is owner:
            with observations:
                assert len(rollback_reports) < 4
                rollback_reports.append((request, reply))
                observations.notify_all()
        return reply

    monkeypatch.setattr(CoreWorker, "report_output_handoff_rollback", report_rollback)
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        context = ray.init(
            num_nodes=1, num_cpus=1, num_workers_per_node=1,
            inline_threshold=_INLINE_THRESHOLD, object_store_bytes=_STORE_BYTES, enable_tracing=False,
            _test_output_publication_gate=OutputPublicationGateConfig(
                0, gate_address, OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK, _WORK_SECONDS,
            ),
        )
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        node = context.nodes[0]
        assert len(node.worker_ids) == 1 and context.trace_address is None
        managed_pids.update((context.gcs_pid, node.node_pid, node.worker_pid))
        managed_addresses.update((context.gcs_address, node.node_address, node.worker_address, runtime.owner_service.address))
        assert len(managed_pids) == 3 and os.getpid() not in managed_pids
        registered = _worker_state(context, node.worker_id, deadline)
        assert registered.state is protocol.WorkerMembershipState.ALIVE and registered.death is None
        assert registered.incarnation.worker_pid == node.worker_pid
        assert registered.incarnation.node_id == node.node_id and registered.incarnation.node_pid == node.node_pid
        epoch = registered.incarnation.node_registration_epoch
        assert epoch == runtime.nodes[0].registration_epoch
        original_rpc, original_retry = core._rpc, core._retry_system_failure

        def inspect_rpc(address, handler, message):
            reply = original_rpc(address, handler, message)
            with observations:
                if handler == REQUEST_LEASE_HANDLER and type(message) is protocol.RequestWorkerLease:
                    if type(reply) is protocol.GrantWorkerLease:
                        key = message.task_id, message.attempt_id.attempt_number
                        pair = message, reply
                        if grants.setdefault(key, pair) != pair:
                            grant_conflicts.append(key)
                if handler == GET_WORKER_LEASE_OUTCOME_HANDLER and type(reply) is protocol.GetWorkerLeaseOutcomeReply:
                    assert len(node_outcomes) < 128, "unbounded outcome replay"
                    node_outcomes.append((message, replace(reply)))
                observations.notify_all()
            return reply

        def inspect_retry(pending, error):
            if pending.task_id == target.get("task_id"):
                # Read-only snapshots at the actual retry boundary, before the
                # original method advances either attempt authority or budget.
                assert not core._state_lock._is_owned(), "retry observer cannot make RPC while holding Core state lock"
                proof = _handoff(runtime.owner_service.address, target["publication"], deadline)
                with core._state_lock:
                    recovery = core._recovery.task_record(pending.task_id)
                    owner = core.owner_table.snapshot(pending.object_id)
                    sample = (pending.spec.attempt_id, recovery.current_attempt,
                              recovery.retries_started, owner, proof)
                with observations:
                    retry_samples.append(sample)
                    assert len(retry_samples) <= 1, "more than one retry"
                    observations.notify_all()
            return original_retry(pending, error)

        core._rpc, core._retry_system_failure = inspect_rpc, inspect_retry
        outer = _owned_child_outer.remote()
        object_id = outer.object_id
        listener.settimeout(_remaining(deadline))
        connection, _address = listener.accept()
        connection.settimeout(_remaining(deadline))
        arrival = recv_output_publication_gate_arrival(connection)
        publication = arrival.publication_id
        assert type(publication) is OutputPublicationID
        assert arrival.phase is OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK
        assert (arrival.node_id, arrival.node_pid, arrival.registration_epoch) == (node.node_id, node.node_pid, epoch)
        assert publication.task_id == object_id.task_id
        assert ((publication.object_id,)) == ((publication.object_id,)) == (object_id,)
        assert publication.attempt_id == AttemptID(object_id.task_id, 0)
        target.update(task_id=object_id.task_id, publication=publication)
        before = _handoff(runtime.owner_service.address, publication, deadline)
        manifest = before.manifest
        assert manifest.manifest_digest == arrival.manifest_digest
        assert manifest.header.executor_worker_id == node.worker_id
        assert manifest.header.owner_worker_id == core.worker_id
        assert before.phase is OutputHandoffPhase.PENDING and before.complete is None and before.adoption is None
        assert not rollback_reports
        assert len(((manifest.value,))) == 1
        slot = (manifest.value)
        assert slot.tier is protocol.ResultStorage.OBJECT_STORE
        assert len(_PADDING) < slot.size_bytes <= 64 * 1024
        assert len(slot.transfers) == 1
        transfer = slot.transfers[0]
        assert type(transfer.source) is OwnedContainedSource
        assert transfer.contained_owner_worker_id == transfer.source.owner_worker_id == node.worker_id
        assert transfer.contained_owner_address == node.worker_address
        assert transfer.provisional_hold.container_owner_worker_id == node.worker_id
        assert transfer.final_hold.container_owner_worker_id == core.worker_id
        physical = _query(node.node_address, GET_OBJECT_HANDLER, protocol.GetObject(object_id, node.node_id), deadline)
        assert type(physical) is protocol.GetObjectReply and physical.found and physical.sealed
        assert physical.producer_attempt_id == publication.attempt_id and physical.owner_worker_id == core.worker_id
        assert physical.size_bytes == slot.size_bytes and physical.checksum == slot.checksum
        assert hashlib.sha256(physical.data).hexdigest() == slot.checksum
        assert core.owner_table.snapshot(object_id).state is ObjectState.PENDING
        assert ray.wait([outer], num_returns=1, timeout=0) == ([], [outer])

        # Identity validation precedes the one material destructive action.
        confirmed = _worker_state(context, node.worker_id, deadline)
        assert confirmed == registered and node.worker_pid > 0 and _pid_exists(node.worker_pid)
        os.kill(node.worker_pid, signal.SIGKILL)

        def dead_owner():
            state = _worker_state(context, node.worker_id, deadline)
            return state.death if state.state is protocol.WorkerMembershipState.DEAD else None

        death = _poll_until(dead_owner, deadline, "Worker death not registered")
        assert death.incarnation == registered.incarnation
        assert death.reason is protocol.WorkerDeathReason.PROCESS_EXIT and death.exit_code == -signal.SIGKILL
        assert death.worker_id == transfer.contained_owner_worker_id and death.worker_pid == node.worker_pid
        _node_alive(context, node, epoch, deadline)
        with observations:
            while not any(
                reply.found and reply.state is protocol.LeaseExecutionState.WORKER_LOST
                and reply.cleanup_pending and request.lease_id == publication.lease_id
                for request, reply in node_outcomes
            ):
                observations.wait(_remaining(deadline))
            assert not retry_samples
            assert set(attempt for task, attempt in grants if task == object_id.task_id) == {0}
        assert _handoff(runtime.owner_service.address, publication, deadline).complete is None
        assert not rollback_reports
        connection.settimeout(_remaining(deadline))
        connection.sendall(OUTPUT_PUBLICATION_GATE_RELEASE)
        connection.close()
        connection = None

        child, padding, replacement_pid = ray.get(outer, timeout=_remaining(deadline))
        assert isinstance(child, ray.ObjectRef) and padding == _PADDING
        assert type(replacement_pid) is int and replacement_pid > 0 and replacement_pid != node.worker_pid
        assert child.owner_worker_id != node.worker_id and child.borrower_token is not None
        assert child.object_id != transfer.contained_object_id
        assert ray.get(child, timeout=_remaining(deadline)) == ("dead-child-replacement", replacement_pid)
        managed_pids.add(replacement_pid)
        managed_addresses.add(child.owner_address)
        replacement = _worker_state(context, child.owner_worker_id, deadline)
        assert replacement.state is protocol.WorkerMembershipState.ALIVE and replacement.death is None
        assert replacement.incarnation.worker_pid == replacement_pid
        assert (replacement.incarnation.node_id, replacement.incarnation.node_pid, replacement.incarnation.node_registration_epoch) == (node.node_id, node.node_pid, epoch)
        _node_alive(context, node, epoch, deadline)
        assert not _pid_exists(node.worker_pid)

        resolved = _handoff(runtime.owner_service.address, publication, deadline)
        assert resolved.manifest == manifest and resolved.complete is None
        assert resolved.phase is OutputHandoffPhase.ABORTED and resolved.adoption is None
        with observations:
            reports = [(request, reply) for request, reply in rollback_reports
                       if request.manifest.publication_id == publication]
        assert reports
        actual_rollback, actual_reply = reports[0]
        assert actual_reply.request == actual_rollback and actual_reply.accepted
        assert all(request == actual_rollback and reply.accepted for request, reply in reports)
        assert actual_rollback.manifest == manifest
        tombstone = actual_rollback.tombstone
        assert tombstone.plan.publication_id == publication and tombstone.plan.manifest_digest == arrival.manifest_digest
        assert tuple((effect.stage, effect.slot_index, effect.transfer_index) for effect in tombstone.plan.effects) == (
            (Stage.SLOT_DROP, 0, None),
            (Stage.FINAL_RELEASE, 0, 0), (Stage.PROVISIONAL_RELEASE, 0, 0),
        )
        assert tuple(ack.effect for ack in tombstone.acknowledgements) == tombstone.plan.effects
        with observations:
            assert not grant_conflicts and len(retry_samples) == 1
            prior_attempt, current_attempt, retries_started, old_owner, retry_proof = retry_samples[0]
            pairs = {attempt: pair for (task, attempt), pair in grants.items() if task == object_id.task_id}
            clean_outcomes = [reply for request, reply in node_outcomes if request.lease_id == publication.lease_id
                              and reply.found and reply.state is protocol.LeaseExecutionState.WORKER_LOST and not reply.cleanup_pending]
        assert retry_proof == resolved
        assert prior_attempt == current_attempt == publication.attempt_id and retries_started == 0
        assert old_owner.state is ObjectState.PENDING and old_owner.current_attempt == publication.attempt_id
        assert old_owner.output_publication is None and not old_owner.locations
        assert clean_outcomes and all(reply.output_publication is None and reply.output_completion is None for reply in clean_outcomes)
        assert set(pairs) == {0, 1}
        first_request, first_grant = pairs[0]
        next_request, next_grant = pairs[1]
        assert first_request.task_id == next_request.task_id == object_id.task_id
        assert first_request.return_ids == next_request.return_ids == (object_id,)
        assert first_request.lease_id == publication.lease_id and next_request.lease_id != publication.lease_id
        assert first_grant.node_id == next_grant.node_id == node.node_id
        assert first_grant.worker_id == node.worker_id and next_grant.worker_id == child.owner_worker_id
        assert next_grant.worker_address == child.owner_address
        _poll_until(lambda: object_id not in core._task_finish_barriers, deadline, "replacement finalizer did not finish")
        owner = core.owner_table.snapshot(object_id)
        assert owner.state is ObjectState.READY_STORED and owner.current_attempt == AttemptID(object_id.task_id, 1)
        assert owner.output_publication is not None and owner.output_publication.publication_id != publication
        task_record = core._recovery.task_record(object_id.task_id)
        assert task_record.state is TaskState.SUCCEEDED and task_record.retries_started == 1
        assert task_record.retries_remaining == 0
        assert core._accepted_task_count == 0 and not core._protocol_unresolved
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if connection is not None:
                try:
                    connection.settimeout(max(0.001, min(0.2, cleanup_deadline - time.monotonic())))
                    connection.sendall(OUTPUT_PUBLICATION_GATE_RELEASE)
                except OSError:
                    pass
                finally:
                    connection.close()
            if listener is not None:
                listener.close()
            for reference in (child, outer):
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            if core is not None and original_rpc is not None:
                core._rpc = original_rpc
            if core is not None and original_retry is not None:
                core._retry_system_failure = original_retry
            report = ray.shutdown()

    assert not close_errors, "bounded reference cleanup failed: {!r}".format(close_errors)
    assert context is not None and report is not None and death is not None and replacement_pid is not None
    assert not ray.is_initialized()
    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean and report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert report.node_pids == context.node_pids and report.node_exitcodes == (0,)
    assert report.worker_pids == (replacement_pid,) and report.worker_exitcodes == (0,)
    # The same Node stayed ALIVE throughout recovery. Normal cluster shutdown
    # then records its expected exit; that is not a publishing-Node failure.
    (node_exit,) = report.node_deaths
    assert type(node_exit) is protocol.NodeDeathRecord
    assert node_exit.reason is protocol.NodeDeathReason.EXPECTED
    assert (node_exit.node_id, node_exit.node_pid, node_exit.registration_epoch, node_exit.exit_code) == (
        node.node_id, node.node_pid, epoch, 0,
    )
    assert len(managed_pids) == 4
    _poll_until(lambda: all(not _pid_exists(pid) for pid in managed_pids), time.monotonic() + 2.0, "managed child survived shutdown")
    assert all(process.pid not in managed_pids for process in mp.active_children())
    for address in managed_addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
