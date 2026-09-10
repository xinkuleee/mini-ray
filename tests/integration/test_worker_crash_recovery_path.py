"""Recover one successful Complete after the executor exits before TaskReply.

The original one PID-returning user Task triggers CRASH once. The Node retains
the real INLINE publication envelope, so its Driver adopts attempt 0 without
reexecuting user code or consuming retry budget. This is not SYSTEM_ERROR and
not the old pre-publication contract where the successful result was lost.

There is one GCS, one Node, one ordinary Worker slot and one 1-MiB store. The
Node reaps the initial Worker before spawning its replacement: three live
children at peak, four child PIDs over the expected lifetime. An additional
independent lease-only probe observes that replacement's real WorkerID/address
and GCS incarnation PID. It has empty inputs/returns and zero resources, never
sends Start or Push, and is cancelled with an exact empty-inventory ACK. It is
not a second user Task, a read-only query, or another failpoint trigger.

Run only this exact node ID through the external 30-second process-tree runner.
Work shares fifteen seconds after init: <=64 passive Core observations, <=256
finish waits, <=64 old-Worker state polls and <=64 same-request capacity polls
for the single probe. The test starts no thread/listener and never sleeps.
Probe cancellation/ACK and public reference close share three seconds in
finally; even an unknown grant or failed cleanup falls through to shutdown.
All observed PIDs and initial/granted/retired-grant endpoints are checked.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import output_protocol as wire, protocol
from miniray.api import _get_runtime
from miniray.control import GET_WORKER_STATE_HANDLER
from miniray.ids import AttemptID, LeaseID, TaskID
from miniray.node import (
    CANCEL_LEASE_HANDLER,
    GET_WORKER_LEASE_OUTCOME_HANDLER,
    REQUEST_LEASE_HANDLER,
)
from miniray.output_publication import OutputPublicationCompleteWitness, OutputPublicationEnvelope
from miniray.ownership import ObjectState
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecution
from miniray.worker import (
    PUSH_TASK_HANDLER,
    WorkerFailpointConfig,
    WorkerFailpointMode,
)
from miniray.transport import request as rpc_request
from tests.integration.test_task_path import _close_reference


pytestmark = pytest.mark.multiprocess_smoke
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 64
_MAX_FINISH_OBSERVATIONS = 256
_MAX_POLLS = 64


@ray.remote(max_retries=1)
def identify_executor() -> int:
    return os.getpid()


def _pid_exists(pid: int) -> bool:
    """Return whether a POSIX process still exists, including a zombie."""

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Worker-after-Complete work exceeded its deadline")
    return remaining


def _query(address, handler, message, deadline):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, message, connect_timeout=min(0.25, remaining),
        request_timeout=min(1.0, remaining), deadline=deadline,
    )


def test_after_complete_worker_crash_recovers_output_without_reexecution() -> None:
    context = core = ref = report = None
    original_rpc = original_push = None
    observations: list[tuple[str, object, object]] = []
    observation_lock = threading.Lock()
    passive_wait = threading.Event()
    overflow = False
    cleanup_errors = []
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    result_pid = replacement_pid = None
    probe_request = probe_cancel = probe_grant = None
    probe_attempted = probe_clean = False

    def observe(handler, message, reply):
        nonlocal overflow
        with observation_lock:
            if len(observations) < _MAX_OBSERVATIONS:
                observations.append((handler, message, reply))
            else:
                overflow = True
        # No observer exception or additional RPC changes the original send,
        # response, timeout, Worker death detection or attempt decision.

    def inspect_rpc(address, handler, message):
        reply = original_rpc(address, handler, message)
        if handler in (
            REQUEST_LEASE_HANDLER,
            GET_WORKER_LEASE_OUTCOME_HANDLER,
            wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER,
        ):
            observe(handler, message, reply)
        return reply

    def inspect_push(address, handler, message):
        if handler == PUSH_TASK_HANDLER:
            observe("push_sent", message, None)
        try:
            reply = original_push(address, handler, message)
        except Exception as exc:
            if handler == PUSH_TASK_HANDLER:
                observe("push_failed", message, exc)
            raise
        if handler == PUSH_TASK_HANDLER:
            observe("push_reply", message, reply)
        return reply

    def worker_state(worker_id, deadline):
        state = _query(
            context.gcs_address, GET_WORKER_STATE_HANDLER,
            protocol.GetWorkerState(worker_id), deadline,
        )
        assert type(state) is protocol.GetWorkerStateReply and state.found
        state = replace(state)
        assert state.worker_id == worker_id and state.incarnation is not None
        assert state.incarnation.node_id == context.node_id
        assert state.incarnation.node_pid == context.node_pid
        managed_pids.add(state.incarnation.worker_pid)
        return state

    def remember_probe_grant(grant):
        nonlocal probe_grant
        grant = protocol.revalidate_worker_lease_grant(grant)
        assert (grant.lease_id, grant.task_id, grant.attempt_id, grant.node_id) == (
            probe_request.lease_id, probe_request.task_id, probe_request.attempt_id, context.node_id,
        )
        assert grant.dependencies == () and grant.scheduling_key is None
        assert grant.worker_id != context.worker_id
        managed_addresses.add(grant.worker_address)
        assert probe_grant is None or probe_grant == grant
        probe_grant = grant

    def cancel_probe(deadline):
        nonlocal probe_clean, replacement_pid
        if not probe_attempted or probe_clean:
            return
        # The complete request is retained before the first send. Cancel can
        # fence an unknown grant even when its Worker/token ACK never arrived.
        reply = _query(context.node_address, CANCEL_LEASE_HANDLER, probe_cancel, deadline)
        assert type(reply) is protocol.CancelWorkerLeaseReply
        reply = replace(reply)
        assert (reply.lease_id, reply.task_id, reply.attempt_id, reply.requester_node_id,
                reply.requester_worker_id, reply.scheduling_key) == (
            probe_request.lease_id, probe_request.task_id, probe_request.attempt_id,
            probe_request.requester_node_id, probe_request.requester_worker_id, None,
        )
        assert reply.accepted and reply.cancelled and reply.state is protocol.LeaseExecutionState.ABANDONED
        if reply.retired_grant is not None:
            remember_probe_grant(reply.retired_grant)
        elif probe_grant is not None:
            raise AssertionError("cancel lost the probe's previously committed grant")
        inventory = reply.dependency_inventory
        assert inventory == protocol.LeaseDependencyInventory(probe_request, context.node_id, ())
        acknowledgement = protocol.AckLeaseDependencyCustody(core.worker_id, inventory)
        ack = _query(
            context.node_address, protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER,
            acknowledgement, deadline,
        )
        assert type(ack) is protocol.AckLeaseDependencyCustodyReply
        assert ack.accepted and ack.request == acknowledgement
        probe_clean = True
        if probe_grant is not None and replacement_pid is None:
            # Failure-finally may learn this incarnation only from Cancel's
            # historical grant. A lookup failure does not undo its real ACK.
            replacement_pid = worker_state(probe_grant.worker_id, deadline).incarnation.worker_pid

    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=1,
            num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
            _test_worker_failpoint=WorkerFailpointConfig(
                mode=WorkerFailpointMode.CRASH
            ),
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update((context.gcs_pid, context.node_pid, context.worker_pid))
        managed_addresses.update((context.gcs_address, context.node_address, context.worker_address))
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None and context.trace_address is None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 3 and len(managed_addresses) == 4
        assert os.getpid() not in managed_pids
        original_rpc, original_push = core._rpc, core._push_task_rpc
        core._rpc, core._push_task_rpc = inspect_rpc, inspect_push
        _remaining(deadline)
        ref = identify_executor.remote()
        original_object_id = ref.object_id
        result_pid = ray.get(ref, timeout=_remaining(deadline))
        assert type(result_pid) is int and result_pid == context.worker_pid
        # READY does not itself prove that publication adoption/retirement and
        # the accepted-task finish barrier have converged. Observe their actual
        # notification without using shutdown to complete the tested attempt.
        with core._completion:
            for _ in range(_MAX_FINISH_OBSERVATIONS):
                if (original_object_id not in core._task_finish_barriers
                        and not core._protocol_unresolved and core._accepted_task_count == 0):
                    break
                core._completion.wait(min(0.1, _remaining(deadline)))
            else:
                raise TimeoutError("Worker-after-Complete finish observations exhausted")
            assert original_object_id.task_id in core._finished_tasks

        with observation_lock:
            observed = tuple(observations)
        assert not overflow

        snapshot = core.owner_table.snapshot(original_object_id)
        assert ref.object_id == original_object_id
        original_attempt = AttemptID(original_object_id.task_id, 0)
        assert snapshot.state is ObjectState.READY_INLINE and snapshot.error is None
        assert snapshot.current_attempt == original_attempt
        recovery = core._recovery.task_record(original_object_id.task_id)
        assert recovery.state is TaskState.SUCCEEDED and recovery.current_attempt == original_attempt
        assert recovery.retries_started == 0 and recovery.retries_remaining == 1

        granted = [
            (index, message, reply)
            for index, (handler, message, reply) in enumerate(observed)
            if handler == REQUEST_LEASE_HANDLER
            and isinstance(message, protocol.RequestWorkerLease)
            and isinstance(reply, protocol.GrantWorkerLease)
        ]
        assert granted
        grant_index, original_request, original_grant = granted[0]
        assert original_request.task_id == original_object_id.task_id
        assert original_request.attempt_id == original_attempt
        assert original_request.return_ids == (original_object_id,) and original_request.dependencies == ()
        assert original_request.requester_worker_id == core.worker_id
        assert original_grant.worker_id == context.worker_id and original_grant.node_id == context.node_id
        assert original_grant.worker_address == context.worker_address
        assert all(request == original_request and grant == original_grant for _, request, grant in granted)
        assert all(message == original_request for handler, message, _ in observed if handler == REQUEST_LEASE_HANDLER)
        pushes = [message for handler, message, _ in observed if handler == "push_sent"]
        assert pushes and all(message == pushes[0] for message in pushes)
        original_push_message = pushes[0]
        assert original_push_message.lease_id == original_request.lease_id
        assert original_push_message.spec.task_id == original_object_id.task_id
        assert original_push_message.spec.attempt_id == original_attempt
        assert original_push_message.worker_id == context.worker_id
        assert any(handler == "push_failed" for handler, _, _ in observed)
        assert not any(
            handler == "push_reply" and isinstance(reply, protocol.TaskReply)
            and reply.status is protocol.TaskReplyStatus.SUCCEEDED
            for handler, _, reply in observed
        )

        terminal_outcomes = [
            (index, request, reply)
            for index, (handler, request, reply) in enumerate(observed)
            if handler == GET_WORKER_LEASE_OUTCOME_HANDLER
            and isinstance(request, protocol.GetWorkerLeaseOutcome)
            and isinstance(reply, protocol.GetWorkerLeaseOutcomeReply)
            and reply.found
            and reply.state is protocol.LeaseExecutionState.COMPLETED
            and reply.completion_status is protocol.TaskReplyStatus.SUCCEEDED
            and reply.output_publication is not None
        ]
        assert terminal_outcomes
        outcome_index, outcome_request, outcome_reply = terminal_outcomes[0]
        assert outcome_request == protocol.GetWorkerLeaseOutcome(
            original_request.lease_id, original_object_id.task_id, original_attempt,
            context.worker_id, core.worker_id, (original_object_id,),
        )
        assert outcome_reply.object_ids == (original_object_id,)
        assert not outcome_reply.cleanup_pending and outcome_reply.output_completion is None
        assert outcome_reply.descriptors == () and outcome_reply.orphan_descriptors == ()
        envelope = replace(outcome_reply.output_publication)
        assert type(envelope) is OutputPublicationEnvelope
        assert envelope.publication_id.lease_id == original_request.lease_id
        assert envelope.publication_id.execution == TaskExecution.from_task_spec(original_push_message.spec)
        assert ((envelope.publication_id.object_id,)) == (original_object_id,)
        assert envelope.manifest.header.job_id == core.job_id
        assert envelope.manifest.header.owner_worker_id == core.worker_id
        assert envelope.manifest.header.executor_worker_id == context.worker_id
        incarnation = envelope.manifest.header.node_incarnation
        assert incarnation.node_id == context.node_id and incarnation.node_pid == context.node_pid
        assert envelope.complete == OutputPublicationCompleteWitness.for_manifest(envelope.manifest)
        result = (envelope.result)
        assert result.object_id == original_object_id and result.storage is protocol.ResultStorage.INLINE
        assert result.owner_worker_id == core.worker_id and result.node_id == context.node_id
        assert result.inline_data == snapshot.inline_data and result.inline_data is not None
        assert result.size_bytes == len(result.inline_data) <= 64
        assert result.checksum == hashlib.sha256(result.inline_data).hexdigest()
        assert all(reply.output_publication == envelope for _, _, reply in terminal_outcomes)
        adopted = [
            (index, message, reply) for index, (handler, message, reply) in enumerate(observed)
            if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
        ]
        assert adopted and grant_index < outcome_index < adopted[0][0]
        for _, message, reply in adopted:
            assert type(message) is wire.AckOutputPublicationAdopted
            assert message.proof.complete == envelope.complete and message.proof.owner_worker_id == core.worker_id
            assert type(reply) is wire.AckOutputPublicationAdoptedReply and reply.accepted
            assert reply.request == message
        retired = _query(context.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, outcome_request, deadline)
        assert type(retired) is protocol.GetWorkerLeaseOutcomeReply and retired.found
        retired = replace(retired)
        assert (retired.lease_id, retired.task_id, retired.attempt_id, retired.executor_worker_id,
                retired.owner_worker_id, retired.object_ids, retired.node_id) == (
            original_request.lease_id, original_object_id.task_id, original_attempt,
            context.worker_id, core.worker_id, (original_object_id,), context.node_id,
        )
        assert retired.output_publication is None and retired.output_completion == envelope.complete
        assert retired.state is protocol.LeaseExecutionState.COMPLETED
        assert retired.completion_status is protocol.TaskReplyStatus.SUCCEEDED

        # Completion, process death and slot replacement are distinct facts.
        # The TCP exception is never used as authority for the latter two.
        for _ in range(_MAX_POLLS):
            old_state = worker_state(context.worker_id, deadline)
            assert old_state.incarnation.worker_pid == context.worker_pid
            if old_state.state is protocol.WorkerMembershipState.DEAD and not _pid_exists(context.worker_pid):
                break
            passive_wait.wait(min(0.05, _remaining(deadline)))
        else:
            raise TimeoutError("initial Worker death did not converge")
        death = old_state.death
        assert death is not None and death.incarnation == old_state.incarnation
        assert death.reason is protocol.WorkerDeathReason.PROCESS_EXIT and death.exit_code == 23
        assert death.node_registration_epoch == incarnation.registration_epoch

        # This independent, unexecuted lease is only an endpoint/slot probe.
        # Replacement Workers inherit CRASH, so a second user Task would alter
        # the fault scenario. No Start/Push or fake owner output is introduced.
        probe_task = TaskID.random()
        assert probe_task != original_object_id.task_id
        probe_request = protocol.RequestWorkerLease(
            LeaseID.random(), probe_task, AttemptID(probe_task, 0), ResourceVector.empty(),
            context.node_id, core.worker_id, preferred_node_id=context.node_id,
            target_node_id=context.node_id, dependencies=(), return_ids=(),
        )
        assert probe_request.lease_id != original_request.lease_id
        probe_cancel = protocol.CancelWorkerLease(
            probe_request.lease_id, probe_request.task_id, probe_request.attempt_id,
            context.node_id, core.worker_id, lease_request=probe_request,
        )
        for _ in range(_MAX_POLLS):
            probe_attempted = True
            reply = _query(context.node_address, REQUEST_LEASE_HANDLER, probe_request, deadline)
            assert (reply.lease_id, reply.task_id, reply.attempt_id) == (
                probe_request.lease_id, probe_task, probe_request.attempt_id,
            )
            if type(reply) is protocol.GrantWorkerLease:
                remember_probe_grant(reply)
                break
            assert type(reply) is protocol.RejectWorkerLease
            assert reply.reason is protocol.LeaseRejectReason.PENDING_CAPACITY
            assert reply.scheduling_key is None
            passive_wait.wait(min(0.05, _remaining(deadline)))
        else:
            raise TimeoutError("replacement Worker did not grant the fixed probe lease")
        replacement_state = worker_state(probe_grant.worker_id, deadline)
        assert replacement_state.state is protocol.WorkerMembershipState.ALIVE and replacement_state.death is None
        replacement_pid = replacement_state.incarnation.worker_pid
        assert replacement_state.incarnation.node_registration_epoch == incarnation.registration_epoch
        assert replacement_pid != context.worker_pid
        assert _pid_exists(replacement_pid) and not _pid_exists(context.worker_pid)
        cancel_probe(deadline)
        assert probe_clean
        cancelled = _query(
            context.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER,
            protocol.GetWorkerLeaseOutcome(
                probe_request.lease_id, probe_task, probe_request.attempt_id,
                probe_grant.worker_id, core.worker_id, (),
            ), deadline,
        )
        assert type(cancelled) is protocol.GetWorkerLeaseOutcomeReply and cancelled.found
        cancelled = replace(cancelled)
        assert (cancelled.lease_id, cancelled.task_id, cancelled.attempt_id, cancelled.executor_worker_id,
                cancelled.owner_worker_id, cancelled.object_ids, cancelled.node_id) == (
            probe_request.lease_id, probe_task, probe_request.attempt_id,
            probe_grant.worker_id, core.worker_id, (), context.node_id,
        )
        assert cancelled.state is protocol.LeaseExecutionState.ABANDONED and cancelled.worker_alive
        assert cancelled.completion_status is None and not cancelled.cleanup_pending
        assert cancelled.descriptors == cancelled.orphan_descriptors == ()
        assert cancelled.output_publication is None and cancelled.output_completion is None
        assert core.owner_table.snapshot(original_object_id).current_attempt == original_attempt
        assert core._recovery.task_record(original_object_id.task_id).retries_started == 0
        with observation_lock:
            assert tuple(observations) == observed and not overflow
        assert len(managed_pids) == 4 and len(managed_addresses) <= 5
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            try:
                cancel_probe(cleanup_deadline)
            except Exception as exc:
                # Keep an unresolved cancellation visible. Cluster drain still
                # gets a chance to abandon the never-started lease; no local
                # flag, invented ACK or skipped assertion declares it clean.
                cleanup_errors.append(exc)
            try:
                _close_reference(ref, cleanup_deadline)
            except Exception as exc:
                cleanup_errors.append(exc)
        finally:
            try:
                if core is not None and original_rpc is not None:
                    core._rpc, core._push_task_rpc = original_rpc, original_push
            finally:
                try:
                    report = ray.shutdown()
                finally:
                    # Capture all actual grants even if ray.get/assertions
                    # failed before the normal observation snapshot. Report
                    # adds the final slot PID even if no result was delivered.
                    with observation_lock:
                        final_observations = tuple(observations)
                    for handler, _request, reply in final_observations:
                        if handler == REQUEST_LEASE_HANDLER and isinstance(reply, protocol.GrantWorkerLease):
                            managed_addresses.add(reply.worker_address)
                    if report is not None:
                        managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
                    surviving_pids = tuple(pid for pid in sorted(managed_pids) if _pid_exists(pid))
                    surviving_children = tuple(
                        child.pid for child in mp.active_children() if child.pid in managed_pids
                    )
                    open_addresses = []
                    for address in sorted(managed_addresses):
                        try:
                            with socket.create_connection(address, timeout=0.1):
                                open_addresses.append(address)
                        except OSError:
                            pass
                    assert not surviving_pids, surviving_pids
                    assert not surviving_children, surviving_children
                    assert not open_addresses, open_addresses
                    assert not cleanup_errors, cleanup_errors
                    if context is not None:
                        assert report is not None
                        assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
                        assert len(report.worker_pids) == 1

    assert context is not None and not overflow
    assert ref is not None
    assert result_pid == context.worker_pid and probe_attempted and probe_clean
    assert replacement_pid is not None
    assert report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.node_exitcode == 0
    assert report.worker_clean and report.worker_exitcode == 0
    assert report.worker_pids == (replacement_pid,)
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert not _pid_exists(context.worker_pid)
    assert not _pid_exists(replacement_pid)
