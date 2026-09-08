"""Bounded publication replay after another placement-group bundle Node exits.

Each exact parameter starts five children: one GCS, two one-CPU Nodes and one
Worker per Node, with 1 MiB per store and no tracing. One two-bundle
STRICT_SPREAD PG runs one tiny, single-return Task on the Driver-local Node.
The other Node is idle and is the only permitted crash target.

Two controlled events are ordered in the original dispatcher: the real GCS
Terminal/Adopted report accepts the completed Task, the managed peer Node
crashes through the complete GCS/survivor/Core barrier, then that one real
metadata ACK is discarded. No fake ACK, test-owned thread/listener, sleep,
replacement ReadyTask or direct adoption call is used. The queued retry must
finish the existing publication even though its PG capability is now LOST.

Work shares fifteen seconds after init; public reference close shares three
seconds and always falls through to shutdown. Startup, synchronous PG control
and shutdown also need the external 30-second process-tree runner; the work
deadline is not transaction cancellation. Run one exact parameter at a time.
Every tracked PID and all six endpoints are checked even after work/close
failure; the deliberately crashed Node remains unclean in the shutdown report.
"""

from __future__ import annotations

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
from miniray.control import GET_PLACEMENT_GROUP_HANDLER
from miniray.node import GET_WORKER_LEASE_OUTCOME_HANDLER
from miniray.ownership import ObjectState
from miniray.recovery import TaskState
from miniray.transport import TransportTimeout, request as rpc_request
from tests.integration.test_task_path import _close_reference, _pid_exists


pytestmark = pytest.mark.multiprocess_smoke
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 32


@ray.remote(num_cpus=1, max_retries=1)
def _identify_and_add(value: int) -> tuple[int, int]:
    return os.getpid(), value + 1


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("PG publication peer-loss work exceeded its deadline")
    return remaining


def _query(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, request, connect_timeout=min(0.5, remaining / 2),
        request_timeout=remaining / 2, deadline=deadline,
    )


@pytest.mark.parametrize("lost_ack", ("terminal", "adopted"))
def test_publication_replay_finishes_after_other_pg_bundle_node_loss(lost_ack):
    context = runtime = core = group = reference = report = None
    original_rpc = original_push = None
    pids, addresses = set(), set()
    close_errors = []
    observation_lock = threading.Lock()
    cut_done = threading.Event()
    overflow = threading.Event()
    cut_claimed = False
    cut = {}
    calls, pushes, push_replies = [], [], []
    selected_report = (
        wire.ReportOutputPublicationTerminal if lost_ack == "terminal"
        else wire.ReportOutputPublicationAdopted
    )

    try:
        context = ray.init(
            num_nodes=2, num_cpus=1, num_workers_per_node=1,
            inline_threshold=1024, object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        survivor, victim = context.nodes
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((
            context.gcs_address, *context.node_addresses, *context.worker_addresses,
        ))
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        addresses.add(runtime.owner_service.address)
        assert len(pids) == 5 and len(addresses) == 6
        assert os.getpid() not in pids and context.trace_address is None
        assert core.node_id == survivor.node_id
        assert len(core._dispatchers) == core._dispatch_lane_count == 2
        assert all(lane.is_alive() for lane in core._dispatchers)

        _remaining(deadline)
        group = ray.placement_group(
            [{"CPU": 1}, {"CPU": 1}], strategy="STRICT_SPREAD",
        )
        _remaining(deadline)
        assert group.bundle_count == 2
        assert {key.node_id for key in group.placements} == {
            survivor.node_id, victim.node_id,
        }
        survivor_key = next(
            key for key in group.placements if key.node_id == survivor.node_id
        )
        group_identity = group.placement_group_id, group.attempt
        original_rpc, original_push = core._rpc, core._push_task_rpc

        def observe_push(address, handler, request):
            with observation_lock:
                if len(pushes) < _MAX_OBSERVATIONS:
                    pushes.append((address, handler, request))
                else:
                    overflow.set()
            reply = original_push(address, handler, request)
            with observation_lock:
                if len(push_replies) < _MAX_OBSERVATIONS:
                    push_replies.append(reply)
                else:
                    overflow.set()
            return reply

        def observe_rpc(address, handler, request):
            nonlocal cut_claimed
            # The actual receiver applies every operation before observation.
            # Overflow never changes its response, timeout or retry behavior.
            reply = original_rpc(address, handler, request)
            should_cut = False
            with observation_lock:
                if type(request) in (
                    wire.ReportOutputPublicationTerminal,
                    wire.ReportOutputPublicationAdopted,
                    wire.AckOutputPublicationAdopted,
                ):
                    if len(calls) < _MAX_OBSERVATIONS:
                        calls.append((address, handler, request, reply))
                    else:
                        overflow.set()
                if (type(request) is selected_report and not cut_claimed
                        and pushes and type(pushes[0][2]) is protocol.PushTask
                        and request.request_identity.publication_id.task_id
                        == pushes[0][2].spec.task_id):
                    cut_claimed = should_cut = True

            if should_cut:
                try:
                    assert threading.current_thread() in core._dispatchers
                    assert not core._state_lock._is_owned()
                    assert address == context.gcs_address
                    assert handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER
                    assert type(reply) is wire.OutputRecoveryReply
                    assert reply.request == request and reply.accepted
                    history = reply.ack.snapshot
                    identity = request.request_identity.publication_id
                    assert history.publication_id == identity
                    assert history.armed and history.complete is not None
                    assert history.manifest.header.node_incarnation.node_id == survivor.node_id
                    assert history.manifest.header.executor_worker_id == survivor.worker_id
                    assert history.manifest.header.owner_worker_id == core.worker_id
                    assert len(history.manifest.slots) == 1
                    assert history.manifest.slots[0].tier is protocol.ResultStorage.INLINE
                    assert history.manifest.slots[0].size_bytes < 1024
                    assert history.manifest.slots[0].transfers == ()
                    with observation_lock:
                        assert len(pushes) == len(push_replies) == 1
                        push_address, _push_handler, push = pushes[0]
                        completed = push_replies[0]
                    assert push_address == survivor.worker_address
                    assert push.spec.scheduling_key == survivor_key
                    assert push.lease_id == identity.lease_id
                    assert type(completed) is protocol.TaskReply
                    assert completed.status is protocol.TaskReplyStatus.SUCCEEDED
                    assert completed.output_publication.manifest == history.manifest
                    assert completed.output_publication.complete == history.complete
                    with core._state_lock:
                        pending = core._task_finish_barriers[identity.output_ids[0]]
                        before = core.owner_table.snapshot(identity.output_ids[0])
                        assert pending.execution == identity.execution
                        assert core._accepted_task_count == 1
                        assert core._placement_group_states[group_identity] is protocol.PlacementGroupPhaseStatus.CREATED
                    assert before.state is (
                        ObjectState.PENDING if lost_ack == "terminal"
                        else ObjectState.READY_INLINE
                    )
                    assert before.error is None
                    assert (history.adopted is not None) is (lost_ack == "adopted")
                    cut["ack"], cut["pending"] = (request, reply), pending

                    # No Core/recorder lock is held across this wait. The
                    # existing Node observer installs local Core truth and
                    # sets its event without waiting for this dispatch lane.
                    death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
                    cut["death"] = death
                    with core._state_lock:
                        assert core._dead_nodes == {victim.node_id: death}
                        installed = core._installed_cluster_snapshot
                        assert installed.membership_epoch >= death.death_epoch
                        assert tuple(node.node_id for node in installed.nodes) == (survivor.node_id,)
                        assert core._placement_group_states[group_identity] is protocol.PlacementGroupPhaseStatus.LOST
                        assert core.owner_table.snapshot(identity.output_ids[0]) == before
                        assert core._task_finish_barriers[identity.output_ids[0]] is pending
                        assert pending.task_key in core._protocol_unresolved
                        assert core._accepted_task_count == 1
                except Exception as exc:
                    # Adoption catches RPC exceptions into its retry path.
                    # Preserve observer failures for the main test thread.
                    cut["error"] = exc
                    raise
                finally:
                    cut_done.set()
                raise TransportTimeout("real publication ACK lost after peer PG Node death")
            return reply

        core._rpc, core._push_task_rpc = observe_rpc, observe_push
        reference = _identify_and_add.options(
            placement_group=group, bundle_index=survivor_key.bundle_index,
        ).remote(41)
        assert cut_done.wait(_remaining(deadline)), "selected metadata cut was not reached"
        assert "error" not in cut, cut.get("error")
        assert cut_claimed and not overflow.is_set()
        lost_request, lost_reply = cut["ack"]
        death, pending = cut["death"], cut["pending"]
        identity = lost_request.request_identity.publication_id
        assert identity.task_id == reference.object_id.task_id
        assert identity.output_ids == (reference.object_id,)
        assert identity.attempt_id.attempt_number == 0
        assert death.node_id == victim.node_id and death.node_pid == victim.node_pid
        assert death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        victim_runtime = next(node for node in runtime.nodes if node.node_id == victim.node_id)
        assert victim_runtime.process.exitcode == death.exit_code
        assert not victim_runtime.process.is_alive()

        # READY may precede the Adopted cut. Wait for the real finish, then
        # inspect the actual retirement ACK that had to precede that finish.
        with core._completion:
            while (reference.object_id in core._task_finish_barriers
                   or pending.task_key in core._protocol_unresolved
                   or core._accepted_task_count):
                core._completion.wait(_remaining(deadline))
            assert pending.task_key in core._finished_tasks
            assert not core._protocol_unresolved and not core._task_finish_barriers
            owner = core.owner_table.snapshot(reference.object_id)
            recovery = core._recovery.task_record(pending.task_id)
            assert owner.state is ObjectState.READY_INLINE and owner.error is None
            assert owner.current_attempt == identity.attempt_id
            assert owner.output_publication.publication_id == identity
            assert recovery.state is TaskState.SUCCEEDED and recovery.retries_started == 0
            assert identity not in getattr(core, "_output_result_custody", {})

        with observation_lock:
            assert not overflow.is_set()
            assert len(pushes) == len(push_replies) == 1
            metadata_calls = tuple(calls)
        selected_calls = tuple(call for call in metadata_calls if type(call[2]) is selected_report)
        assert len(selected_calls) == 2
        assert all(call[2] == lost_request and call[3].accepted for call in selected_calls)
        retirements = tuple(call for call in metadata_calls if type(call[2]) is wire.AckOutputPublicationAdopted)
        assert len(retirements) == 1
        retirement_address, retirement_handler, retirement_request, retirement_reply = retirements[0]
        assert retirement_address == survivor.node_address
        assert retirement_handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
        assert type(retirement_reply) is wire.AckOutputPublicationAdoptedReply
        assert retirement_reply.request == retirement_request and retirement_reply.accepted
        assert retirement_request.proof.complete == lost_reply.ack.snapshot.complete
        assert ray.get(reference, timeout=_remaining(deadline)) == (survivor.worker_pid, 42)

        # Observe the real Node journal after its recorded ACK, without
        # sending a second retirement operation that could repair the bug.
        outcome_request = protocol.GetWorkerLeaseOutcome(
            identity.lease_id, identity.task_id, identity.attempt_id,
            survivor.worker_id, core.worker_id, identity.output_ids, survivor_key,
        )
        outcome = _query(
            survivor.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER, outcome_request, deadline,
        )
        assert type(outcome) is protocol.GetWorkerLeaseOutcomeReply
        assert outcome.found and outcome.worker_alive and not outcome.cleanup_pending
        assert (outcome.lease_id, outcome.task_id, outcome.attempt_id, outcome.executor_worker_id,
                outcome.owner_worker_id, outcome.object_ids, outcome.node_id, outcome.scheduling_key) == (
            identity.lease_id, identity.task_id, identity.attempt_id, survivor.worker_id,
            core.worker_id, identity.output_ids, survivor.node_id, survivor_key,
        )
        assert outcome.state is protocol.LeaseExecutionState.COMPLETED
        assert outcome.completion_status is protocol.TaskReplyStatus.SUCCEEDED
        assert outcome.output_publication is None and outcome.descriptors == ()
        assert outcome.output_completion == retirement_request.proof.complete
        assert outcome.orphan_descriptors == () and outcome.target_execution is None
        history_request = wire.GetOutputPublicationRecovery(identity)
        history_reply = _query(
            context.gcs_address, wire.GET_OUTPUT_PUBLICATION_RECOVERY_HANDLER, history_request, deadline,
        )
        assert type(history_reply) is wire.GetOutputPublicationRecoveryReply
        assert history_reply.request == history_request and history_reply.found
        history = history_reply.snapshot
        assert history.manifest == lost_reply.ack.snapshot.manifest
        assert history.complete == retirement_request.proof.complete
        assert history.adopted == retirement_request.proof
        assert history.frozen_node_death is None and history.rollback is None
        assert history.slot_collections == ()

        pg_reply = _query(
            context.gcs_address, GET_PLACEMENT_GROUP_HANDLER,
            protocol.GetPlacementGroupRequest(group.placement_group_id), deadline,
        )
        assert type(pg_reply) is protocol.GetPlacementGroupReply
        assert pg_reply.found and pg_reply.attempt == group.attempt
        assert pg_reply.phase is protocol.PlacementGroupPhaseStatus.LOST
        assert pg_reply.placements == ()
        submission_index = core._submission_index
        for bundle_index in range(group.bundle_count):
            _remaining(deadline)
            with pytest.raises(ray.PlacementGroupLostError):
                _identify_and_add.options(
                    placement_group=group, bundle_index=bundle_index,
                ).remote(99)
        assert core._submission_index == submission_index
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            try:
                _close_reference(reference, cleanup_deadline)
            except Exception as exc:
                close_errors.append(exc)
        finally:
            try:
                if core is not None and original_rpc is not None:
                    core._rpc, core._push_task_rpc = original_rpc, original_push
            finally:
                try:
                    report = ray.shutdown()
                finally:
                    surviving_pids = tuple(pid for pid in sorted(pids) if _pid_exists(pid))
                    surviving_children = tuple(child.pid for child in mp.active_children() if child.pid in pids)
                    open_addresses = []
                    for address in sorted(addresses):
                        try:
                            with socket.create_connection(address, timeout=0.1):
                                open_addresses.append(address)
                        except OSError:
                            pass
                    assert not surviving_pids, surviving_pids
                    assert not surviving_children, surviving_children
                    assert not open_addresses, open_addresses
                    assert not close_errors, close_errors

    assert context is not None and report is not None and cut_done.is_set()
    assert not ray.is_initialized() and not overflow.is_set()
    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert report.gcs_pid == context.gcs_pid
    assert not report.gcs_forced and not report.forced
    survivor, victim = context.nodes
    death = cut["death"]
    assert report.node_pids == (survivor.node_pid, victim.node_pid)
    assert report.node_exitcodes == (0, death.exit_code)
    assert report.node_cleans == (True, False)
    assert report.node_forced == (False, False)
    assert report.node_finalized == (True, False)
    assert report.node_shutdown_ack_clean == (True, False)
    assert report.node_resources_clean == (True, False)
    assert report.worker_pids == (survivor.worker_pid, victim.worker_pid)
    assert report.worker_exitcodes == (0, None)
    assert report.worker_cleans == (True, False)
    assert report.worker_forced == (False, False)
    expected_exit, recorded_crash = report.node_deaths
    assert expected_exit is not None and expected_exit.reason is protocol.NodeDeathReason.EXPECTED
    assert expected_exit.exit_code == 0 and recorded_crash == death
