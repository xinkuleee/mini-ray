"""F2: targeted borrowed ARM loss, with one shared workflow for two scopes.

The narrow case selects STORED slot 1 of two returns. The mixed case selects
slots 1 and 2 of three returns while healthy INLINE slot 0 stays unchanged.
Initial targets are STORED on idle home Node A; public drop_object removes
each as a reconstruction precondition. A blocker occupies A and one public
wait(timeout=0) records all losses while holding the existing Core RLock, so
the coordinator cannot close OPEN between them. No target state is invented.

The test producer deliberately depends on its real execution attempt: in the
mixed case slot 1 has 8 KiB padding only in attempt 0 and is INLINE in attempts
1/2; slot 2 is always STORED. This is legal per-attempt serialization-tier
change, not a claim of deterministic outputs. The unselected recomputed slot
0 is ignored by the runtime's normal selected-output path. Every slot shares
one live Driver child. B's existing AFTER_ARM gate then observes a mixed
selected batch (original indices 1/2, ordinals 0/1), but no owner Complete.

One Node-B crash requires exact selected cleanup before SYSTEM retry on A.
max_retries=2 counts one explicit reconstruction and one SYSTEM retry. One or
two debug drops are deliberate setup events, distinct from the Node crash.
Each case has five startup children, two 1 MiB stores, one tiny put, a blocker
and one producer executing at most three times; at most two 8 KiB replicas.
One listener/two connections, no extra test thread, Actor/PG or tracing.

Post-init work shares 15 s; the gate stays capped at 10 s and final refs/gate
cleanup shares 3 s. Four grant keys, two cleanup ACKs and two retry cuts bound
observations; five/narrow or eleven/mixed physical queries occur on success.
No owner history, death, cleanup ACK or retry is fabricated. Run each exact ID
alone through the 30 s process-tree runner plus its bounded reap grace.
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
from miniray.node import REQUEST_LEASE_HANDLER, SHUTDOWN_STATUS_HANDLER
from miniray.output_recovery import OutputRecoveryAction, OutputRecoveryOwnerDecision
from miniray.ownership import ObjectCollectionState, ObjectOwnerSnapshot, ObjectState
from miniray.publication_gate import (
    OUTPUT_PUBLICATION_GATE_RELEASE, OutputPublicationGateConfig,
    OutputPublicationGatePhase, recv_output_publication_gate_arrival,
)
from miniray.publication_sources import BorrowedContainedSource
from miniray.recovery import TaskState
from miniray.runtime_binding import current_execution_context
from miniray.targeted_reconstruction import TargetedSessionPhase
from miniray.task_outputs import TargetExecutionKey
from tests.integration.test_borrowed_output_unknown_path import _physical, _wait
from tests.integration.test_multi_contained_output_path import _close_local
from tests.integration.test_stored_outer_node_loss_path import (
    _BLOCKER_RELEASE, _SURVIVOR_RESOURCE, _assert_metadata_only, _graph,
    _node_loss, _occupy_survivor, _pid_exists, _poll_until, _query,
    _recovery, _recv_exact, _release_connection, _remaining,
)


pytestmark = pytest.mark.multiprocess_smoke
_WORK_SECONDS = 15.0
_SOURCE_VALUE = ("targeted-unknown-driver-child", 42)
_PADDING = b"T" * (8 * 1024)


@ray.remote(num_cpus=1, num_returns=2, max_retries=2)
def _mixed_targeted_child(container, mixed_targets=False):
    child, = container
    assert isinstance(child, ray.ObjectRef) and child.borrower_token is not None
    execution = current_execution_context()
    assert execution is not None and execution.parent_attempt_id.attempt_number in (0, 1, 2)
    count = 3 if mixed_targets else 2
    values = tuple({"child": child, "slot": index} for index in range(count))
    for index in range(1, count):
        if index == count - 1 or execution.parent_attempt_id.attempt_number == 0:
            values[index].update(padding=_PADDING, executor_pid=os.getpid())
    return values


@dataclass(frozen=True)
class _TargetRetryCut:
    resolution: object
    current_attempt: AttemptID
    retries_started: int
    execution: TargetExecutionKey
    reconstruction_origin_attempt: AttemptID
    healthy: ObjectOwnerSnapshot
    targets: tuple[ObjectOwnerSnapshot, ...]
    source: ObjectOwnerSnapshot
    dependency_hold: protocol.TaskReferenceHold
    release_seen: tuple[bool, ...]
    healthy_hold_released: bool
    had_envelope: bool


def test_targeted_borrowed_arm_loss_retries_only_lost_slot_and_preserves_healthy_sibling():
    _run_targeted(mixed_targets=False)


def test_targeted_mixed_borrowed_arm_loss_retries_selected_batch_and_preserves_healthy_sibling():
    _run_targeted(mixed_targets=True)


def _run_targeted(*, mixed_targets):
    return_count = 3 if mixed_targets else 2
    target_indices = tuple(range(1, return_count))
    target_count = len(target_indices)
    target_tiers = ((protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE)
                    if mixed_targets else (protocol.ResultStorage.OBJECT_STORE,))
    target_states = tuple(ObjectState.READY_INLINE if tier is protocol.ResultStorage.INLINE
                          else ObjectState.READY_STORED for tier in target_tiers)
    physical_query_limit = 6 * target_count - 1
    listener = blocker_connection = publication_connection = None
    context = core = report = death = None
    source = blocker = None
    refs, restored = (), []
    publication = healthy_transfer = None
    transfers = ()
    original_rpc = original_retry = None
    pids, addresses = set(), set()
    observation_lock = threading.Lock()
    requests, grants, cleanup_acks, retry_cuts = {}, {}, {}, {}
    conflicts = set()
    overflow = threading.Event()
    physical_queries, close_errors = [], []
    phase = OutputPublicationGatePhase.AFTER_ARM_ACK_BEFORE_COMPLETE
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        address = listener.getsockname()
        addresses.add(address)
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _SURVIVOR_RESOURCE: 1}, {"CPU": 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024,
            _test_output_publication_gate=OutputPublicationGateConfig(1, address, phase, 10.0),
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        survivor, victim = context.nodes
        assert core.node_id == survivor.node_id and context.trace_address is None
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, runtime.owner_service.address))
        for node in context.nodes:
            assert len(node.worker_ids) == 1
            addresses.update((node.node_address, node.worker_address))
        assert len(pids) == 5 and len(addresses) == 7 and os.getpid() not in pids
        source = ray.put(_SOURCE_VALUE)
        source_id = source.object_id
        initial_source = core.owner_table.snapshot(source_id)
        assert initial_source.state is ObjectState.READY_INLINE
        assert source.owner_worker_id == core.worker_id and source.borrower_token is None
        original_rpc, original_retry = core._rpc, core._retry_system_failure

        def physical(node, object_id):
            physical_queries.append((node.node_id, object_id))
            assert len(physical_queries) <= physical_query_limit
            return _physical(node, object_id, deadline)

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
                snapshot = (reply.snapshot if type(reply) is wire.OutputNodeLossReply else
                            reply.snapshot if type(reply) is wire.GetOutputNodeLossReply and reply.found else None)
                if snapshot is not None and snapshot.resolution is not None:
                    identity = snapshot.publication_id
                    if identity not in cleanup_acks and len(cleanup_acks) >= 2:
                        overflow.set()
                    elif cleanup_acks.setdefault(identity, snapshot.resolution) != snapshot.resolution:
                        conflicts.add((identity.task_id, identity.attempt_id.attempt_number))
            return reply

        def inspect_retry(pending, error, **kwargs):
            if transfers and publication is not None and pending.task_id == publication.task_id:
                with core._completion:
                    record = core._recovery.task_record(pending.task_id)
                    child = core.owner_table.snapshot(source_id)
                    released = tuple(core.owner_table.contained_release_was_seen(source_id, hold)
                                     for transfer in transfers for hold in (transfer.final_hold, transfer.provisional_hold))
                    healthy_released = core.owner_table.contained_release_was_seen(source_id, healthy_transfer.final_hold)
                    with observation_lock:
                        key = pending.task_id, pending.spec.attempt_id
                        if key not in retry_cuts and len(retry_cuts) >= 2:
                            overflow.set()
                        else:
                            retry_cuts.setdefault(key, _TargetRetryCut(
                                cleanup_acks.get(publication), record.current_attempt, record.retries_started,
                                pending.target_execution, pending.reconstruction_origin_attempt,
                                core.owner_table.snapshot(output_ids[0]),
                                tuple(core.owner_table.snapshot(object_id) for object_id in pending.output_ids),
                                child, pending.dependency_hold, released, healthy_released,
                                publication in getattr(core, "_output_result_custody", {}),
                            ))
            return original_retry(pending, error, **kwargs)

        core._rpc, core._retry_system_failure = inspect_rpc, inspect_retry
        refs = tuple(_mixed_targeted_child.options(num_returns=return_count).remote([source], mixed_targets))
        assert len(refs) == return_count
        output_ids = tuple(reference.object_id for reference in refs)
        target_ids = tuple(output_ids[index] for index in target_indices)
        target_refs = tuple(refs[index] for index in target_indices)
        task_id = output_ids[0].task_id
        assert tuple(value.return_index for value in output_ids) == tuple(range(return_count))
        values = ray.get(refs, timeout=_remaining(deadline))
        assert len(values) == return_count
        for index, value in enumerate(values):
            child = value["child"]
            restored.append(child)
            assert value["slot"] == index and isinstance(child, ray.ObjectRef)
            assert child.object_id == source_id and child.owner_worker_id == core.worker_id and child.borrower_token is None
            assert ray.get(child, timeout=_remaining(deadline)) == _SOURCE_VALUE
        for index in target_indices:
            assert values[index]["padding"] == _PADDING and values[index]["executor_pid"] == survivor.worker_pid
        _wait(core, lambda: all(object_id not in core._task_finish_barriers for object_id in output_ids), deadline)
        initial_outputs = tuple(core.owner_table.snapshot(object_id) for object_id in output_ids)
        healthy = initial_outputs[0]
        assert healthy.state is ObjectState.READY_INLINE
        assert all(initial_outputs[index].state is ObjectState.READY_STORED for index in target_indices)
        assert all(value.current_attempt == AttemptID(task_id, 0) for value in initial_outputs)
        initial_members = tuple(value.output_publication for value in initial_outputs)
        assert all(member is not None for member in initial_members)
        initial_publication = initial_members[0].publication_id
        assert all(member.publication_id == initial_publication for member in initial_members)
        assert initial_publication.output_ids == initial_publication.full_output_ids == output_ids
        assert initial_members[0].manifest.header.node_incarnation.node_id == survivor.node_id
        assert initial_members[0].manifest.header.executor_worker_id == survivor.worker_id
        assert tuple(member.slot.tier for member in initial_members) == (
            protocol.ResultStorage.INLINE, *(protocol.ResultStorage.OBJECT_STORE for _ in target_indices),
        )
        initial_transfers = tuple(member.slot.transfers[0] for member in initial_members)
        healthy_transfer = initial_transfers[0]
        replaced_transfers = tuple(initial_transfers[index] for index in target_indices)
        assert all(type(item.source) is BorrowedContainedSource for item in initial_transfers)
        assert all(item.source == healthy_transfer.source for item in initial_transfers)
        assert type(healthy_transfer.source.original_source) is protocol.TaskHoldSource
        initial_hold = healthy_transfer.source.original_source.hold
        assert initial_hold.origin_attempt_id == AttemptID(task_id, 0)
        assert len({item.final_hold for item in initial_transfers}) == return_count
        initial_history = _recovery(context, initial_publication, deadline)
        assert initial_history.complete is not None and initial_history.adopted is not None and not initial_history.slot_collections
        for index in target_indices:
            initial_physical = physical(survivor, output_ids[index])
            assert initial_physical.found and initial_physical.sealed and initial_physical.producer_attempt_id == AttemptID(task_id, 0)
            assert initial_physical.checksum == initial_members[index].slot.checksum
        for reference in restored:
            _close_local(reference, deadline)
        baseline_child = core.owner_table.snapshot(source_id)
        assert baseline_child.local_tokens == initial_source.local_tokens
        assert baseline_child.contained_holds == frozenset(item.final_hold for item in initial_transfers)
        assert not baseline_child.submitted_tokens and not baseline_child.borrowed_tokens and len(baseline_child.lineage_tokens) == 1

        def healthy_unchanged():
            assert core.owner_table.snapshot(output_ids[0]) == healthy
            child = core.owner_table.snapshot(source_id)
            assert healthy_transfer.final_hold in child.contained_holds
            assert not core.owner_table.contained_release_was_seen(source_id, healthy_transfer.final_hold)
            return child

        for index in target_indices:
            assert ray.drop_object(refs[index], node_id=survivor.node_id)
            lost = core.owner_table.snapshot(output_ids[index])
            assert lost.state is ObjectState.LOST and lost.current_attempt == AttemptID(task_id, 0)
            assert lost.output_publication == initial_members[index] and not lost.locations
            assert not physical(survivor, output_ids[index]).found
        healthy_unchanged()
        assert core._recovery.task_record(task_id).retries_started == 0
        blocker = _occupy_survivor.remote(address, deadline)
        listener.settimeout(_remaining(deadline))
        blocker_connection, _ = listener.accept()
        assert int.from_bytes(_recv_exact(blocker_connection, 8, deadline), "big") == survivor.worker_pid
        # Both public loss observations enter OPEN before the coordinator may
        # close its target vector. The RLock schedules events, not owner data.
        with core._completion:
            ready, pending = ray.wait(target_refs, num_returns=target_count, timeout=0)
        assert ready == [] and pending == list(target_refs)
        listener.settimeout(_remaining(deadline))
        publication_connection, _ = listener.accept()
        publication_connection.settimeout(_remaining(deadline))
        arrival = recv_output_publication_gate_arrival(publication_connection)
        publication = arrival.publication_id
        assert arrival.phase is phase
        assert (arrival.node_id, arrival.node_pid, arrival.registration_epoch) == (
            victim.node_id, victim.node_pid, runtime.nodes[1].registration_epoch,
        )
        assert type(publication.execution) is TargetExecutionKey
        assert publication.full_output_ids == output_ids and publication.output_ids == target_ids
        assert publication.attempt_id == AttemptID(task_id, 1) and publication != initial_publication
        _assert_metadata_only(arrival)
        before = _recovery(context, publication, deadline)
        manifest = before.manifest
        assert manifest.manifest_digest == arrival.manifest_digest
        assert manifest.header.owner_worker_id == core.worker_id and manifest.header.executor_worker_id == victim.worker_id
        assert before.armed and before.complete is None and before.recovery_action is OutputRecoveryAction.COMPLETION_UNKNOWN
        assert before.adopted is before.rollback is before.owner_decision is before.resolution is None
        slots = manifest.slots
        assert tuple(slot.object_id for slot in slots) == target_ids
        assert tuple(slot.object_id.return_index for slot in slots) == target_indices
        assert tuple(slot.tier for slot in slots) == target_tiers
        assert all(len(slot.transfers) == 1 for slot in slots)
        transfers = tuple(slot.transfers[0] for slot in slots)
        borrowed = transfers[0].source
        assert type(borrowed) is BorrowedContainedSource and all(item.source == borrowed for item in transfers)
        assert borrowed.borrower_worker_id == victim.worker_id and type(borrowed.original_source) is protocol.TaskHoldSource
        reconstruction_hold = borrowed.original_source.hold
        assert reconstruction_hold != initial_hold
        assert (reconstruction_hold.kind, reconstruction_hold.submitting_worker_id, reconstruction_hold.task_id,
                reconstruction_hold.origin_attempt_id) == (
            protocol.TaskReferenceHoldKind.SUBMITTED, core.worker_id, task_id, publication.attempt_id,
        )
        for object_id, transfer in zip(target_ids, transfers):
            assert (transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.contained_owner_address) == (
                source_id, core.worker_id, runtime.owner_service.address,
            )
            assert transfer.final_hold.container_owner_worker_id == core.worker_id
            assert transfer.provisional_hold.container_owner_worker_id == victim.worker_id
            assert transfer.final_hold.container_object_id == transfer.provisional_hold.container_object_id == object_id
        at_arm = healthy_unchanged()
        assert at_arm.contained_holds == frozenset((healthy_transfer.final_hold, *(item.final_hold for item in transfers)))
        assert len({hold for item in transfers for hold in (item.final_hold, item.provisional_hold)}) == 2 * target_count
        assert at_arm.submitted_tokens == frozenset((reconstruction_hold,))
        assert at_arm.borrowed_tokens == frozenset((borrowed.owner_table_token,))
        assert at_arm.lineage_tokens == baseline_child.lineage_tokens and at_arm.local_tokens == initial_source.local_tokens
        assert at_arm.inline_data == initial_source.inline_data and at_arm.current_attempt == initial_source.current_attempt
        for transfer in transfers:
            assert core.owner_table.contained_release_was_seen(source_id, transfer.provisional_hold)
            assert not core.owner_table.contained_release_was_seen(source_id, transfer.final_hold)
        assert all(core.owner_table.contained_release_was_seen(source_id, hold)
                   for transfer in replaced_transfers for hold in (transfer.final_hold, transfer.provisional_hold))
        old_retired = _recovery(context, initial_publication, deadline)
        assert tuple((proof.slot_index, proof.object_id) for proof in old_retired.slot_collections) == tuple(zip(target_indices, target_ids))
        assert old_retired.complete == initial_history.complete and old_retired.adopted == initial_history.adopted
        assert _graph(context, publication, deadline).manifest == manifest.to_graph_manifest()
        for slot in slots:
            stored = physical(victim, slot.object_id)
            if slot.tier is protocol.ResultStorage.INLINE:
                assert 0 < slot.size_bytes <= 1024 and not stored.found
            else:
                assert len(_PADDING) < slot.size_bytes < 32 * 1024
                assert stored.found and stored.sealed and stored.owner_worker_id == core.worker_id
                assert stored.producer_attempt_id == publication.attempt_id and stored.size_bytes == slot.size_bytes
                assert stored.checksum == slot.checksum == hashlib.sha256(stored.data).hexdigest()
            target = core.owner_table.snapshot(slot.object_id)
            assert target.state is ObjectState.PENDING and target.current_attempt == publication.attempt_id
            assert target.output_publication is target.inline_data is target.canonical_stored_result is None and not target.locations
        assert publication not in getattr(core, "_output_result_custody", {})
        assert core._recovery.task_record(task_id).retries_started == 1

        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert (death.node_id, death.node_pid, death.registration_epoch) == (arrival.node_id, arrival.node_pid, arrival.registration_epoch)
        assert death.exit_code == -signal.SIGKILL and death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        publication_connection.close()
        publication_connection = None

        def observed_retry():
            with observation_lock:
                return retry_cuts.get((task_id, publication.attempt_id))

        cut = _poll_until(observed_retry, deadline, "targeted borrowed cleanup never reached SYSTEM retry")
        assert cut.current_attempt == publication.attempt_id and cut.retries_started == 1
        assert cut.execution == publication.execution and cut.reconstruction_origin_attempt == publication.attempt_id
        assert cut.healthy == healthy and not cut.healthy_hold_released and not cut.had_envelope
        assert tuple(value.object_id for value in cut.targets) == target_ids
        assert all(value.state is ObjectState.PENDING and value.current_attempt == publication.attempt_id
                   and value.output_publication is None and value.inline_data is None and not value.locations for value in cut.targets)
        resolution = cut.resolution
        assert resolution is not None and resolution.publication_id == publication
        assert resolution.manifest_digest == manifest.manifest_digest and resolution.owner_worker_id == core.worker_id
        assert resolution.node_death == death and resolution.complete is None and resolution.kept_slots == ()
        assert cut.release_seen == (True, True) * target_count
        assert cut.source.contained_holds == frozenset((healthy_transfer.final_hold,))
        assert cut.dependency_hold == reconstruction_hold and cut.source.submitted_tokens == at_arm.submitted_tokens
        assert cut.source.lineage_tokens == at_arm.lineage_tokens and cut.source.inline_data == at_arm.inline_data
        terminal = _node_loss(context, publication, core.worker_id, death, deadline)
        assert terminal.work.action is OutputRecoveryAction.COMPLETION_UNKNOWN
        assert terminal.work.snapshot == replace(before, frozen_node_death=death)
        resolved = terminal.snapshot
        assert resolved.resolution == resolution and resolved.complete is None and resolved.owner_decision.complete is None
        assert tuple((value.slot_index, value.object_id, value.decision) for value in resolved.owner_decision.slots) == tuple(
            (ordinal, object_id, OutputRecoveryOwnerDecision.DROP) for ordinal, object_id in enumerate(target_ids)
        )
        assert resolved.adopted is resolved.rollback is resolved.owner_death is resolved.owner_cleaned is None
        assert not resolved.terminal_report_allowed and _recovery(context, publication, deadline) == resolved
        healthy_unchanged()

        def retry_admitted():
            record = core._recovery.task_record(task_id)
            session = core._targeted_reconstruction_coordinator().current_session(task_id)
            return (record.current_attempt == AttemptID(task_id, 2) and record.retries_started == 2
                    and record.state is TaskState.RETRY_PENDING and session is not None
                    and session.phase is TargetedSessionPhase.STARTED
                    and session.execution == publication.execution.for_attempt(AttemptID(task_id, 2)))

        _wait(core, retry_admitted, deadline)

        def dead_borrower_retired():
            child = core.owner_table.snapshot(source_id)
            installed = core.owner_table.dead_worker_record(victim.worker_id)
            return child if (installed is not None and borrowed.owner_table_token not in child.borrowed_tokens
                             and borrowed.owner_table_token in child.released_borrowed_tokens) else None

        child_after_death = _wait(core, dead_borrower_retired, deadline)
        assert child_after_death.submitted_tokens == at_arm.submitted_tokens
        assert child_after_death.lineage_tokens == baseline_child.lineage_tokens
        worker = _query(context.gcs_address, GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(victim.worker_id), deadline)
        assert type(worker) is protocol.GetWorkerStateReply and worker.found and worker.worker_id == victim.worker_id
        assert worker.state is protocol.WorkerMembershipState.DEAD and worker.death.reason is protocol.WorkerDeathReason.NODE_EXIT
        assert (worker.death.node_id, worker.death.node_pid, worker.death.node_registration_epoch, worker.death.worker_pid) == (
            victim.node_id, victim.node_pid, arrival.registration_epoch, victim.worker_pid,
        )
        assert core.owner_table.dead_worker_record(victim.worker_id).death_id == _worker_death_reference_id(worker.death)
        healthy_unchanged()
        with observation_lock:
            assert {attempt for task, attempt in grants if task == task_id} == {0, 1}
            assert len(retry_cuts) == 1 and not conflicts and not overflow.is_set()
        blocker_connection.settimeout(_remaining(deadline))
        blocker_connection.sendall(_BLOCKER_RELEASE)
        assert ray.get(blocker, timeout=_remaining(deadline)) == survivor.worker_pid
        blocker_connection.close()
        blocker_connection = None
        rebuilt = ray.get(target_refs, timeout=_remaining(deadline))
        assert len(rebuilt) == target_count
        for index, tier, value in zip(target_indices, target_tiers, rebuilt):
            child = value["child"]
            restored.append(child)
            assert value["slot"] == index
            if tier is protocol.ResultStorage.OBJECT_STORE:
                assert value["padding"] == _PADDING and value["executor_pid"] == survivor.worker_pid
            else:
                assert "padding" not in value and "executor_pid" not in value
            assert isinstance(child, ray.ObjectRef) and child.object_id == source_id
            assert child.owner_worker_id == core.worker_id and child.borrower_token is None
            assert ray.get(child, timeout=_remaining(deadline)) == _SOURCE_VALUE
        _wait(core, lambda: all(object_id not in core._task_finish_barriers for object_id in target_ids), deadline)
        final_targets = tuple(core.owner_table.snapshot(object_id) for object_id in target_ids)
        assert tuple(value.state for value in final_targets) == target_states
        assert all(value.current_attempt == AttemptID(task_id, 2) for value in final_targets)
        final_members = tuple(value.output_publication for value in final_targets)
        assert all(member is not None for member in final_members)
        assert tuple(member.slot_index for member in final_members) == tuple(range(target_count))
        assert tuple(member.slot.object_id.return_index for member in final_members) == target_indices
        final_publication = final_members[0].publication_id
        assert all(member.publication_id == final_publication and member.manifest == final_members[0].manifest for member in final_members)
        assert type(final_publication.execution) is TargetExecutionKey
        assert final_publication.execution == publication.execution.for_attempt(AttemptID(task_id, 2))
        assert final_publication.output_ids == target_ids and final_publication.full_output_ids == output_ids
        assert final_members[0].manifest.header.node_incarnation.node_id == survivor.node_id
        assert tuple(member.slot.tier for member in final_members) == target_tiers
        final_transfers = tuple(member.slot.transfers[0] for member in final_members)
        assert all(type(item.source) is BorrowedContainedSource for item in final_transfers)
        final_borrowed = final_transfers[0].source
        assert all(item.source == final_borrowed for item in final_transfers)
        assert final_borrowed.borrower_worker_id == survivor.worker_id and final_borrowed.original_source == borrowed.original_source
        all_old_holds = {hold for item in initial_transfers + transfers for hold in (item.final_hold, item.provisional_hold)}
        final_holds = {hold for item in final_transfers for hold in (item.final_hold, item.provisional_hold)}
        assert len(final_holds) == 2 * target_count and not final_holds.intersection(all_old_holds)
        for object_id, item in zip(target_ids, final_transfers):
            assert item.contained_object_id == source_id and item.contained_owner_worker_id == core.worker_id
            assert item.final_hold.container_owner_worker_id == core.worker_id
            assert item.final_hold.container_object_id == item.provisional_hold.container_object_id == object_id
        assert all(core.owner_table.contained_release_was_seen(source_id, hold)
                   for item in transfers for hold in (item.final_hold, item.provisional_hold))
        assert all(core.owner_table.contained_release_was_seen(source_id, item.provisional_hold) for item in final_transfers)
        child_after_retry = healthy_unchanged()
        assert child_after_retry.contained_holds == frozenset((healthy_transfer.final_hold, *(item.final_hold for item in final_transfers)))
        assert child_after_retry.lineage_tokens == baseline_child.lineage_tokens
        assert not child_after_retry.submitted_tokens and not child_after_retry.borrowed_tokens
        assert final_borrowed.owner_table_token in child_after_retry.released_borrowed_tokens
        record = core._recovery.task_record(task_id)
        assert record.state is TaskState.SUCCEEDED and record.max_retries == record.retries_started == 2
        assert record.retries_remaining == 0 and record.current_attempt == AttemptID(task_id, 2)
        assert core._targeted_reconstruction_coordinator().current_session(task_id) is None
        assert core._recovery.active_recovery(task_id) is None
        final_physical = []
        for member in final_members:
            new_physical = physical(survivor, member.object_id)
            final_physical.append(new_physical)
            if member.slot.tier is protocol.ResultStorage.INLINE:
                assert not new_physical.found
            else:
                assert new_physical.found and new_physical.sealed and new_physical.producer_attempt_id == AttemptID(task_id, 2)
                assert new_physical.owner_worker_id == core.worker_id and new_physical.size_bytes == member.slot.size_bytes
                assert new_physical.checksum == member.slot.checksum == hashlib.sha256(new_physical.data).hexdigest()
        old_progress = wire.ProgressOutputNodeLoss(terminal.work)
        replay = _query(context.gcs_address, wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER, old_progress, deadline)
        assert type(replay) is wire.OutputNodeLossReply and replay.request == old_progress and replay.snapshot == resolved
        assert healthy_unchanged() == child_after_retry
        assert tuple(core.owner_table.snapshot(object_id) for object_id in target_ids) == final_targets
        assert _recovery(context, initial_publication, deadline) == old_retired
        assert _recovery(context, publication, deadline) == resolved
        final_history = _recovery(context, final_publication, deadline)
        assert final_history.complete is not None and final_history.adopted is not None and not final_history.slot_collections
        with observation_lock:
            task_grants = {attempt: pair for (task, attempt), pair in grants.items() if task == task_id}
            assert set(task_grants) == {0, 1, 2} and len(retry_cuts) == 1 and not conflicts and not overflow.is_set()
        assert tuple(task_grants[index][1].node_id for index in (0, 1, 2)) == (survivor.node_id, victim.node_id, survivor.node_id)
        assert len({pair[0].lease_id for pair in task_grants.values()}) == 3
        assert task_grants[0][0].return_ids == output_ids and task_grants[0][0].target_execution is None
        for attempt in (1, 2):
            request, grant = task_grants[attempt]
            execution = publication.execution.for_attempt(AttemptID(task_id, attempt))
            assert request.return_ids == target_ids and request.target_execution == grant.target_execution == execution

        for reference in restored[return_count:]:
            _close_local(reference, deadline)
        prior_collections = ()
        for ordinal, object_id in enumerate(target_ids):
            _close_local(target_refs[ordinal], deadline)
            _wait(core, lambda: core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED, deadline)
            assert not physical(survivor, object_id).found
            after_target_gc = healthy_unchanged()
            remaining_transfers = final_transfers[ordinal + 1:]
            assert after_target_gc.contained_holds == frozenset((healthy_transfer.final_hold, *(item.final_hold for item in remaining_transfers)))
            assert after_target_gc.lineage_tokens == baseline_child.lineage_tokens and after_target_gc.local_tokens == initial_source.local_tokens
            assert core.owner_table.contained_release_was_seen(source_id, final_transfers[ordinal].final_hold)
            for other in range(ordinal + 1, target_count):
                assert core.owner_table.snapshot(target_ids[other]) == final_targets[other]
                assert not core.owner_table.contained_release_was_seen(source_id, final_transfers[other].final_hold)
                assert physical(survivor, target_ids[other]) == final_physical[other]
            collected_target = _recovery(context, final_publication, deadline)
            assert tuple((proof.slot_index, proof.object_id) for proof in collected_target.slot_collections) == tuple(enumerate(target_ids[:ordinal + 1]))
            assert collected_target.slot_collections[:len(prior_collections)] == prior_collections
            prior_collections = collected_target.slot_collections
            assert _recovery(context, initial_publication, deadline) == old_retired
        assert len(physical_queries) == physical_query_limit
        assert ray.get(source, timeout=_remaining(deadline)) == _SOURCE_VALUE
        _close_local(refs[0], deadline)
        _wait(core, lambda: core.owner_table.collection_state(output_ids[0]) is ObjectCollectionState.COLLECTED, deadline)
        after_both = core.owner_table.snapshot(source_id)
        assert not after_both.contained_holds and not after_both.lineage_tokens
        assert not after_both.submitted_tokens and not after_both.borrowed_tokens and not after_both.retained_tokens
        assert after_both.local_tokens == initial_source.local_tokens
        assert core.owner_table.contained_release_was_seen(source_id, healthy_transfer.final_hold)
        collected_initial = _recovery(context, initial_publication, deadline)
        assert tuple((proof.slot_index, proof.object_id) for proof in collected_initial.slot_collections) == tuple(enumerate(output_ids))
        assert collected_initial.slot_collections[1:] == old_retired.slot_collections
        assert _recovery(context, publication, deadline) == resolved
        _close_local(blocker, deadline)
        _close_local(source, deadline)
        _wait(core, lambda: all(core.owner_table.collection_state(reference.object_id) is ObjectCollectionState.COLLECTED
                               for reference in (blocker, source)), deadline)
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations
        assert not core._output_retirement_work

        def survivor_clean():
            status = _query(survivor.node_address, SHUTDOWN_STATUS_HANDLER,
                            protocol.ShutdownStatusRequest("inspect-targeted-borrowed-unknown"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested
            return status if status.resources_clean else None

        assert _poll_until(survivor_clean, deadline, "targeted survivor cleanup did not settle").child_pids == (survivor.worker_pid,)
        assert not _pid_exists(victim.node_pid) and not _pid_exists(victim.worker_pid)
    finally:
        cleanup_deadline = time.monotonic() + 3.0
        try:
            _release_connection(publication_connection, OUTPUT_PUBLICATION_GATE_RELEASE, cleanup_deadline)
            _release_connection(blocker_connection, _BLOCKER_RELEASE, cleanup_deadline)
            if listener is not None:
                listener.close()
            for reference in (*restored, *refs, blocker, source):
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
    _poll_until(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0,
                "managed process survived targeted-output unknown shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
