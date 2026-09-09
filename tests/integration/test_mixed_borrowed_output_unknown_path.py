"""F1: mixed shared-borrowed outputs lose their publisher after ARM.

One live Driver child is returned in two slots, INLINE then OBJECT_STORE. The
real AFTER_ARM_ACK_BEFORE_COMPLETE gate observes both promotions, but neither
Complete nor any output bytes have reached the owner. Publisher death must
therefore resolve both slots as DROP. Each slot's old final/provisional holds
must be retired before one actual SYSTEM retry reuses the shared Task hold.

Five startup children (GCS, two Nodes, one Worker each), one survivor blocker,
one two-return producer with at most two physical attempts, one tiny put, 8 KiB
padding and two 1 MiB stores. One listener/two connections, one managed Node
crash, no new test thread, Actor, PG or tracing. The existing gate has a 10 s
bound; all post-init work shares 12 s and final refs/gate cleanup shares 3 s.

Observations retain at most four task-attempt grant keys, two cleanup ACKs and
two retry cuts; exactly six physical queries are made on the successful path.
Only real RPC receipts/death facts/GC are used. Run this exact test alone with
the 30-second process-tree runner and its bounded TERM/KILL/reap grace.
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
from tests.integration.test_borrowed_output_unknown_path import _physical, _wait
from tests.support._legacy_reference_cleanup import _close_local
from tests.integration.test_stored_outer_node_loss_path import (
    _BLOCKER_RELEASE, _SURVIVOR_RESOURCE, _assert_metadata_only, _graph,
    _node_loss, _occupy_survivor, _pid_exists, _poll_until, _query,
    _recovery, _recv_exact, _release_connection, _remaining,
)


pytestmark = pytest.mark.multiprocess_smoke
_WORK_SECONDS = 12.0
_SOURCE_VALUE = ("mixed-armed-shared-driver-child", 42)
_PADDING = b"M" * (8 * 1024)


@ray.remote(num_cpus=1, num_returns=2, max_retries=1)
def _return_mixed_borrowed_child(container):
    child, = container
    assert isinstance(child, ray.ObjectRef) and child.borrower_token is not None
    return {"child": child, "slot": 0}, {
        "child": child, "slot": 1, "padding": _PADDING, "executor_pid": os.getpid(),
    }


@dataclass(frozen=True)
class _RetryCut:
    resolution: object
    current_attempt: AttemptID
    retries_started: int
    outputs: tuple[ObjectOwnerSnapshot, ...]
    source: ObjectOwnerSnapshot
    dependency_hold: protocol.TaskReferenceHold
    release_seen: tuple[bool, ...]
    had_envelope: bool


def test_armed_unknown_mixed_borrowed_outputs_release_each_old_slot_before_one_retry():
    listener = blocker_connection = publication_connection = None
    context = core = report = death = None
    source = blocker = None
    refs, restored = (), []
    transfers = ()
    publication = None
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
        assert source.owner_worker_id == core.worker_id and source.borrower_token is None
        assert initial_source.state is ObjectState.READY_INLINE and initial_source.inline_data is not None
        original_rpc, original_retry = core._rpc, core._retry_system_failure

        def physical(node, object_id):
            physical_queries.append((node.node_id, object_id))
            assert len(physical_queries) <= 6
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
            if publication is not None and pending.task_id == publication.task_id:
                with core._completion:
                    record = core._recovery.task_record(pending.task_id)
                    outputs = tuple(core.owner_table.snapshot(value) for value in pending.output_ids)
                    child = core.owner_table.snapshot(source_id)
                    released = tuple(core.owner_table.contained_release_was_seen(source_id, hold)
                                     for transfer in transfers for hold in (transfer.final_hold, transfer.provisional_hold))
                    had_envelope = publication in getattr(core, "_output_result_custody", {})
                    with observation_lock:
                        key = pending.task_id, pending.spec.attempt_id
                        if key not in retry_cuts and len(retry_cuts) >= 2:
                            overflow.set()
                        else:
                            retry_cuts.setdefault(key, _RetryCut(
                                cleanup_acks.get(publication), record.current_attempt, record.retries_started,
                                outputs, child, pending.dependency_hold, released, had_envelope,
                            ))
            return original_retry(pending, error, **kwargs)

        core._rpc, core._retry_system_failure = inspect_rpc, inspect_retry
        blocker = _occupy_survivor.remote(address, deadline)
        listener.settimeout(_remaining(deadline))
        blocker_connection, _ = listener.accept()
        assert int.from_bytes(_recv_exact(blocker_connection, 8, deadline), "big") == survivor.worker_pid
        refs = tuple(_return_mixed_borrowed_child.remote([source]))
        assert len(refs) == 2
        output_ids = tuple(reference.object_id for reference in refs)
        task_id = output_ids[0].task_id
        assert tuple(value.return_index for value in output_ids) == (0, 1)
        listener.settimeout(_remaining(deadline))
        publication_connection, _ = listener.accept()
        publication_connection.settimeout(_remaining(deadline))
        arrival = recv_output_publication_gate_arrival(publication_connection)
        publication = arrival.publication_id
        assert arrival.phase is phase
        assert (arrival.node_id, arrival.node_pid, arrival.registration_epoch) == (
            victim.node_id, victim.node_pid, runtime.nodes[1].registration_epoch,
        )
        assert publication.task_id == task_id and publication.output_ids == publication.full_output_ids == output_ids
        assert publication.attempt_id == AttemptID(task_id, 0)
        _assert_metadata_only(arrival)
        before = _recovery(context, publication, deadline)
        manifest = before.manifest
        assert manifest.manifest_digest == arrival.manifest_digest
        assert manifest.header.owner_worker_id == core.worker_id and manifest.header.executor_worker_id == victim.worker_id
        assert before.armed and before.complete is None
        assert before.recovery_action is OutputRecoveryAction.COMPLETION_UNKNOWN
        assert before.adopted is before.rollback is before.owner_decision is before.resolution is None
        assert before.frozen_node_death is before.owner_death is None and before.slot_collections == ()
        slots = manifest.slots
        assert tuple(slot.object_id for slot in slots) == output_ids
        assert tuple(slot.tier for slot in slots) == (protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE)
        assert 0 < slots[0].size_bytes <= 1024 and len(_PADDING) < slots[1].size_bytes < 32 * 1024
        assert all(len(slot.transfers) == 1 for slot in slots)
        transfers = tuple(slot.transfers[0] for slot in slots)
        assert all(type(transfer.source) is BorrowedContainedSource for transfer in transfers)
        assert transfers[0].source == transfers[1].source
        borrowed = transfers[0].source
        assert borrowed.borrower_worker_id == victim.worker_id and type(borrowed.original_source) is protocol.TaskHoldSource
        hold = borrowed.original_source.hold
        assert hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert (hold.submitting_worker_id, hold.task_id, hold.origin_attempt_id) == (core.worker_id, task_id, publication.attempt_id)
        for index, transfer in enumerate(transfers):
            assert (transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.contained_owner_address) == (
                source_id, core.worker_id, runtime.owner_service.address,
            )
            assert transfer.final_hold.container_owner_worker_id == core.worker_id
            assert transfer.provisional_hold.container_owner_worker_id == victim.worker_id
            assert transfer.final_hold.container_object_id == transfer.provisional_hold.container_object_id == output_ids[index]
        old_holds = frozenset(hold for transfer in transfers for hold in (transfer.final_hold, transfer.provisional_hold))
        assert len(old_holds) == 4
        at_arm = core.owner_table.snapshot(source_id)
        assert at_arm.inline_data == initial_source.inline_data and at_arm.state is ObjectState.READY_INLINE
        assert at_arm.current_attempt == initial_source.current_attempt and at_arm.local_tokens == initial_source.local_tokens
        assert at_arm.borrowed_tokens == frozenset((borrowed.owner_table_token,))
        assert dict(at_arm.borrowed_sources)[borrowed.owner_table_token] == borrowed.original_source
        assert at_arm.submitted_tokens == frozenset((hold,)) and len(at_arm.lineage_tokens) == 1
        assert not at_arm.retained_tokens
        assert at_arm.contained_holds == frozenset(transfer.final_hold for transfer in transfers)
        for transfer in transfers:
            assert core.owner_table.contained_release_was_seen(source_id, transfer.provisional_hold)
            assert not core.owner_table.contained_release_was_seen(source_id, transfer.final_hold)
        assert _graph(context, publication, deadline).manifest == manifest.to_graph_manifest()
        assert not physical(victim, output_ids[0]).found  # INLINE bytes are not a physical replica.
        stored = physical(victim, output_ids[1])
        assert stored.found and stored.sealed and stored.owner_worker_id == core.worker_id
        assert stored.producer_attempt_id == publication.attempt_id and stored.size_bytes == slots[1].size_bytes
        assert stored.checksum == slots[1].checksum == hashlib.sha256(stored.data).hexdigest()
        for object_id in output_ids:
            pending = core.owner_table.snapshot(object_id)
            assert pending.state is ObjectState.PENDING and pending.current_attempt == publication.attempt_id
            assert pending.inline_data is pending.canonical_stored_result is pending.output_publication is None
            assert not pending.locations
        assert publication not in getattr(core, "_output_result_custody", {})

        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert (death.node_id, death.node_pid, death.registration_epoch) == (arrival.node_id, arrival.node_pid, arrival.registration_epoch)
        assert death.exit_code == -signal.SIGKILL and death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        publication_connection.close()
        publication_connection = None

        def observed_retry():
            with observation_lock:
                return retry_cuts.get((task_id, publication.attempt_id))

        cut = _poll_until(observed_retry, deadline, "mixed shared-child cleanup never reached SYSTEM retry")
        assert cut.current_attempt == publication.attempt_id and cut.retries_started == 0 and not cut.had_envelope
        assert tuple(value.object_id for value in cut.outputs) == output_ids
        assert all(value.state is ObjectState.PENDING and value.current_attempt == publication.attempt_id
                   and value.inline_data is None and value.output_publication is None and not value.locations for value in cut.outputs)
        resolution = cut.resolution
        assert resolution is not None and resolution.publication_id == publication
        assert resolution.manifest_digest == manifest.manifest_digest and resolution.owner_worker_id == core.worker_id
        assert resolution.node_death == death and resolution.complete is None and resolution.kept_slots == ()
        assert cut.release_seen == (True, True, True, True) and not cut.source.contained_holds
        assert cut.dependency_hold == hold and cut.source.submitted_tokens == at_arm.submitted_tokens
        assert cut.source.lineage_tokens == at_arm.lineage_tokens and cut.source.inline_data == at_arm.inline_data
        terminal = _node_loss(context, publication, core.worker_id, death, deadline)
        assert terminal.work.action is OutputRecoveryAction.COMPLETION_UNKNOWN
        assert terminal.work.snapshot == replace(before, frozen_node_death=death)
        resolved = terminal.snapshot
        assert resolved.resolution == resolution and resolved.armed and resolved.complete is None
        assert resolved.owner_decision.complete is None
        assert tuple((value.slot_index, value.object_id, value.decision) for value in resolved.owner_decision.slots) == tuple(
            (index, object_id, OutputRecoveryOwnerDecision.DROP) for index, object_id in enumerate(output_ids)
        )
        assert resolved.adopted is resolved.rollback is resolved.owner_death is resolved.owner_cleaned is None
        assert not resolved.terminal_report_allowed and _recovery(context, publication, deadline) == resolved

        def retry_admitted():
            record = core._recovery.task_record(task_id)
            return (record.current_attempt == publication.attempt_id.next() and record.retries_started == 1
                    and record.state is TaskState.RETRY_PENDING)

        _wait(core, retry_admitted, deadline)

        def dead_borrower_retired():
            child = core.owner_table.snapshot(source_id)
            installed = core.owner_table.dead_worker_record(victim.worker_id)
            return child if (installed is not None and borrowed.owner_table_token not in child.borrowed_tokens
                             and borrowed.owner_table_token in child.released_borrowed_tokens) else None

        retired_source = _wait(core, dead_borrower_retired, deadline)
        worker = _query(context.gcs_address, GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(victim.worker_id), deadline)
        assert type(worker) is protocol.GetWorkerStateReply and worker.found and worker.worker_id == victim.worker_id
        assert worker.state is protocol.WorkerMembershipState.DEAD and worker.death.reason is protocol.WorkerDeathReason.NODE_EXIT
        assert (worker.death.node_id, worker.death.node_pid, worker.death.node_registration_epoch, worker.death.worker_pid) == (
            victim.node_id, victim.node_pid, arrival.registration_epoch, victim.worker_pid,
        )
        assert core.owner_table.dead_worker_record(victim.worker_id).death_id == _worker_death_reference_id(worker.death)
        assert not core.owner_table.dead_worker_record(core.worker_id)
        assert retired_source.submitted_tokens == at_arm.submitted_tokens and retired_source.lineage_tokens == at_arm.lineage_tokens
        assert not retired_source.contained_holds and retired_source.local_tokens == initial_source.local_tokens
        with observation_lock:
            assert {attempt for task, attempt in grants if task == task_id} == {0}
            assert len(retry_cuts) == 1 and not conflicts and not overflow.is_set()
        blocker_connection.settimeout(_remaining(deadline))
        blocker_connection.sendall(_BLOCKER_RELEASE)
        assert ray.get(blocker, timeout=_remaining(deadline)) == survivor.worker_pid
        blocker_connection.close()
        blocker_connection = None

        values = ray.get(refs, timeout=_remaining(deadline))
        assert len(values) == 2
        for index, value in enumerate(values):
            child = value["child"]
            restored.append(child)
            assert value["slot"] == index and isinstance(child, ray.ObjectRef)
            assert child.object_id == source_id and child.owner_worker_id == core.worker_id and child.borrower_token is None
            assert ray.get(child, timeout=_remaining(deadline)) == _SOURCE_VALUE
        assert values[1]["padding"] == _PADDING and values[1]["executor_pid"] == survivor.worker_pid
        _wait(core, lambda: all(object_id not in core._task_finish_barriers for object_id in output_ids), deadline)
        completed = tuple(core.owner_table.snapshot(object_id) for object_id in output_ids)
        assert tuple(value.state for value in completed) == (ObjectState.READY_INLINE, ObjectState.READY_STORED)
        assert all(value.current_attempt == publication.attempt_id.next() for value in completed)
        members = tuple(value.output_publication for value in completed)
        assert all(member is not None for member in members)
        next_publication = members[0].publication_id
        assert next_publication != publication and next_publication.output_ids == next_publication.full_output_ids == output_ids
        assert all(member.publication_id == next_publication and member.manifest == members[0].manifest for member in members)
        assert tuple(member.slot_index for member in members) == (0, 1)
        assert members[0].manifest.header.executor_worker_id == survivor.worker_id
        assert members[0].manifest.header.node_incarnation.node_id == survivor.node_id
        new_transfers = tuple(member.slot.transfers[0] for member in members)
        assert all(type(transfer.source) is BorrowedContainedSource for transfer in new_transfers)
        assert new_transfers[0].source == new_transfers[1].source
        assert new_transfers[0].source.original_source == borrowed.original_source
        assert new_transfers[0].source.borrower_worker_id == survivor.worker_id
        new_holds = frozenset(hold for transfer in new_transfers for hold in (transfer.final_hold, transfer.provisional_hold))
        assert len(new_holds) == 4 and not old_holds.intersection(new_holds)
        for index, transfer in enumerate(new_transfers):
            assert transfer.contained_object_id == source_id and transfer.contained_owner_worker_id == core.worker_id
            assert transfer.final_hold.container_object_id == transfer.provisional_hold.container_object_id == output_ids[index]
        child_after = core.owner_table.snapshot(source_id)
        assert child_after.state is ObjectState.READY_INLINE and child_after.inline_data == initial_source.inline_data
        assert child_after.current_attempt == initial_source.current_attempt
        assert child_after.contained_holds == frozenset(transfer.final_hold for transfer in new_transfers)
        assert not child_after.submitted_tokens and not child_after.borrowed_tokens
        assert child_after.lineage_tokens == at_arm.lineage_tokens
        assert new_transfers[0].source.owner_table_token in child_after.released_borrowed_tokens
        assert all(core.owner_table.contained_release_was_seen(source_id, hold) for hold in old_holds)
        record = core._recovery.task_record(task_id)
        assert record.state is TaskState.SUCCEEDED and record.max_retries == record.retries_started == 1
        assert record.retries_remaining == 0 and record.current_attempt == publication.attempt_id.next()
        assert core._recovery.active_recovery(task_id) is None
        assert not physical(survivor, output_ids[0]).found
        stored_after_retry = physical(survivor, output_ids[1])
        assert stored_after_retry.found and stored_after_retry.sealed and stored_after_retry.owner_worker_id == core.worker_id
        assert stored_after_retry.producer_attempt_id == publication.attempt_id.next()
        assert stored_after_retry.size_bytes == members[1].slot.size_bytes == len(stored_after_retry.data)
        assert stored_after_retry.checksum == members[1].slot.checksum == hashlib.sha256(stored_after_retry.data).hexdigest()

        # Terminal old progress is a real idempotent RPC, not test-side cleanup.
        old_progress = wire.ProgressOutputNodeLoss(terminal.work)
        replay = _query(context.gcs_address, wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER, old_progress, deadline)
        assert type(replay) is wire.OutputNodeLossReply and replay.request == old_progress and replay.snapshot == resolved
        assert tuple(core.owner_table.snapshot(object_id) for object_id in output_ids) == completed
        assert core.owner_table.snapshot(source_id) == child_after
        assert _recovery(context, publication, deadline) == resolved
        successful = _recovery(context, next_publication, deadline)
        assert successful.complete is not None and successful.adopted is not None and not successful.slot_collections
        with observation_lock:
            task_grants = {attempt: pair for (task, attempt), pair in grants.items() if task == task_id}
            assert set(task_grants) == {0, 1} and len(retry_cuts) == 1 and not conflicts and not overflow.is_set()
        first_request, first_grant = task_grants[0]
        next_request, next_grant = task_grants[1]
        assert first_request.return_ids == next_request.return_ids == output_ids
        assert first_request.lease_id != next_request.lease_id
        assert (first_grant.node_id, next_grant.node_id) == (victim.node_id, survivor.node_id)

        for reference in restored:
            _close_local(reference, deadline)
        _close_local(refs[0], deadline)
        _wait(core, lambda: core.owner_table.collection_state(output_ids[0]) is ObjectCollectionState.COLLECTED, deadline)
        assert core.owner_table.snapshot(output_ids[1]) == completed[1]
        between = core.owner_table.snapshot(source_id)
        assert between.contained_holds == frozenset((new_transfers[1].final_hold,))
        assert between.lineage_tokens == at_arm.lineage_tokens and between.local_tokens == initial_source.local_tokens
        assert core.owner_table.contained_release_was_seen(source_id, new_transfers[0].final_hold)
        assert not core.owner_table.contained_release_was_seen(source_id, new_transfers[1].final_hold)
        assert physical(survivor, output_ids[1]) == stored_after_retry
        first_collected = _recovery(context, next_publication, deadline)
        assert tuple(proof.slot_index for proof in first_collected.slot_collections) == (0,)
        assert first_collected.slot_collections[0].object_id == output_ids[0]
        assert first_collected.complete == successful.complete and first_collected.adopted == successful.adopted
        assert ray.get(source, timeout=_remaining(deadline)) == _SOURCE_VALUE

        _close_local(refs[1], deadline)
        _wait(core, lambda: core.owner_table.collection_state(output_ids[1]) is ObjectCollectionState.COLLECTED, deadline)
        assert not physical(survivor, output_ids[1]).found and len(physical_queries) == 6
        after_siblings = core.owner_table.snapshot(source_id)
        assert not after_siblings.contained_holds and not after_siblings.lineage_tokens
        assert not after_siblings.submitted_tokens and not after_siblings.borrowed_tokens and not after_siblings.retained_tokens
        assert after_siblings.local_tokens == initial_source.local_tokens and after_siblings.inline_data == initial_source.inline_data
        assert all(core.owner_table.contained_release_was_seen(source_id, hold) for hold in new_holds)
        both_collected = _recovery(context, next_publication, deadline)
        assert tuple((proof.slot_index, proof.object_id) for proof in both_collected.slot_collections) == tuple(enumerate(output_ids))
        assert both_collected.slot_collections[0] == first_collected.slot_collections[0]
        assert _recovery(context, publication, deadline) == resolved
        _close_local(blocker, deadline)
        _close_local(source, deadline)
        _wait(core, lambda: all(core.owner_table.collection_state(reference.object_id) is ObjectCollectionState.COLLECTED
                               for reference in (blocker, source)), deadline)
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations

        def survivor_clean():
            status = _query(survivor.node_address, SHUTDOWN_STATUS_HANDLER,
                            protocol.ShutdownStatusRequest("inspect-mixed-borrowed-unknown"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested
            return status if status.resources_clean else None

        assert _poll_until(survivor_clean, deadline, "mixed-output survivor cleanup did not settle").child_pids == (survivor.worker_pid,)
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
                "managed process survived mixed-output unknown shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
