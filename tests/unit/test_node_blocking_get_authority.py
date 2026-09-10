"""Node-authoritative blocking-get CPU transitions with explicit test scope.

The six unit-marked contracts call real handlers and the resource ledger on
an unstarted Node; they create no listener, process, or background thread.
The shared synchronous fixture prepares one tiny ref-free stored output through
the real journal/adapter and an in-memory owner handoff before Complete.
A directly allocated ``child`` token isolates accounting, not Worker scheduling.
The final L1 contract races two real handlers behind a three-party barrier:
waits are at most one second, normal joins share two seconds, and failure
cleanup shares one second. It settles the actual terminal outbox but retains
the result slot and the 13-byte stored replica pending owner adoption. It
does not prove owner adoption, physical GC, or a running Worker/GCS process.
"""

from __future__ import annotations

from dataclasses import replace
import socket
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol
from miniray.ids import WorkerID
from miniray.node import NodeServer, _LeaseOutcome
from miniray.resources import (
    AllocationState,
    AllocationToken,
    NodeSnapshot,
    ResourceLedger,
    ResourceVector,
)
from tests.unit.test_output_publication_node_server import _node as _publication_node


@pytest.fixture(autouse=True)
def _unit_has_no_runtime(request, monkeypatch):
    if (request.node.get_closest_marker("heavy") is not None
            or request.node.get_closest_marker("loopback_smoke") is not None):
        return

    def forbidden(*_args, **_kwargs):
        pytest.fail("pure blocking-get authority attempted runtime work")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _running_lease(
    requested: ResourceVector, total: ResourceVector | None = None
) -> tuple[
    NodeServer, protocol.RequestWorkerLease, protocol.GrantWorkerLease
]:
    fixture, node, record, _completion = _publication_node(refs=False)
    capacity = total or requested
    # Keep the real publication/lease identity while using this contract's
    # exact CPU/GPU/custom allocation, including zero-CPU and spare-capacity cases.
    fixture.ledger = node._ledger = ResourceLedger(capacity)
    node._ledger.allocate(requested, record.allocation_token)
    node._cluster_nodes = (
        NodeSnapshot(node.node_id, capacity, node._ledger.available),
    )
    request = replace(
        record.request, resources=requested, target_node_id=node.node_id,
    )
    record.request = request
    grant = record.grant
    assert node.worker_id == grant.worker_id
    node._workers[node.worker_id].active_lease_id = request.lease_id
    node._lease_outcomes = {request.lease_id: _LeaseOutcome(request, grant)}
    baseline = node._ledger.snapshot()
    prepared = node._handle_prepare_output_publication(
        wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))
    )
    assert prepared.accepted
    assert record.output_publication_id == fixture.id
    assert fixture.journal.snapshot(fixture.id).ready_to_complete
    assert fixture.handoffs.query(fixture.id).manifest == fixture.manifest
    assert fixture.handoffs.query(fixture.id).complete is None
    assert node._ledger.snapshot() == baseline
    return node, request, grant


def _blocked(
    request: protocol.RequestWorkerLease, worker_id: WorkerID, sequence: int
) -> protocol.NotifyWorkerBlocked:
    return protocol.NotifyWorkerBlocked(
        request.lease_id, request.task_id, request.attempt_id, worker_id, sequence
    )


def _unblocked(
    request: protocol.RequestWorkerLease, worker_id: WorkerID, sequence: int
) -> protocol.NotifyWorkerUnblocked:
    return protocol.NotifyWorkerUnblocked(
        request.lease_id, request.task_id, request.attempt_id, worker_id, sequence
    )


def _complete(
    request: protocol.RequestWorkerLease, worker_id: WorkerID
) -> protocol.CompleteWorkerLease:
    return protocol.CompleteWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, worker_id,
        protocol.TaskReplyStatus.SUCCEEDED,
    )


@pytest.mark.unit
def test_node_yields_only_cpu_and_replays_one_episode_idempotently() -> None:
    total = ResourceVector({
        "CPU": 2, "GPU": 1, "memory": 4, "accelerator": 1,
    })
    requested = ResourceVector({
        "CPU": 1, "GPU": 1, "memory": 2, "accelerator": 1,
    })
    node, request, grant = _running_lease(requested, total)
    message = _blocked(request, grant.worker_id, 0)

    first = node._handle_notify_worker_blocked(message)
    replay = node._handle_notify_worker_blocked(message)

    assert first.accepted and first.changed
    assert replay.accepted and not replay.changed
    assert node.resource_ledger.available == ResourceVector(
        {"CPU": 2, "memory": 2}
    )
    allocation = node.resource_ledger.record(grant.allocation_token)
    assert allocation is not None
    assert allocation.state is AllocationState.CPU_YIELDED
    assert allocation.held_resources == ResourceVector(
        {"GPU": 1, "memory": 2, "accelerator": 1}
    )
    record = node._leases[request.lease_id]
    assert record.blocking_sequence == 0
    assert record.blocking_open and record.cpu_yielded


@pytest.mark.unit
def test_unblock_is_immediate_with_debt_and_completion_reconciles_once() -> None:
    resources = ResourceVector({"CPU": 1, "GPU": 1})
    node, request, grant = _running_lease(resources)
    blocked = node._handle_notify_worker_blocked(
        _blocked(request, grant.worker_id, 0)
    )
    assert blocked.accepted and blocked.changed

    # Future Worker-pool work will acquire this token through another lease.
    # Here it isolates the authority/accounting invariant without a process.
    child = node.resource_ledger.allocate(
        ResourceVector({"CPU": 1}), AllocationToken("child-allocation")
    )
    unblocked_message = _unblocked(request, grant.worker_id, 0)
    first = node._handle_notify_worker_unblocked(unblocked_message)
    replay = node._handle_notify_worker_unblocked(unblocked_message)

    assert first.accepted and first.changed
    assert replay.accepted and not replay.changed
    assert node.resource_ledger.cpu_debt == 1
    assert node.resource_ledger.available == ResourceVector.empty()
    assert not node.resource_ledger.can_allocate(ResourceVector({"CPU": 0.001}))

    completed = node._handle_complete_worker_lease(
        _complete(request, grant.worker_id)
    )
    assert completed.accepted and completed.released
    assert node.resource_ledger.cpu_debt == 0
    assert node.resource_ledger.available == ResourceVector({"GPU": 1})
    assert node.resource_ledger.release(child)
    assert node.resource_ledger.available == resources
    assert not node._leases[request.lease_id].blocking_open


@pytest.mark.unit
def test_unblock_before_block_tombstones_ambiguous_episode() -> None:
    resources = ResourceVector({"CPU": 1})
    node, request, grant = _running_lease(resources)
    unblocked = _unblocked(request, grant.worker_id, 0)

    close_first = node._handle_notify_worker_unblocked(unblocked)
    close_replay = node._handle_notify_worker_unblocked(unblocked)
    late_block = node._handle_notify_worker_blocked(
        _blocked(request, grant.worker_id, 0)
    )

    assert close_first.accepted and close_first.changed
    assert close_replay.accepted and not close_replay.changed
    assert not late_block.accepted and not late_block.changed
    assert "already closed" in late_block.error
    assert node.resource_ledger.available == ResourceVector.empty()

    next_block = node._handle_notify_worker_blocked(
        _blocked(request, grant.worker_id, 1)
    )
    assert next_block.accepted and next_block.changed
    assert node.resource_ledger.available == resources


@pytest.mark.unit
def test_sequence_and_execution_identity_fence_every_resource_mutation() -> None:
    resources = ResourceVector({"CPU": 1})
    node, request, grant = _running_lease(resources)

    skipped = node._handle_notify_worker_blocked(
        _blocked(request, grant.worker_id, 1)
    )
    wrong_worker = node._handle_notify_worker_blocked(
        _blocked(request, WorkerID.random(), 0)
    )
    wrong_attempt = protocol.NotifyWorkerBlocked(
        request.lease_id, request.task_id, request.attempt_id.next(),
        grant.worker_id, 0,
    )
    wrong_attempt_reply = node._handle_notify_worker_blocked(wrong_attempt)

    assert not skipped.accepted and "skipped" in skipped.error
    assert not wrong_worker.accepted and "identity" in wrong_worker.error
    assert not wrong_attempt_reply.accepted
    assert node.resource_ledger.available == ResourceVector.empty()

    assert node._handle_notify_worker_blocked(
        _blocked(request, grant.worker_id, 0)
    ).accepted
    assert node._handle_notify_worker_unblocked(
        _unblocked(request, grant.worker_id, 0)
    ).accepted
    stale = node._handle_notify_worker_unblocked(
        _unblocked(request, grant.worker_id, 0)
    )
    # Exact unblocked replay is accepted; after episode 1 opens, sequence 0 is
    # stale rather than being confused with another replay.
    assert stale.accepted and not stale.changed
    assert node._handle_notify_worker_blocked(
        _blocked(request, grant.worker_id, 1)
    ).accepted
    truly_stale = node._handle_notify_worker_unblocked(
        _unblocked(request, grant.worker_id, 0)
    )
    assert not truly_stale.accepted and "stale" in truly_stale.error


@pytest.mark.unit
def test_zero_cpu_lease_tracks_episode_without_changing_resources() -> None:
    requested = ResourceVector({"GPU": 1})
    total = ResourceVector({"CPU": 1, "GPU": 1})
    node, request, grant = _running_lease(requested, total)
    baseline = node.resource_ledger.available

    blocked = node._handle_notify_worker_blocked(
        _blocked(request, grant.worker_id, 0)
    )
    unblocked = node._handle_notify_worker_unblocked(
        _unblocked(request, grant.worker_id, 0)
    )

    assert blocked.accepted and blocked.changed
    assert unblocked.accepted and unblocked.changed
    assert node.resource_ledger.available == baseline
    assert node.resource_ledger.cpu_debt == 0


@pytest.mark.unit
def test_completion_and_worker_loss_clean_yielded_allocations() -> None:
    resources = ResourceVector({"CPU": 1, "GPU": 1})

    completed_node, completed_request, completed_grant = _running_lease(resources)
    assert completed_node._handle_notify_worker_blocked(
        _blocked(completed_request, completed_grant.worker_id, 0)
    ).accepted
    completion = completed_node._handle_complete_worker_lease(
        _complete(completed_request, completed_grant.worker_id)
    )
    late_unblock = completed_node._handle_notify_worker_unblocked(
        _unblocked(completed_request, completed_grant.worker_id, 0)
    )
    assert completion.accepted and completion.released
    assert not late_unblock.accepted and not late_unblock.changed
    assert completed_node.resource_ledger.available == resources

    lost_node, lost_request, lost_grant = _running_lease(resources)
    assert lost_node._handle_notify_worker_blocked(
        _blocked(lost_request, lost_grant.worker_id, 0)
    ).accepted
    with lost_node._state_lock:
        assert lost_node._reclaim_active_lease_after_worker_exit_locked()
        assert not lost_node._reclaim_active_lease_after_worker_exit_locked()
    lost_record = lost_node._leases[lost_request.lease_id]
    assert lost_record.state is protocol.LeaseExecutionState.WORKER_LOST
    assert not lost_record.blocking_open and not lost_record.cpu_yielded
    assert lost_node.resource_ledger.available == resources


@pytest.mark.loopback_smoke
def test_concurrent_block_and_completion_linearize_without_leaking(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One genuine two-handler race; no claim to observe both lock orders.

    The barrier releases both callers to the unchanged production handlers.
    Neither a test lock nor a synthetic CPU transition serializes their work.
    Terminal-report ACK is checked separately from owner adoption and GC.
    """
    import multiprocessing.process
    import queue
    import subprocess

    from miniray import transport
    from miniray.output_publication_journal import OutputPublicationJournalState

    resources = ResourceVector({"CPU": 1, "GPU": 1})
    barrier = threading.Barrier(3, timeout=1.0)
    outcomes = queue.Queue(maxsize=2)
    errors = queue.Queue(maxsize=16)
    terminal_acks = queue.Queue(maxsize=1)
    error_overflow = False
    baseline = set(threading.enumerate())
    threads = ()
    attempted_starts = []
    real_start = threading.Thread.start

    def retain_error(error):
        nonlocal error_overflow
        try:
            errors.put_nowait(error)
        except queue.Full:
            error_overflow = True

    def forbidden(*_args, **_kwargs):
        error = AssertionError("blocking-get L1 attempted runtime infrastructure")
        # Node observation/drain paths may catch exceptions. Preserve the
        # failure before raising so a returned success cannot hide it.
        retain_error(error)
        raise error

    def start(thread):
        if (len(threads) != 2 or not any(thread is item for item in threads)
                or any(thread is item for item in attempted_starts)):
            forbidden()
        attempted_starts.append(thread)
        return real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    monkeypatch.setattr(threading.Timer, "__init__", forbidden)
    monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", forbidden)
    monkeypatch.setattr(multiprocessing.process.BaseProcess, "join", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(transport.TCPServer, "__init__", forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)

    def block() -> None:
        try:
            barrier.wait(timeout=1.0)
            reply = node._handle_notify_worker_blocked(block_message)
            outcomes.put_nowait((threading.current_thread(), reply))
        except BaseException as exc:
            retain_error(exc)

    def complete() -> None:
        try:
            barrier.wait(timeout=1.0)
            reply = node._handle_complete_worker_lease(complete_message)
            outcomes.put_nowait((threading.current_thread(), reply))
        except BaseException as exc:
            retain_error(exc)

    try:
        node, request, grant = _running_lease(resources)
        record = node._leases[request.lease_id]
        identity = record.output_publication_id
        journal, adapter = node._output_publication_journal, node._output_publications
        state_lock, journal_lock = node._state_lock, journal._lock
        ledger = node.resource_ledger
        block_message = _blocked(request, grant.worker_id, 0)
        complete_message = _complete(request, grant.worker_id)
        original_terminal = adapter._report_complete

        def report_terminal(witness):
            try:
                # The original callback validates the actual owner-handoff
                # reply before returning; inspect that authority's saved fact.
                original_terminal(witness)
                owner_handoff = original_terminal.__self__.handoffs.query(identity)
                assert owner_handoff.complete == witness
                terminal_acks.put_nowait(owner_handoff)
            except BaseException as exc:
                retain_error(exc)
                raise

        monkeypatch.setattr(adapter, "_report_complete", report_terminal)
        monkeypatch.setattr(node, "_background_rpc", forbidden)
        threads = (
            threading.Thread(target=block, name="miniray-test-block-complete-block", daemon=True),
            threading.Thread(target=complete, name="miniray-test-block-complete-complete", daemon=True),
        )
        for thread in threads:
            thread.start()
        barrier.wait(timeout=1.0)
        deadline = time.monotonic() + 2.0
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        assert not any(thread.is_alive() for thread in threads)
        assert errors.empty() and not error_overflow, tuple(errors.queue)
        observed = tuple(outcomes.get_nowait() for _ in range(2))
        for _ in observed:
            outcomes.task_done()
        assert outcomes.empty() and outcomes.unfinished_tasks == 0
        assert {thread for thread, _ in observed} == set(threads)
        replies = tuple(reply for _, reply in observed)

        assert record.state is protocol.LeaseExecutionState.COMPLETED
        assert not record.blocking_open and not record.cpu_yielded
        assert node.resource_ledger.available == resources
        assert node.resource_ledger.cpu_debt == 0
        completion = next(
            reply for reply in replies
            if isinstance(reply, protocol.CompleteWorkerLeaseReply)
        )
        blocked = next(
            reply for reply in replies
            if isinstance(reply, protocol.NotifyWorkerBlockedReply)
        )
        assert completion.accepted and completion.released
        assert blocked.accepted != (blocked.state is protocol.LeaseExecutionState.COMPLETED)
        if blocked.accepted:
            assert blocked.changed and blocked.state is protocol.LeaseExecutionState.RUNNING
            assert record.blocking_sequence == 0
        else:
            assert not blocked.changed and blocked.state is protocol.LeaseExecutionState.COMPLETED
            assert record.blocking_sequence == -1
        for reply in replies:
            assert (reply.lease_id, reply.task_id, reply.attempt_id, reply.worker_id) == (
                request.lease_id, request.task_id, request.attempt_id, grant.worker_id,
            )
        assert record.completion == complete_message and record.output_complete_inflight is None
        assert node._state_lock is state_lock and journal._lock is journal_lock
        assert node.resource_ledger is ledger
        allocation = ledger.record(grant.allocation_token)
        assert allocation is not None and allocation.state is AllocationState.RELEASED
        assert allocation.held_resources == ResourceVector.empty()
        assert ledger.snapshot().allocations == (allocation,)
        assert node._workers[grant.worker_id].active_lease_id is None
        assert node._workers[node.worker_id].active_lease_id is None

        envelope = completion.output_publication
        assert envelope is not None and envelope.publication_id == identity
        witness = envelope.complete
        assert journal.snapshot(identity).complete == witness
        assert adapter.pending_terminal_reports() == (witness,)
        assert not adapter.pending_lease_completions() and not adapter.pending_rollbacks()
        assert not adapter._tickets and terminal_acks.empty()
        assert node._drive_output_publications()
        acknowledgement = terminal_acks.get_nowait()
        terminal_acks.task_done()
        assert acknowledgement.manifest == envelope.manifest
        assert acknowledgement.complete == witness
        assert acknowledgement.adoption is None
        assert not adapter.pending_terminal_reports()

        released_ledger = ledger.snapshot()
        replay = node._handle_complete_worker_lease(complete_message)
        assert replay.accepted and not replay.released and replay.output_publication == envelope
        late_block = node._handle_notify_worker_blocked(block_message)
        assert not late_block.accepted and not late_block.changed
        assert late_block.state is protocol.LeaseExecutionState.COMPLETED
        outcome = node._handle_get_worker_lease_outcome(protocol.GetWorkerLeaseOutcome(
            request.lease_id, request.task_id, request.attempt_id, grant.worker_id,
            request.requester_worker_id, request.return_ids,
        ))
        assert outcome.found and outcome.state is protocol.LeaseExecutionState.COMPLETED
        assert outcome.completion_status is protocol.TaskReplyStatus.SUCCEEDED
        assert not outcome.cleanup_pending and outcome.output_publication == envelope
        assert ledger.snapshot() == released_ledger
        assert not adapter.pending_terminal_reports() and not adapter.pending_lease_completions()
        assert not adapter.pending_rollbacks() and not adapter._tickets
        assert terminal_acks.empty() and terminal_acks.unfinished_tasks == 0

        # This fixture has no owner adoption. Complete/report ACK preserves
        # delivery custody; it cannot authorize retirement or physical GC.
        snapshot = journal.snapshot(identity)
        assert snapshot.state is OutputPublicationJournalState.COMPLETED
        assert snapshot.retained_result_slots == (0,) and snapshot.retired_slots == ()
        assert snapshot.rollback is None and snapshot.rollback_tombstone is None
        stored = (envelope.result)
        assert stored.inline_data is None
        assert node.object_store.capacity_bytes == 1024 and node.object_store.used_bytes == 13
        assert node.object_store.get(stored.object_id) == b"stored-result"
        assert node.object_store.snapshot(stored.object_id).pin_count == 0
        assert not node._local_replica_write_claims and not node._dropped_metadata
        assert errors.empty() and not error_overflow, tuple(errors.queue)
    finally:
        # Never reenter a possibly held Node/journal lock after thread failure.
        # Abort only our barrier, join every actually started owned thread, and
        # leave all publication/replica authority intact for failure diagnosis.
        barrier.abort()
        deadline = time.monotonic() + 1.0
        for thread in threads:
            if thread.ident is not None:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
        assert not any(thread.is_alive() for thread in threads)
        assert not (set(threading.enumerate()) - baseline)
        assert errors.empty() and not error_overflow, tuple(errors.queue)
