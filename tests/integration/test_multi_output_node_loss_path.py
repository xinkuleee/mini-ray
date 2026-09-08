"""Bounded mixed contained-output loss with real owner payload custody.

Two Nodes/one CPU and Worker each (five children/six endpoints), one Driver
put, one two-return Task/two physical attempts, 1 MiB per store and 8 KiB
padding. Both output slots contain the same live Driver-owned child. The
Worker's real nested import uses its embedded Core, not an extra OS process.

The only fault crashes the exact publisher at the original first graph-COMMIT
boundary, after real Complete delivery/Terminal ACK and before owner CAS.
Neither replies nor state are fabricated. Passive bounded observations show
KEEP for INLINE custody, DROP for the unavailable STORED slot, and the later
explicit-get targeted reconstruction with an unchanged healthy sibling.

Gets, crash-barrier waits and at most 256 predicate waits per observation
share a 15s budget. Inner recovery RPCs retain their own finite timeouts, not
distributed cancellation. Five handles and actual staged GC share one final
3s epoch, reused by failure cleanup/PID observation. Shutdown retains its own
protocol budget under the external 30s runner. Failure finally always checks
known PIDs/endpoints; survivor-ledger assertions certify the successful path.
Run only this exact ID through scripts/run_bounded_test.py.
"""

from dataclasses import replace
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
from miniray.ids import AttemptID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope, OutputPublicationID,
)
from miniray.output_recovery import OutputRecoveryOwnerDecision
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_sources import BorrowedContainedSource
from miniray.recovery import TaskState, UnknownTaskError
from miniray.task_outputs import TargetExecutionKey, TaskExecutionKey


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_POLLS = 256
_MAX_PUSHES = 8
_MAX_CONTROLS = 64
_CHILD_VALUE = ("living-driver-child", 42)


@ray.remote(num_returns=2, max_retries=1)
def _same_child_two_tiers(container):
    return {"child": container[0]}, {"child": container[0], "data": b"N" * 8192}


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("mixed Node-loss observation exceeded its deadline")
    return remaining


def _wait_current(core, predicate, deadline):
    with core._completion:
        for _ in range(_MAX_POLLS):
            if predicate():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            core._completion.wait(min(0.05, remaining))
        assert predicate(), "mixed Node-loss owner transition did not converge"


def _close_current(reference, deadline):
    done = reference._release_done
    assert reference.borrower_token is None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_received_mixed_result_keeps_inline_and_reconstructs_only_lost_stored(monkeypatch):
    context = report = core = source = death = None
    original_rpc = original_push = cleanup_deadline = None
    refs = ()
    handles, restored, close_errors = [], [], []
    pids, addresses = set(), set()
    controls, pushes, cuts, hook_errors = [], [], [], []
    observation_lock = threading.Lock()
    observation_failed = overflow = crash_claimed = False

    def record(destination, item, cap):
        nonlocal overflow, observation_failed
        try:
            with observation_lock:
                if len(destination) < cap:
                    destination.append(item)
                else:
                    overflow = True
        except Exception:
            observation_failed = True

    def observe_push(address, handler, request):
        reply = original_push(address, handler, request)
        record(pushes, (address, handler, request, reply), _MAX_PUSHES)
        return reply

    def crash_before_commit(address, handler, request):
        nonlocal death, crash_claimed
        should_crash = False
        if (handler == "commit_contained_graph"
                and type(request) is protocol.CommitContainedGraph
                and type(request.manifest.publication_id) is OutputPublicationID):
            with observation_lock:
                if not crash_claimed:
                    crash_claimed = should_crash = True
        if should_crash:
            try:
                identity = request.manifest.publication_id
                with core._state_lock:
                    envelope = core._output_result_custody[identity]
                    owned = tuple(core.owner_table.snapshot(value) for value in identity.output_ids)
                    child = core.owner_table.snapshot(source.object_id)
                # Scope-check before signaling. Never hold either test or Core
                # locks while the real death barrier notifies this same Core.
                victim = context.nodes[0]
                header = envelope.manifest.header
                if (header.node_incarnation.node_id != victim.node_id
                        or header.node_incarnation.node_pid != victim.node_pid
                        or header.executor_worker_id != victim.worker_id):
                    raise RuntimeError("graph-COMMIT did not name the original publisher")
                record(cuts, (request, envelope, owned, child), 1)
                death = _test_crash_node(victim.node_id, timeout=min(10.0, _remaining(deadline)))
            except Exception as exc:
                # Keep callback failures visible without manufacturing an RPC
                # error/ACK loss in addition to the single Node crash.
                record(hook_errors, exc, 1)
        reply = original_rpc(address, handler, request)
        if handler in (
            wire.REPORT_OUTPUT_PUBLICATION_HANDLER, wire.GET_OUTPUT_NODE_LOSS_HANDLER,
            wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER, wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER,
            "commit_contained_graph",
        ):
            record(controls, (address, handler, request, reply), _MAX_CONTROLS)
        return reply

    def finished():
        return (all(ref.object_id not in core._task_finish_barriers for ref in refs)
                and not core._protocol_unresolved and core._accepted_task_count == 0)

    try:
        context = ray.init(num_nodes=2, num_cpus=1, num_workers_per_node=1,
                           inline_threshold=1024, object_store_bytes=1024 * 1024,
                           enable_tracing=False)
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        victim, survivor = context.nodes
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        assert runtime.owner_service is not None
        addresses.add(runtime.owner_service.address)
        assert len(pids) == 5 and len(addresses) == 6 and os.getpid() not in pids
        assert context.trace_address is None
        source = ray.put(_CHILD_VALUE)
        handles.append(source)
        original_rpc, original_push = core._rpc, core._push_task_rpc
        monkeypatch.setattr(core, "_rpc", crash_before_commit)
        monkeypatch.setattr(core, "_push_task_rpc", observe_push)
        submitted = _same_child_two_tiers.remote([source])
        for reference in submitted if isinstance(submitted, tuple) else (submitted,):
            if isinstance(reference, ray.ObjectRef):
                handles.append(reference)
        assert isinstance(submitted, tuple) and len(submitted) == 2
        assert all(isinstance(ref, ray.ObjectRef) for ref in submitted)
        refs = submitted
        output_ids = tuple(ref.object_id for ref in refs)
        task_id = output_ids[0].task_id
        initial_attempt = AttemptID(task_id, 0)
        assert tuple(value.task_id for value in output_ids) == (task_id, task_id)
        assert tuple(value.return_index for value in output_ids) == (0, 1)

        first = ray.get(refs[0], timeout=_remaining(deadline))
        first_child = first.get("child") if isinstance(first, dict) else None
        if isinstance(first_child, ray.ObjectRef):
            handles.append(first_child)
            restored.append(first_child)
        assert isinstance(first_child, ray.ObjectRef)
        assert ray.get(first_child, timeout=_remaining(deadline)) == _CHILD_VALUE
        _wait_current(core, finished, deadline)
        with observation_lock:
            assert not hook_errors and not overflow and not observation_failed
            assert crash_claimed and len(cuts) == 1
            (cut_request, envelope, pending_at_cut, child_at_cut), = cuts
            loss_controls = tuple(controls)
        assert death is not None and core._dead_nodes[victim.node_id] == death
        assert death.node_pid == victim.node_pid and death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        assert death.registration_epoch == envelope.manifest.header.node_incarnation.registration_epoch
        publication = envelope.publication_id
        assert cut_request.manifest == envelope.manifest.to_graph_manifest()
        assert publication.full_output_ids == publication.output_ids == output_ids
        assert publication.attempt_id == initial_attempt
        assert type(publication.execution) is TaskExecutionKey
        assert tuple(slot.tier for slot in envelope.manifest.slots) == (
            protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE,
        )
        assert all(value.state is ObjectState.PENDING and value.output_publication is None
                   and value.current_attempt == initial_attempt for value in pending_at_cut)
        transfers = tuple(slot.transfers[0] for slot in envelope.manifest.slots)
        assert all(len(slot.transfers) == 1 for slot in envelope.manifest.slots)
        assert child_at_cut.contained_holds == frozenset(value.final_hold for value in transfers)
        assert transfers[0].final_hold != transfers[1].final_hold
        terminals = [reply for _, _, request, reply in loss_controls
                     if type(request) is wire.ReportOutputPublicationTerminal
                     and request.witness == envelope.complete]
        assert terminals and all(type(reply) is wire.OutputRecoveryReply and reply.accepted
                                 and reply.ack.snapshot.complete == envelope.complete for reply in terminals)
        resolutions = [reply.snapshot for _, _, request, reply in loss_controls
                       if (type(reply) is wire.OutputNodeLossReply
                           or type(reply) is wire.GetOutputNodeLossReply and reply.found)
                       and reply.snapshot.publication_id == publication
                       and reply.snapshot.resolution is not None]
        assert resolutions
        resolved = resolutions[-1]
        assert resolved.frozen_node_death == death and resolved.owner_death is None
        assert resolved.complete == resolved.resolution.complete == envelope.complete
        assert resolved.resolution.kept_slots == (0,) and resolved.rollback is None
        assert tuple(value.decision for value in resolved.owner_decision.slots) == (
            OutputRecoveryOwnerDecision.KEEP, OutputRecoveryOwnerDecision.DROP,
        )

        inline, lost = (core.owner_table.snapshot(value) for value in output_ids)
        assert inline.state is ObjectState.READY_INLINE and inline.current_attempt == initial_attempt
        assert inline.inline_data == envelope.results[0].inline_data
        assert inline.output_publication.manifest == envelope.manifest
        assert lost.state is ObjectState.LOST and lost.current_attempt == initial_attempt
        assert lost.output_publication is None and not lost.locations
        child_after_loss = core.owner_table.snapshot(source.object_id)
        assert child_after_loss.contained_holds == frozenset({transfers[0].final_hold})
        assert core.owner_table.contained_release_was_seen(source.object_id, transfers[1].final_hold)
        recovery = core._recovery.task_record(task_id)
        assert recovery.state is TaskState.SUCCEEDED and recovery.current_attempt == initial_attempt
        assert recovery.retries_started == 0 and core._recovery.active_recovery(task_id) is None

        second = ray.get(refs[1], timeout=_remaining(deadline))
        second_child = second.get("child") if isinstance(second, dict) else None
        if isinstance(second_child, ray.ObjectRef):
            handles.append(second_child)
            restored.append(second_child)
        assert isinstance(second_child, ray.ObjectRef)
        assert second["data"] == b"N" * 8192
        assert ray.get(second_child, timeout=_remaining(deadline)) == _CHILD_VALUE
        _wait_current(core, finished, deadline)
        assert core.owner_table.snapshot(output_ids[0]) == inline
        rebuilt = core.owner_table.snapshot(output_ids[1])
        assert rebuilt.state is ObjectState.READY_STORED
        assert rebuilt.current_attempt == initial_attempt.next()
        assert rebuilt.locations == frozenset({survivor.node_id})
        replacement = rebuilt.output_publication.slot.transfers[0]
        assert replacement.final_hold != transfers[1].final_hold
        assert core.owner_table.snapshot(source.object_id).contained_holds == frozenset({
            transfers[0].final_hold, replacement.final_hold,
        })
        assert core.owner_table.contained_release_was_seen(source.object_id, transfers[1].final_hold)
        assert all(ref.object_id == source.object_id and ref.owner_worker_id == core.worker_id
                   and ref.borrower_token is None for ref in restored)
        assert len({ref._local_token for ref in (source, *restored)}) == 3
        with observation_lock:
            exchanges = tuple(pushes)
            assert not overflow and not observation_failed and not hook_errors
        by_attempt = {}
        for address, handler, push, reply in exchanges:
            assert handler == "push_task" and type(push) is protocol.PushTask and type(reply) is protocol.TaskReply
            reply = replace(reply)
            assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
            assert reply.task_id == push.spec.task_id == task_id
            assert reply.attempt_id == push.spec.attempt_id
            node = victim if reply.attempt_id == initial_attempt else survivor
            assert address == node.worker_address and reply.worker_id == push.worker_id == node.worker_id
            assert push.spec.return_ids() == output_ids and push.dependencies == ()
            actual = reply.output_publication
            assert type(actual) is OutputPublicationEnvelope
            assert actual.publication_id.lease_id == push.lease_id
            assert actual.results == reply.results and actual.complete == OutputPublicationCompleteWitness.for_manifest(actual.manifest)
            assert actual.manifest.header.node_incarnation.node_id == node.node_id
            assert actual.manifest.header.owner_worker_id == core.worker_id
            assert actual.manifest.header.executor_worker_id == node.worker_id
            assert by_attempt.setdefault(reply.attempt_id, (push, reply)) == (push, reply)
            for slot in actual.manifest.slots:
                assert len(slot.transfers) == 1
                transfer = slot.transfers[0]
                assert transfer.contained_object_id == source.object_id
                assert transfer.contained_owner_worker_id == core.worker_id
                assert isinstance(transfer.source, BorrowedContainedSource)
                assert transfer.source.borrower_worker_id == node.worker_id
                assert isinstance(transfer.source.original_source, protocol.TaskHoldSource)
                assert transfer.source.original_source.hold.origin_attempt_id == reply.attempt_id
        assert set(by_attempt) == {initial_attempt, initial_attempt.next()}
        first_push, first_reply = by_attempt[initial_attempt]
        second_push, second_reply = by_attempt[initial_attempt.next()]
        assert first_reply.output_publication == envelope and first_push.target_execution is None
        target = TargetExecutionKey.from_task_spec(second_push.spec, (output_ids[1],))
        assert second_push.target_execution == second_reply.target_execution == target
        assert second_reply.output_publication.publication_id.execution == target
        assert second_reply.output_publication.publication_id.full_output_ids == output_ids
        assert second_reply.output_publication.publication_id.output_ids == (output_ids[1],)
        assert rebuilt.output_publication.manifest == second_reply.output_publication.manifest
        assert first_push.lease_id != second_push.lease_id
        recovery = core._recovery.task_record(task_id)
        assert recovery.state is TaskState.SUCCEEDED and recovery.current_attempt == initial_attempt.next()
        assert recovery.retries_started == 1 and core._recovery.active_recovery(task_id) is None
        assert core._targeted_reconstruction_coordinator().current_session(task_id) is None

        _remaining(deadline)
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        for reference in restored:
            _close_current(reference, cleanup_deadline)
        _close_current(refs[0], cleanup_deadline)
        _wait_current(core, lambda: core.owner_table.collection_state(output_ids[0]) is ObjectCollectionState.COLLECTED, cleanup_deadline)
        assert core.owner_table.snapshot(output_ids[1]).state is ObjectState.READY_STORED
        assert core._recovery.lineage_for_object(output_ids[1]).output_ids == output_ids
        assert core.owner_table.snapshot(source.object_id).contained_holds == frozenset({replacement.final_hold})
        _close_current(refs[1], cleanup_deadline)
        _wait_current(core, lambda: all(core.owner_table.collection_state(value) is ObjectCollectionState.COLLECTED for value in output_ids), cleanup_deadline)
        child_after_gc = core.owner_table.snapshot(source.object_id)
        assert child_after_gc.local_tokens == frozenset({source._local_token})
        assert not child_after_gc.contained_holds and not child_after_gc.lineage_tokens
        with pytest.raises(UnknownTaskError):
            core._recovery.task_record(task_id)
        _close_current(source, cleanup_deadline)
        _wait_current(core, lambda: core.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED, cleanup_deadline)
        assert all(not core.owner_table.contains(value) and value not in core._objects
                   and value not in core._stored_descriptors and value not in core._object_gc_obligations
                   for value in (*output_ids, source.object_id))
        assert not core._output_retirement_work and finished()
    finally:
        if cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if core is not None and original_rpc is not None:
                core._rpc = original_rpc
            if core is not None and original_push is not None:
                core._push_task_rpc = original_push
            for reference in handles:
                try:
                    _close_current(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                if report is not None:
                    pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
                wake = threading.Event()
                for _ in range(_MAX_POLLS):
                    remaining = cleanup_deadline - time.monotonic()
                    if not any(_pid_exists(pid) for pid in pids) or remaining <= 0:
                        break
                    wake.wait(min(0.02, remaining))
                surviving = tuple(pid for pid in pids if _pid_exists(pid))
                surviving_children = tuple(child.pid for child in mp.active_children() if child.pid in pids)
                open_addresses = []
                for address in addresses:
                    try:
                        with socket.create_connection(address, timeout=0.1):
                            open_addresses.append(address)
                    except OSError:
                        pass
                assert not surviving, surviving
                assert not surviving_children, surviving_children
                assert not open_addresses, open_addresses
                assert not close_errors, close_errors
                assert not hook_errors and not overflow and not observation_failed
                assert not ray.is_initialized()
    assert context is not None and report is not None and death is not None
    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0 and not report.forced
    assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.node_cleans == (False, True) and report.node_forced == (False, False)
    assert report.node_exitcodes == (-signal.SIGKILL, 0) and report.node_deaths[0] == death
    assert report.node_resources_clean[1] and report.node_finalized[1] and report.node_shutdown_ack_clean[1]
    assert report.worker_cleans[1] and report.worker_exitcodes[1] == 0
