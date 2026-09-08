"""Bounded unified mixed-output/contained-reference acceptance.

One GCS, one Node, one Worker (three children), one Driver-owned tiny child
put and one two-return producer; one explicit stored-slot drop and one targeted
reconstruction, one CPU, a 1-MiB store and 8-KiB padding. Both physical
attempts run the whole producer; only the lost return index 1 is published again.
There is no ACK loss, Worker/Node crash, extra Task, gate, thread or trace.

This case's public gets/finish predicates share fifteen seconds after init.
Six handles and the staged outer/child GC share one final three-second epoch,
also reused in failure-finally. A <=8-entry passive Push observer verifies
actual whole/selected envelopes; only original protocols create child holds.
No queue join or manual finalizer supplies collection evidence. All three
PIDs/four endpoints are checked after unconditional shutdown under the exact
node ID's external 30-second runner. Legacy helpers below retain their original
semantics for other importing tests; this case uses only the *_current helpers.
The shared work budget is not cancellation of synchronous reconstruction RPCs.
"""

from dataclasses import replace
import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.core import _RPC_CALL_DEADLINE, _RPC_TOTAL_TIMEOUT_SECONDS
from miniray.ids import AttemptID
from miniray.output_publication import OutputPublicationCompleteWitness, OutputPublicationEnvelope
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.protocol import TaskHoldSource
from miniray.publication_sources import BorrowedContainedSource
from miniray.recovery import UnknownTaskError
from miniray.task_outputs import TargetExecutionKey, TaskExecutionKey


pytestmark = pytest.mark.multiprocess_smoke

_TIMEOUT = 10.0
_PADDING = b"M" * (8 * 1024)
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 8
_MAX_POLLS = 128


@ray.remote(num_returns=2, max_retries=1)
def _mixed_same_child(container):
    child = container[0]
    return {"child": child, "slot": 0}, {"child": child, "slot": 1, "padding": _PADDING}


def _wait(core, predicate):
    deadline = time.monotonic() + _TIMEOUT
    with core._completion:
        while not predicate():
            remaining = deadline - time.monotonic()
            assert remaining > 0, "bounded owner transition did not finish"
            core._completion.wait(remaining)


def _close_local(reference, deadline):
    if reference is None or reference.closed:
        return
    assert reference.borrower_token is None
    reference._closed = True
    if reference._finalizer is not None:
        reference._finalizer()
    if reference._release_done is not None:
        assert reference._release_done.wait(max(0.0, deadline - time.monotonic()))


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def _remaining_current(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("mixed-contained work or cleanup deadline expired")
    return remaining


def _wait_current(core, predicate, deadline):
    with core._completion:
        for _ in range(_MAX_POLLS):
            if predicate():
                return
            core._completion.wait(min(0.05, _remaining_current(deadline)))
        if predicate():
            return
    raise TimeoutError("mixed-contained owner transition did not converge")


def _close_current(reference, deadline):
    if reference is None:
        return
    assert reference.borrower_token is None
    finalizer, done = reference._finalizer, reference._release_done
    assert finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _pid_exists_current(pid):
    try:
        return _pid_exists(pid)
    except PermissionError:
        return True


def test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot():
    context = report = core = source = None
    refs = ()
    restored = []
    handles = []
    original_push = None
    cleanup_deadline = None
    close_errors = []
    observations = []
    observation_lock = threading.Lock()
    observation_failed = overflow = False
    pids, addresses = set(), set()

    def inspect_push(address, handler, request):
        nonlocal observation_failed, overflow
        reply = original_push(address, handler, request)
        try:
            with observation_lock:
                if len(observations) < _MAX_OBSERVATIONS:
                    observations.append((address, handler, request, reply))
                else:
                    overflow = True
        except Exception:
            observation_failed = True
        # No observer assertion, new failure, mutation or replacement ACK.
        return reply

    def remember_child(value):
        child = value["child"]
        if isinstance(child, ray.ObjectRef):
            handles.append(child)
            restored.append(child)
        assert isinstance(child, ray.ObjectRef)
        assert child.object_id == source.object_id and child.owner_worker_id == core.worker_id
        assert child.owner_address == core.owner_address and child.borrower_token is None
        return child

    try:
        context = ray.init(num_nodes=1, num_cpus=1, num_workers_per_node=1,
                           inline_threshold=1024, object_store_bytes=1024 * 1024,
                           enable_tracing=False)
        deadline = time.monotonic() + _WORK_SECONDS
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None and context.trace_address is None
        addresses.add(runtime.owner_service.address)
        assert len(pids) == 3 and os.getpid() not in pids and len(addresses) == 4
        original_push = core._push_task_rpc
        core._push_task_rpc = inspect_push
        _remaining_current(deadline)
        source = ray.put(("shared-driver-child", 42))
        handles.append(source)
        submitted = _mixed_same_child.remote([source])
        for reference in submitted if isinstance(submitted, tuple) else (submitted,):
            if isinstance(reference, ray.ObjectRef):
                handles.append(reference)
        assert isinstance(submitted, tuple) and len(submitted) == 2
        assert all(isinstance(reference, ray.ObjectRef) for reference in submitted)
        refs = submitted
        # get_many uses this same order. Retain each reconstructed child handle
        # immediately so a later outer-get failure cannot lose its public close.
        first = ray.get(refs[0], timeout=_remaining_current(deadline))
        first_child = remember_child(first)
        second = ray.get(refs[1], timeout=_remaining_current(deadline))
        remember_child(second)
        assert ray.get(first_child, timeout=_remaining_current(deadline)) == ("shared-driver-child", 42)
        assert second["padding"] == _PADDING
        assert first["slot"] == 0 and second["slot"] == 1
        _wait_current(core, lambda: all(ref.object_id not in core._task_finish_barriers for ref in refs)
                      and not core._protocol_unresolved and core._accepted_task_count == 0, deadline)
        before = tuple(core.owner_table.snapshot(ref.object_id) for ref in refs)
        assert tuple(value.state for value in before) == (ObjectState.READY_INLINE, ObjectState.READY_STORED)
        memberships = tuple(value.output_publication for value in before)
        assert all(member is not None for member in memberships)
        output_ids = tuple(reference.object_id for reference in refs)
        task_id = output_ids[0].task_id
        attempt_0 = AttemptID(task_id, 0)
        assert tuple(object_id.return_index for object_id in output_ids) == (0, 1)
        assert all(object_id.task_id == task_id for object_id in output_ids)
        assert all(snapshot.current_attempt == attempt_0 for snapshot in before)
        assert memberships[0].publication_id == memberships[1].publication_id
        assert memberships[0].manifest == memberships[1].manifest
        assert all(len(member.slot.transfers) == 1 for member in memberships)
        transfers = tuple(member.slot.transfers[0] for member in memberships)
        assert transfers[0].contained_object_id == transfers[1].contained_object_id == source.object_id
        assert transfers[0].final_hold != transfers[1].final_hold
        assert all(isinstance(value.source, BorrowedContainedSource)
                   and isinstance(value.source.original_source, TaskHoldSource) for value in transfers)
        source_before = core.owner_table.snapshot(source.object_id)
        assert source_before.contained_holds == frozenset(transfer.final_hold for transfer in transfers)
        assert all(transfer.provisional_hold not in source_before.contained_holds for transfer in transfers)
        _remaining_current(deadline)
        drop_deadline = min(deadline, time.monotonic() + _RPC_TOTAL_TIMEOUT_SECONDS)
        enclosing_deadline = _RPC_CALL_DEADLINE.get()
        if enclosing_deadline is not None:
            drop_deadline = min(drop_deadline, enclosing_deadline)
        token = _RPC_CALL_DEADLINE.set(drop_deadline)
        try:
            assert ray.drop_object(refs[1])
        finally:
            _RPC_CALL_DEADLINE.reset(token)
        assert core.owner_table.snapshot(refs[1].object_id).state is ObjectState.LOST
        rebuilt = ray.get(refs[1], timeout=_remaining_current(deadline))
        rebuilt_child = remember_child(rebuilt)
        assert rebuilt["padding"] == _PADDING
        assert rebuilt["slot"] == 1
        assert ray.get(rebuilt_child, timeout=_remaining_current(deadline)) == ("shared-driver-child", 42)
        _wait_current(core, lambda: refs[1].object_id not in core._task_finish_barriers
                      and not core._protocol_unresolved and core._accepted_task_count == 0, deadline)
        assert core.owner_table.snapshot(refs[0].object_id) == before[0]
        replaced = core.owner_table.snapshot(refs[1].object_id)
        assert replaced.current_attempt == before[1].current_attempt.next()
        assert replaced.output_publication.slot.object_id.return_index == 1
        assert replaced.output_publication.publication_id != memberships[1].publication_id
        attempt_1 = attempt_0.next()
        assert replaced.current_attempt == attempt_1
        assert replaced.state is ObjectState.READY_STORED and replaced.locations == frozenset((context.node_id,))
        replacement_transfer, = replaced.output_publication.slot.transfers
        assert replacement_transfer.final_hold not in (transfers[0].final_hold, transfers[1].final_hold)
        source_after = core.owner_table.snapshot(source.object_id)
        assert source_after.contained_holds == frozenset((transfers[0].final_hold, replacement_transfer.final_hold))
        assert transfers[1].final_hold not in source_after.contained_holds
        assert replacement_transfer.provisional_hold not in source_after.contained_holds
        assert source_after.lineage_tokens == source_before.lineage_tokens
        assert core._recovery.task_record(task_id).current_attempt == attempt_1
        assert core._recovery.task_record(task_id).retries_started == 1
        assert core._recovery.active_recovery(task_id) is None

        with observation_lock:
            exchanges = tuple(observations)
            assert not overflow and not observation_failed
        by_attempt = {}
        for address, handler, push, reply in exchanges:
            assert address == context.worker_address and handler == "push_task"
            assert type(push) is protocol.PushTask and type(reply) is protocol.TaskReply
            reply = replace(reply)
            assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
            assert push.worker_id == reply.worker_id == context.worker_id
            assert push.spec.task_id == reply.task_id == task_id
            assert push.spec.attempt_id == reply.attempt_id and push.spec.return_ids() == output_ids
            assert push.dependencies == () and len(push.spec.args) == 1
            nested_arg = push.spec.args[0]
            assert type(nested_arg) is protocol.InlineArg
            nested_transfer, = nested_arg.nested_refs
            envelope = reply.output_publication
            assert type(envelope) is OutputPublicationEnvelope and envelope.results == reply.results
            assert envelope.publication_id.lease_id == push.lease_id
            assert envelope.publication_id.attempt_id == reply.attempt_id
            assert envelope.publication_id.full_output_ids == output_ids
            assert envelope.complete == OutputPublicationCompleteWitness.for_manifest(envelope.manifest)
            assert envelope.manifest.header.owner_worker_id == core.worker_id
            assert envelope.manifest.header.executor_worker_id == context.worker_id
            assert envelope.manifest.header.job_id == core.job_id
            assert envelope.manifest.header.node_incarnation.node_id == context.node_id
            assert envelope.manifest.header.node_incarnation.node_pid == context.node_pid
            for slot in envelope.manifest.slots:
                transfer, = slot.transfers
                assert transfer.contained_object_id == source.object_id
                assert transfer.contained_owner_worker_id == core.worker_id
                assert transfer.contained_owner_address == core.owner_address
                assert isinstance(transfer.source, BorrowedContainedSource)
                assert isinstance(transfer.source.original_source, TaskHoldSource)
                assert transfer.source.borrower_worker_id == context.worker_id
                assert transfer.source.original_source.hold == nested_transfer.hold
                assert nested_transfer.hold.origin_attempt_id == reply.attempt_id
                assert transfer.final_hold.container_object_id == slot.object_id
                assert transfer.final_hold.container_owner_worker_id == core.worker_id
                assert transfer.provisional_hold.container_owner_worker_id == context.worker_id
            previous = by_attempt.setdefault(reply.attempt_id, (push, reply))
            assert previous == (push, reply)
        assert set(by_attempt) == {attempt_0, attempt_1}
        initial_push, initial_reply = by_attempt[attempt_0]
        targeted_push, targeted_reply = by_attempt[attempt_1]
        initial, targeted = initial_reply.output_publication, targeted_reply.output_publication
        assert initial.manifest == memberships[0].manifest
        assert type(initial.publication_id.execution) is TaskExecutionKey
        assert initial_push.target_execution is None and initial_reply.target_execution is None
        assert initial.publication_id.output_ids == output_ids
        assert tuple(slot.tier for slot in initial.manifest.slots) == (
            protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE,
        )
        assert targeted.manifest == replaced.output_publication.manifest
        selected = targeted.publication_id.execution
        assert type(selected) is TargetExecutionKey and selected == targeted_push.target_execution == targeted_reply.target_execution
        assert selected.full_output_ids == output_ids and selected.target_output_ids == (output_ids[1],)
        assert targeted.publication_id.output_ids == (output_ids[1],)
        assert tuple(slot.tier for slot in targeted.manifest.slots) == (protocol.ResultStorage.OBJECT_STORE,)
        assert initial_push.lease_id != targeted_push.lease_id
        assert targeted.results == (replaced.canonical_stored_result,)
        assert len(handles) == 6 and len(restored) == 3
        _remaining_current(deadline)
        core._push_task_rpc = original_push

        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        for reference in restored:
            _close_current(reference, cleanup_deadline)
        _close_current(refs[0], cleanup_deadline)
        _wait_current(core, lambda: core.owner_table.collection_state(refs[0].object_id) is ObjectCollectionState.COLLECTED, cleanup_deadline)
        assert core.owner_table.snapshot(refs[1].object_id).state is ObjectState.READY_STORED
        child = core.owner_table.snapshot(source.object_id)
        assert child.contained_holds == frozenset((replacement_transfer.final_hold,))
        assert child.lineage_tokens and child.local_tokens
        _close_current(refs[1], cleanup_deadline)
        _wait_current(core, lambda: core.owner_table.collection_state(refs[1].object_id) is ObjectCollectionState.COLLECTED, cleanup_deadline)
        child = core.owner_table.snapshot(source.object_id)
        assert not child.contained_holds and not child.lineage_tokens
        assert child.state is ObjectState.READY_INLINE and len(child.local_tokens) == 1
        _close_current(source, cleanup_deadline)
        _wait_current(core, lambda: core.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED, cleanup_deadline)
        assert not core._output_retirement_work
        logical_ids = (*output_ids, source.object_id)
        assert all(not core.owner_table.contains(object_id) for object_id in logical_ids)
        assert all(object_id not in core._objects and object_id not in core._stored_descriptors
                   and object_id not in core._object_gc_obligations for object_id in logical_ids)
        assert all(core._recovery.lineage_for_object(object_id) is None for object_id in output_ids)
        with pytest.raises(UnknownTaskError):
            core._recovery.task_record(task_id)
    finally:
        if cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
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
                if core is not None and core.owner_address is not None:
                    addresses.add(core.owner_address)
                surviving_pids = tuple(pid for pid in sorted(pids) if _pid_exists_current(pid))
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
                assert not observation_failed and not overflow
                assert not ray.is_initialized()
    assert context is not None and report is not None
    assert report.core_stopped and report.gcs_clean and report.node_clean and report.worker_clean
    assert report.gcs_pid == context.gcs_pid and report.gcs_exitcode == 0
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert all(exitcode == 0 for exitcode in report.node_exitcodes + report.worker_exitcodes)
    assert report.shutdown_ack_clean
    assert report.resources_clean and report.finalized and not report.forced
    assert not ray.is_initialized()
