"""Bounded one-node partial-loss multi-return reconstruction smoke.

Run only this exact node ID through ``scripts/run_bounded_test.py``.  Static
bounds are one GCS, one Node, one ordinary Worker, one logical three-return
Task and two physical attempts. A single debug drop removes return index 1.
Both user-code invocations still compute all three 12/16/20-KiB values; only
the selected lost slot is published again. Healthy slots retain attempt 0.
All serialized values remain below 32 KiB, with one 1-MiB store and one CPU.

Public gets and two bounded finish observations share fifteen seconds after
init. The local drop uses the existing Core RPC deadline, without changing its
request or completion authority. A passive observer retains at most eight
actual Push replies to verify the full and selected publication manifests.
Three public reference closes and true local owner/lineage GC share one final
three-second cleanup epoch, also reused by failure-finally. No extra Task,
fault, thread, listener, sleep or tracing is introduced. All three PIDs/four
endpoints are checked after unconditional shutdown; the outer runner remains
the 30-second process-tree bound for startup and shutdown.
"""

from __future__ import annotations

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
from miniray.recovery import UnknownTaskError
from miniray.task_outputs import TargetExecutionKey, TaskExecutionKey


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 8
_MAX_POLLS = 128
_OBJECT_STORE_BYTES = 1024 * 1024
_VALUE_BYTES = (12 * 1024, 16 * 1024, 20 * 1024)


@ray.remote(num_returns=3, max_retries=1)
def _produce_three_stored() -> tuple[bytes, bytes, bytes]:
    return tuple(
        bytes([65 + index]) * size
        for index, size in enumerate(_VALUE_BYTES)
    )


def _pid_exists(pid: int) -> bool:
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
        raise TimeoutError("partial multi-return work exceeded its deadline")
    return remaining


def _close_reference(reference, deadline):
    finalizer, done = reference._finalizer, reference._release_done
    assert finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _wait_finished(core, output_ids, deadline):
    with core._completion:
        for _ in range(_MAX_POLLS):
            if (all(object_id not in core._task_finish_barriers for object_id in output_ids)
                    and not core._protocol_unresolved and core._accepted_task_count == 0):
                return
            core._completion.wait(min(0.1, _remaining(deadline)))
    raise TimeoutError("multi-return publication finish did not converge")


def test_one_lost_return_reconstructs_without_changing_healthy_siblings() -> None:
    context = None
    report = None
    refs: list[ray.ObjectRef] = []
    core = original_push = None
    cleanup_deadline = None
    close_errors = []
    observations = []
    observation_lock = threading.Lock()
    observation_failed = overflow = False
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()

    def inspect_push(address, handler, message):
        nonlocal observation_failed, overflow
        reply = original_push(address, handler, message)
        try:
            with observation_lock:
                if len(observations) < _MAX_OBSERVATIONS:
                    observations.append((address, handler, message, reply))
                else:
                    overflow = True
        except Exception:
            observation_failed = True
        # Observation cannot change execution, manufacture Complete, or turn
        # a successful RPC into an injected failure. Assertions stay in main.
        return reply

    try:
        context = ray.init(
            num_nodes=1, num_cpus=1, num_workers_per_node=1,
            inline_threshold=1024, object_store_bytes=_OBJECT_STORE_BYTES,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 3 and os.getpid() not in managed_pids
        assert len(managed_addresses) == 4 and context.trace_address is None
        original_push = core._push_task_rpc
        core._push_task_rpc = inspect_push

        _remaining(deadline)
        submitted = _produce_three_stored.remote()
        for reference in submitted if isinstance(submitted, tuple) else (submitted,):
            if isinstance(reference, ray.ObjectRef):
                refs.append(reference)
        assert isinstance(submitted, tuple) and len(submitted) == 3
        assert tuple(refs) == submitted
        expected = [
            bytes([65 + index]) * size
            for index, size in enumerate(_VALUE_BYTES)
        ]
        assert ray.get(refs, timeout=_remaining(deadline)) == expected
        output_ids = tuple(ref.object_id for ref in refs)
        task_id = output_ids[0].task_id
        attempt_0 = AttemptID(task_id, 0)
        assert tuple(object_id.task_id for object_id in output_ids) == (task_id,) * 3
        assert tuple(object_id.return_index for object_id in output_ids) == (0, 1, 2)
        # READY precedes Node payload retirement. Take the healthy-sibling
        # baseline only after the original successful publication has finished.
        _wait_finished(core, output_ids, deadline)
        healthy_ids = (output_ids[0], output_ids[2])
        healthy_snapshots = tuple(
            core.owner_table.snapshot(value) for value in healthy_ids
        )
        healthy_descriptors = tuple(
            core._stored_descriptors[value] for value in healthy_ids
        )
        assert all(
            snapshot.state is ObjectState.READY_STORED
            and snapshot.current_attempt == attempt_0
            and snapshot.locations == frozenset({context.node_id})
            for snapshot in healthy_snapshots
        )

        lost_ref = refs[1]
        _remaining(deadline)
        drop_deadline = min(deadline, time.monotonic() + _RPC_TOTAL_TIMEOUT_SECONDS)
        enclosing_deadline = _RPC_CALL_DEADLINE.get()
        if enclosing_deadline is not None:
            drop_deadline = min(drop_deadline, enclosing_deadline)
        token = _RPC_CALL_DEADLINE.set(drop_deadline)
        try:
            assert ray.drop_object(lost_ref)
        finally:
            _RPC_CALL_DEADLINE.reset(token)
        assert core.owner_table.snapshot(output_ids[1]).state is ObjectState.LOST
        assert ray.get(lost_ref, timeout=_remaining(deadline)) == expected[1]
        assert ray.get(refs, timeout=_remaining(deadline)) == expected
        _wait_finished(core, output_ids, deadline)

        attempt_1 = attempt_0.next()
        target = core.owner_table.snapshot(output_ids[1])
        assert target.state is ObjectState.READY_STORED
        assert target.current_attempt == attempt_1
        assert target.locations == frozenset({context.node_id})
        assert core._stored_descriptors[output_ids[1]].object_id == output_ids[1]
        assert tuple(
            core.owner_table.snapshot(value) for value in healthy_ids
        ) == healthy_snapshots
        assert tuple(
            core._stored_descriptors[value] for value in healthy_ids
        ) == healthy_descriptors
        assert all(
            snapshot.current_attempt == attempt_0
            for snapshot in healthy_snapshots
        )
        assert core._recovery.task_record(task_id).current_attempt == attempt_1
        assert core._recovery.task_record(task_id).retries_started == 1
        assert core._recovery.active_recovery(task_id) is None
        assert tuple(ref.object_id for ref in refs) == output_ids

        with observation_lock:
            exchanges = tuple(observations)
            assert not overflow and not observation_failed
        by_attempt = {}
        for address, handler, push, reply in exchanges:
            assert address == context.worker_address and handler == "push_task"
            assert type(push) is protocol.PushTask and type(reply) is protocol.TaskReply
            reply = replace(reply)
            assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
            assert push.spec.task_id == reply.task_id == task_id
            assert push.spec.attempt_id == reply.attempt_id
            assert push.worker_id == reply.worker_id == context.worker_id
            assert push.spec.return_ids() == output_ids and push.dependencies == ()
            previous = by_attempt.setdefault(reply.attempt_id, (push, reply))
            assert previous == (push, reply)  # transport replay cannot change the envelope
            envelope = reply.output_publication
            assert type(envelope) is OutputPublicationEnvelope
            assert envelope.publication_id.lease_id == push.lease_id
            assert envelope.publication_id.attempt_id == reply.attempt_id
            assert envelope.publication_id.full_output_ids == output_ids
            assert envelope.manifest.header.owner_worker_id == core.worker_id
            assert envelope.manifest.header.executor_worker_id == context.worker_id
            assert envelope.manifest.header.job_id == core.job_id
            incarnation = envelope.manifest.header.node_incarnation
            assert incarnation.node_id == context.node_id and incarnation.node_pid == context.node_pid
            assert envelope.complete == OutputPublicationCompleteWitness.for_manifest(envelope.manifest)
            assert all(result.storage is protocol.ResultStorage.OBJECT_STORE and result.inline_data is None
                       and result.owner_worker_id == core.worker_id and result.node_id == context.node_id
                       and _VALUE_BYTES[result.object_id.return_index] < result.size_bytes < 32 * 1024
                       for result in envelope.results)
            assert all(not slot.transfers for slot in envelope.manifest.slots)
        assert set(by_attempt) == {attempt_0, attempt_1}
        first_push, first_reply = by_attempt[attempt_0]
        next_push, next_reply = by_attempt[attempt_1]
        first_envelope, next_envelope = first_reply.output_publication, next_reply.output_publication
        assert first_push.lease_id != next_push.lease_id
        assert first_push.target_execution is None and first_reply.target_execution is None
        assert type(first_envelope.publication_id.execution) is TaskExecutionKey
        assert first_envelope.publication_id.output_ids == output_ids
        assert tuple(result.object_id for result in first_envelope.results) == output_ids
        selected = next_envelope.publication_id.execution
        assert type(selected) is TargetExecutionKey
        assert selected == next_push.target_execution == next_reply.target_execution
        assert selected.full_output_ids == output_ids and selected.target_output_ids == (output_ids[1],)
        assert next_envelope.publication_id.output_ids == (output_ids[1],)
        assert tuple(result.object_id for result in next_envelope.results) == (output_ids[1],)
        assert target.canonical_stored_result == next_envelope.results[0]
        assert target.output_publication.publication_id == next_envelope.publication_id
        assert tuple(snapshot.canonical_stored_result for snapshot in healthy_snapshots) == (
            first_envelope.results[0], first_envelope.results[2],
        )
        _remaining(deadline)
        core._push_task_rpc = original_push

        # Final close and real per-slot GC get one shared epoch, not a fresh
        # three-second wait per ref followed by another work-sized GC budget.
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        for reference in refs:
            _close_reference(reference, cleanup_deadline)
        with core._completion:
            for _ in range(_MAX_POLLS):
                if (all(core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
                        for object_id in output_ids) and not core._object_gc_obligations):
                    break
                core._completion.wait(min(0.05, _remaining(cleanup_deadline)))
            else:
                raise TimeoutError("partial multi-return GC did not converge")
            assert all(not core.owner_table.contains(object_id) for object_id in output_ids)
            assert all(core._recovery.lineage_for_object(object_id) is None for object_id in output_ids)
            with pytest.raises(UnknownTaskError):
                core._recovery.task_record(task_id)
            assert core._recovery.active_recovery(task_id) is None
        # COLLECTED is the real owner's post-Drop-ACK commit. No fake deletion
        # receipt, mailbox clearing or shutdown repair supplies this evidence.
    finally:
        if cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if core is not None and original_push is not None:
                core._push_task_rpc = original_push
            for reference in refs:
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                if report is not None:
                    managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
                if core is not None and core.owner_address is not None:
                    managed_addresses.add(core.owner_address)
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
                assert not close_errors, close_errors
                assert not observation_failed and not overflow
                assert not ray.is_initialized()

    assert context is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.node_exitcode == 0
    assert report.worker_clean and report.worker_exitcode == 0
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
