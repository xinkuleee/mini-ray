"""Bounded public multi-return acceptance path.

One fail-once producer declares two return slots and retries as one logical
task.  Its successful attempt publishes one inline value and one store-backed
value atomically.  Each sibling then drives a distinct downstream task before
the test closes the consumers and proves that producer lineage is task-scoped:
collecting slot zero preserves the task while slot one is live, and collecting
the final slot removes the complete manifest.

Run only this exact node ID through ``scripts/run_bounded_test.py``.  Static
bounds are one GCS, one Node, one ordinary Worker, three logical tasks (four
physical attempts including the injected retry), four result objects and a
32-KiB maximum application value plus small serialization metadata. Attempt 0
fails with SYSTEM_ERROR before user-code decode: the producer callable itself
runs once, as does each consumer. This is a whole-manifest system retry, not
lineage reconstruction or a targeted-slot execution.

There is one CPU and a 1-MiB ObjectStore, no trace collector, test-owned thread,
gate or sleep. Public get/wait calls share fifteen seconds after init. A
passive observer retains at most eight actual Push replies. All three staged
close/GC gates share one final three-second cleanup epoch, which finally
reuses rather than resetting. Failure paths always reach shutdown and exact
three-PID/four-endpoint checks. Startup/shutdown retain the runner's 30-second
process-tree bound.
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
from miniray import protocol
from miniray.api import _get_runtime
from miniray.ids import AttemptID
from miniray.output_publication import OutputPublicationCompleteWitness, OutputPublicationEnvelope
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import UnknownTaskError
from miniray.task_outputs import TaskExecutionKey
from miniray.worker import WorkerFailpointConfig


pytestmark = pytest.mark.multiprocess_smoke

_STORED_BYTES = 32 * 1024
_STORED_BYTE = b"M"
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 8
_MAX_GC_POLLS = 128
_OBJECT_STORE_BYTES = 1024 * 1024


@ray.remote(num_returns=2, max_retries=1)
def _make_mixed_outputs() -> tuple[int, bytes]:
    return 7, _STORED_BYTE * _STORED_BYTES


@ray.remote
def _consume_inline(value: int) -> tuple[int, str, int]:
    return os.getpid(), "inline", value * 3


@ray.remote
def _consume_stored(value: bytes) -> tuple[int, str, int, str]:
    return os.getpid(), "stored", len(value), hashlib.sha256(value).hexdigest()


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
        raise TimeoutError("mixed-output work or cleanup exceeded its deadline")
    return remaining


def _close_reference(reference, deadline):
    finalizer, done = reference._finalizer, reference._release_done
    assert finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _wait_collected(core: object, refs, deadline: float) -> None:
    """Wait for exact owner GC, not merely a transient mailbox-empty state.

    READY may precede the final adoption ACK and task-finish barrier.  A close
    event can therefore be consumed before its later GC wake is enqueued.
    """
    with core._completion:
        for _ in range(_MAX_GC_POLLS):
            if all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED for ref in refs):
                return
            core._completion.wait(min(0.05, _remaining(deadline)))
        if all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED for ref in refs):
            return
    raise TimeoutError("owner collection exceeded its finite observation bound")


def test_public_multi_return_mixed_outputs_retry_dependencies_and_gc() -> None:
    context = None
    report = None
    core = None
    output_refs: tuple[ray.ObjectRef, ...] = ()
    consumer_refs: tuple[ray.ObjectRef, ...] = ()
    refs: list[ray.ObjectRef] = []
    original_push = None
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
        # Never alter the real reply or execute assertions in the RPC path.
        # The existing Worker failpoint is this test's only injected failure.
        return reply

    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=1,
            num_workers_per_node=1,
            inline_threshold=1024,
            object_store_bytes=_OBJECT_STORE_BYTES,
            enable_tracing=False,
            _test_worker_failpoint=WorkerFailpointConfig(),
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        assert context.trace_address is None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 3
        assert len(managed_addresses) == 4
        assert os.getpid() not in managed_pids
        original_push = core._push_task_rpc
        core._push_task_rpc = inspect_push

        _remaining(deadline)
        submitted = _make_mixed_outputs.remote()
        received = submitted if isinstance(submitted, tuple) else (submitted,)
        for reference in received:
            if isinstance(reference, ray.ObjectRef):
                refs.append(reference)
        assert isinstance(submitted, tuple) and len(submitted) == 2
        assert all(isinstance(ref, ray.ObjectRef) for ref in submitted)
        output_refs = submitted
        inline_ref, stored_ref = output_refs
        output_ids = tuple(ref.object_id for ref in output_refs)
        task_id = output_ids[0].task_id
        assert tuple(object_id.task_id for object_id in output_ids) == (
            task_id, task_id
        )
        assert tuple(object_id.return_index for object_id in output_ids) == (0, 1)

        # The public wait/get surfaces preserve the caller's order even though
        # the two slots use different storage tiers of one publication.
        ready, remaining = ray.wait(
            (stored_ref, inline_ref),
            num_returns=2,
            timeout=_remaining(deadline),
        )
        assert ready == [stored_ref, inline_ref]
        assert remaining == []
        payload = _STORED_BYTE * _STORED_BYTES
        assert ray.get(
            (stored_ref, inline_ref), timeout=_remaining(deadline)
        ) == [payload, 7]

        snapshots = tuple(core.owner_table.snapshot(value) for value in output_ids)
        retry_attempt = snapshots[0].current_attempt
        assert retry_attempt is not None and retry_attempt.attempt_number == 1
        assert tuple(snapshot.current_attempt for snapshot in snapshots) == (
            retry_attempt, retry_attempt
        )
        assert tuple(snapshot.state for snapshot in snapshots) == (
            ObjectState.READY_INLINE, ObjectState.READY_STORED
        )
        assert all(
            snapshot.producer_task_spec is not None
            and snapshot.producer_task_spec.task_id == task_id
            and snapshot.producer_task_spec.return_ids() == output_ids
            for snapshot in snapshots
        )
        stored_descriptor = core._stored_descriptors[stored_ref.object_id]
        assert stored_descriptor.object_id == stored_ref.object_id
        assert stored_descriptor.owner_worker_id == core.worker_id
        assert stored_descriptor.node_id == context.node_id
        assert snapshots[1].locations == frozenset({context.node_id})

        inline_consumer = _consume_inline.remote(inline_ref)
        refs.append(inline_consumer)
        stored_consumer = _consume_stored.remote(stored_ref)
        refs.append(stored_consumer)
        consumer_refs = inline_consumer, stored_consumer
        assert ray.get(
            inline_consumer, timeout=_remaining(deadline)
        ) == (context.worker_pid, "inline", 21)
        assert ray.get(
            stored_consumer, timeout=_remaining(deadline)
        ) == (
            context.worker_pid,
            "stored",
            _STORED_BYTES,
            hashlib.sha256(payload).hexdigest(),
        )

        with observation_lock:
            exchanges = tuple(observations)
            assert not overflow and not observation_failed
        by_attempt = {}
        for address, handler, push, reply in exchanges:
            assert address == context.worker_address and handler == "push_task"
            assert type(push) is protocol.PushTask and type(reply) is protocol.TaskReply
            reply = replace(reply)
            assert push.worker_id == reply.worker_id == context.worker_id
            assert push.spec.task_id == reply.task_id and push.spec.attempt_id == reply.attempt_id
            assert push.target_execution is None and reply.target_execution is None
            previous = by_attempt.setdefault(reply.attempt_id, (push, reply))
            assert previous == (push, reply)  # an exact transport replay is not another attempt
            if reply.status is protocol.TaskReplyStatus.SUCCEEDED:
                envelope = reply.output_publication
                assert type(envelope) is OutputPublicationEnvelope
                assert type(envelope.publication_id.execution) is TaskExecutionKey
                assert envelope.publication_id.execution == TaskExecutionKey.from_task_spec(push.spec)
                assert envelope.publication_id.lease_id == push.lease_id
                assert envelope.results == reply.results
                assert envelope.complete == OutputPublicationCompleteWitness.for_manifest(envelope.manifest)
                assert envelope.manifest.header.owner_worker_id == core.worker_id
                assert envelope.manifest.header.executor_worker_id == context.worker_id
                assert envelope.manifest.header.job_id == core.job_id
                incarnation = envelope.manifest.header.node_incarnation
                assert incarnation.node_id == context.node_id and incarnation.node_pid == context.node_pid
        initial_attempt = AttemptID(task_id, 0)
        inline_attempt = AttemptID(inline_consumer.object_id.task_id, 0)
        stored_attempt = AttemptID(stored_consumer.object_id.task_id, 0)
        assert set(by_attempt) == {initial_attempt, retry_attempt, inline_attempt, stored_attempt}
        failed_push, failed_reply = by_attempt[initial_attempt]
        producer_push, producer_reply = by_attempt[retry_attempt]
        assert failed_reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
        assert failed_reply.results == () and failed_reply.output_publication is None
        assert failed_reply.error is not None
        assert failed_reply.error.message == "injected system failure before user-code decode"
        assert producer_reply.status is protocol.TaskReplyStatus.SUCCEEDED
        assert failed_push.spec.return_ids() == producer_push.spec.return_ids() == output_ids
        assert failed_push.spec.function == producer_push.spec.function
        assert failed_push.lease_id != producer_push.lease_id
        assert failed_push.dependencies == producer_push.dependencies == ()
        publication = producer_reply.output_publication
        assert publication.publication_id.full_output_ids == publication.publication_id.output_ids == output_ids
        assert tuple(result.storage for result in publication.results) == (
            protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE,
        )
        assert publication.results[0].inline_data == snapshots[0].inline_data
        assert publication.results[1] == stored_descriptor == snapshots[1].canonical_stored_result
        assert _STORED_BYTES < stored_descriptor.size_bytes < _STORED_BYTES + 1024
        assert all(snapshot.output_publication.publication_id == publication.publication_id for snapshot in snapshots)
        assert core._recovery.task_record(task_id).retries_started == 1
        assert core._recovery.active_recovery(task_id) is None
        inline_push, inline_reply = by_attempt[inline_attempt]
        stored_push, stored_reply = by_attempt[stored_attempt]
        assert inline_reply.status is stored_reply.status is protocol.TaskReplyStatus.SUCCEEDED
        assert inline_push.spec.return_ids() == (inline_consumer.object_id,)
        assert stored_push.spec.return_ids() == (stored_consumer.object_id,)
        assert len(inline_push.spec.args) == len(stored_push.spec.args) == 1
        inline_arg = inline_push.spec.args[0]
        assert type(inline_arg) is protocol.InlineArg and inline_arg.data == snapshots[0].inline_data
        assert inline_arg.nested_refs == () and inline_push.dependencies == ()
        stored_arg = stored_push.spec.args[0]
        assert type(stored_arg) is protocol.RefArg
        assert (stored_arg.object_id, stored_arg.owner_worker_id) == (stored_ref.object_id, core.worker_id)
        assert stored_push.dependencies == (protocol.ObjectStoreDescriptor(
            stored_ref.object_id, core.worker_id, retry_attempt, context.node_id,
            stored_descriptor.size_bytes, stored_descriptor.checksum,
        ),)
        _remaining(deadline)
        core._push_task_rpc = original_push

        # Consumer lineage owns the input holds after execution.  Releasing the
        # two consumer outputs first leaves exactly the two public producer
        # handles as roots, so the sibling-lifetime assertions below are not a
        # side effect of unrelated downstream lineage.
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        for ref in consumer_refs:
            _close_reference(ref, cleanup_deadline)
            assert ref.closed
        _wait_collected(core, consumer_refs, cleanup_deadline)
        assert all(
            core.owner_table.snapshot(object_id).lineage_tokens == frozenset()
            for object_id in output_ids
        )

        _close_reference(inline_ref, cleanup_deadline)
        assert inline_ref.closed and not stored_ref.closed
        _wait_collected(core, (inline_ref,), cleanup_deadline)
        assert core.owner_table.collection_state(
            inline_ref.object_id
        ) is ObjectCollectionState.COLLECTED
        assert not core.owner_table.contains(inline_ref.object_id)
        surviving_lineage = core._recovery.lineage_for_object(
            stored_ref.object_id
        )
        assert surviving_lineage is not None
        assert surviving_lineage.task_id == task_id
        assert surviving_lineage.output_ids == output_ids
        assert core._recovery.task_record(task_id).current_attempt == retry_attempt
        assert core.owner_table.snapshot(
            stored_ref.object_id
        ).state is ObjectState.READY_STORED

        _close_reference(stored_ref, cleanup_deadline)
        assert stored_ref.closed
        _wait_collected(core, (stored_ref,), cleanup_deadline)
        assert all(
            core.owner_table.collection_state(object_id)
            is ObjectCollectionState.COLLECTED
            for object_id in output_ids
        )
        assert all(not core.owner_table.contains(value) for value in output_ids)
        assert all(value not in core._objects for value in output_ids)
        assert all(value not in core._stored_descriptors for value in output_ids)
        assert all(value not in core._object_gc_obligations for value in output_ids)
        assert all(
            core._recovery.lineage_for_object(value) is None
            for value in output_ids
        )
        with pytest.raises(UnknownTaskError):
            core._recovery.task_record(task_id)
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
