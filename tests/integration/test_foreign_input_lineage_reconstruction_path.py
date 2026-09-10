"""Bounded foreign-input lineage reconstruction smoke.

A Worker-owned inline object escapes through an outer result and becomes a
Driver-submitted consumer dependency. Both original Python handles close after
consumer submission, before waiting for its first result. Its stored output
then loses its only replica: reconstruction must atomically replace the foreign retained hold,
replay the consumer under the same TaskID/ObjectID, and finally release the new
hold when the consumer output is collected.

After review and registration, run only this exact node ID through ``scripts/run_baseline.py --case EXACT`` after its
fixed two-CPU exception is approved. One Node, two Workers, four managed
children, five runtime/owner endpoints, three logical Tasks/four executions,
one drop/reconstruction and a 1 MiB store bound the experiment. There is no
Actor, tracing, GPU, external network, extra listener or test-created thread.

Public get/drop calls, early closes and finish observations share fifteen
seconds after init. Drop uses the existing real Node RPC deadline. Internal
foreign renewal RPCs keep their original finite retry timeouts: the work
budget is checked around calls, not claimed to cancel them. The outer
30-second process-tree runner still bounds startup, shutdown and those calls.
Replacement observation is a passive FIFO capped at 32 records; local waits
make at most 256 checks plus a final predicate check. Final public closes and
actual owner/foreign-lineage collection share three seconds before unconditional
shutdown and failure-finally checks of every recorded PID and endpoint.

This file has no registered smoke or migration selector in the current
manifest. Review and register the exact selector and its input closure
before using the current runner's --case route.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import threading
import time
import uuid

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.core import _RPC_CALL_DEADLINE, _RPC_TOTAL_TIMEOUT_SECONDS
from miniray.foreign_lineage import ForeignLineageRole
from miniray.ids import AttemptID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_POLL_SECONDS = 0.1
_MAX_POLLS = 256
_MAX_REPLACEMENTS = 32
_PADDING = b"foreign-lineage-reconstruction" * 256


@ray.remote(num_cpus=1)
def _foreign_lineage_source() -> tuple[str, int]:
    return "foreign-lineage-source", os.getpid()


@ray.remote(num_cpus=0)
def _return_foreign_lineage_source() -> object:
    return _foreign_lineage_source.remote()


@ray.remote(num_cpus=1, max_retries=1)
def _foreign_lineage_consumer(
    value: tuple[str, int],
) -> tuple[str, int, str, bytes]:
    label, source_pid = value
    return label, source_pid, uuid.uuid4().hex, _PADDING


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
        raise TimeoutError("foreign lineage exceeded its shared deadline")
    return remaining


def _get_before(reference, deadline: float):
    value = ray.get(reference, timeout=_remaining(deadline))
    _remaining(deadline)
    return value


def _drop_before(reference, deadline: float) -> bool:
    _remaining(deadline)
    rpc_deadline = min(deadline, time.monotonic() + _RPC_TOTAL_TIMEOUT_SECONDS)
    parent_deadline = _RPC_CALL_DEADLINE.get()
    if parent_deadline is not None:
        rpc_deadline = min(rpc_deadline, parent_deadline)
    token = _RPC_CALL_DEADLINE.set(rpc_deadline)
    try:
        dropped = ray.drop_object(reference)
    finally:
        _RPC_CALL_DEADLINE.reset(token)
    _remaining(deadline)
    return dropped


def _close(reference, deadline: float) -> None:
    done = reference._release_done
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done is not None and done.is_set()


def _wait_local(core, predicate, deadline: float, detail: str) -> None:
    # Wakeups are observational. A missed notification can delay the next check,
    # but only the existing owner/recovery/receipt state can satisfy it.
    with core._completion:
        for _ in range(_MAX_POLLS):
            if predicate():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            core._completion.wait(min(_POLL_SECONDS, remaining))
        if not predicate():
            raise TimeoutError(detail)


def _consumer_ready(core, reference, foreign_id, foreign_owner, node_id, attempt_number):
    with core._state_lock:
        snapshot = core.owner_table.snapshot(reference.object_id)
        attempt = AttemptID(reference.object_id.task_id, attempt_number)
        assert snapshot.state is ObjectState.READY_STORED
        assert snapshot.current_attempt == attempt
        assert snapshot.locations == frozenset({node_id})
        assert snapshot.local_tokens and not snapshot.submitted_tokens
        assert snapshot.inline_data is None and snapshot.error is None
        assert not snapshot.collection_pending and snapshot.output_retirement_id is None
        descriptor = snapshot.canonical_stored_result
        assert descriptor is not None and descriptor.object_id == reference.object_id
        assert descriptor.storage is protocol.ResultStorage.OBJECT_STORE
        assert descriptor.owner_worker_id == core.worker_id and descriptor.node_id == node_id
        assert len(_PADDING) < descriptor.size_bytes < 16 * 1024
        spec = snapshot.producer_task_spec
        assert isinstance(spec, protocol.TaskSpec)
        assert spec.task_id == reference.object_id.task_id
        assert spec.owner_worker_id == core.worker_id and spec.max_retries == 1
        assert spec.args == (protocol.RefArg(foreign_id, foreign_owner),)
        assert not spec.kwargs and spec.return_ids() == (reference.object_id,)
        lineage = core._recovery.lineage_for_object(reference.object_id)
        assert lineage is not None and lineage.task_spec == spec
        record = core._recovery.task_record(reference.object_id.task_id)
        assert record.state is TaskState.SUCCEEDED and record.current_attempt == attempt
        assert record.retries_started == attempt_number and record.max_retries == 1
        assert record.retries_remaining == 1 - attempt_number
        assert core._recovery.active_recovery(reference.object_id.task_id) is None
    return snapshot


def _wait_collected(core, references, consumer_id, foreign_key, deadline: float) -> None:
    def collected() -> bool:
        return (
            all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
                for ref in references)
            and (foreign_key is None or foreign_key not in core._borrowed_release_obligations)
            and (consumer_id is None or (
                core._foreign_lineage_registry.snapshot(consumer_id.task_id) is None
                and consumer_id not in core._foreign_lineage_collection_receipts
                and consumer_id not in core._foreign_lineage_prepared_collection_receipts
            ))
        )

    _wait_local(core, collected, deadline, "foreign lineage collection exceeded observation bound")
    with core._state_lock:
        for reference in references:
            object_id = reference.object_id
            assert not core.owner_table.contains(object_id)
            assert core._recovery.lineage_for_object(object_id) is None
            assert core._recovery.active_recovery(object_id.task_id) is None
            assert not core.owner_table.task_lineage_edges(object_id.task_id)
            assert object_id not in core._objects and object_id not in core._stored_descriptors
            assert object_id not in core._task_finish_barriers
            assert object_id not in getattr(core, "_output_retirement_work", {})
        # The source Worker is alive. Registry completion therefore requires
        # the real final retained-hold release ACK, not an owner-death shortcut.
        if foreign_key is not None:
            assert not core._owner_is_dead(foreign_key[0])
            assert not core._foreign_lineage_registry.owner_is_dead(foreign_key[0])
        assert not core._accepted_task_count and not core._protocol_unresolved


def test_foreign_input_hold_replaced_before_consumer_reconstruction() -> None:
    context = core = owner_service = None
    report = None
    outer = foreign = consumer = None
    consumer_id = foreign_key = close_deadline = None
    original_borrow_rpc = None
    replacements: list[protocol.ReplaceRetainedObjectForTask] = []
    replacement_lock = threading.Lock()
    observation_overflow = threading.Event()
    observation_error = threading.Event()
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    cleanup_errors: list[Exception] = []
    try:
        context = ray.init(
            num_nodes=1, num_cpus=2, num_workers_per_node=2,
            inline_threshold=1024, object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        node = context.nodes[0]
        managed_pids.update(
            {context.gcs_pid, node.node_pid, *node.worker_pids}
        )
        managed_addresses.update(
            {context.gcs_address, node.node_address, *node.worker_addresses}
        )
        runtime = _get_runtime()
        core, owner_service = runtime.core_worker, runtime.owner_service
        if owner_service is not None:
            managed_addresses.add(owner_service.address)
        assert owner_service is not None and owner_service.is_running
        assert len(managed_pids) == 4 and os.getpid() not in managed_pids
        assert len(managed_addresses) == 5
        assert len(node.worker_ids) == len(node.worker_pids) == 2
        assert context.trace_address is None

        outer = _return_foreign_lineage_source.remote()
        foreign = _get_before(outer, deadline)
        assert isinstance(foreign, ray.ObjectRef)
        assert foreign.owner_worker_id in node.worker_ids
        assert foreign.owner_address in node.worker_addresses
        assert foreign.borrower_token is not None

        foreign_key = (foreign.owner_worker_id, foreign.object_id, core.worker_id, foreign.borrower_token)
        original_borrow_rpc = core._borrow_rpc

        def inspect_borrow(address, handler, message):
            try:
                if isinstance(message, protocol.ReplaceRetainedObjectForTask):
                    with replacement_lock:
                        if len(replacements) < _MAX_REPLACEMENTS:
                            replacements.append(message)
                        else:
                            observation_overflow.set()
            except Exception:
                observation_error.set()
            # Observation never raises into, redirects, or shortens the actual
            # owner RPC. Exact request replay and typed ACKs stay authoritative.
            return original_borrow_rpc(address, handler, message)

        core._borrow_rpc = inspect_borrow
        consumer = _foreign_lineage_consumer.remote(foreign)
        consumer_id = consumer.object_id
        foreign_id = foreign.object_id
        foreign_owner = foreign.owner_worker_id
        _close(foreign, deadline)
        _close(outer, deadline)
        assert foreign.closed and outer.closed

        first = _get_before(consumer, deadline)
        assert first[0] == "foreign-lineage-source"
        assert first[1] in node.worker_pids
        assert len(first[2]) == 32 and first[3] == _PADDING
        _wait_local(
            core, lambda: (
                consumer_id not in core._task_finish_barriers
                and core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED
                and foreign_key not in core._borrowed_release_obligations
            ), deadline, "foreign input release/consumer finish exceeded observation bound",
        )
        before = _consumer_ready(core, consumer, foreign_id, foreign_owner, node.node_id, 0)

        original_task_id = consumer_id.task_id
        with core._state_lock:
            initial_lineage = core._foreign_lineage_registry.snapshot(original_task_id)
        assert initial_lineage is not None
        assert initial_lineage.task_id == original_task_id
        assert initial_lineage.output_ids == (consumer_id,)
        assert len(initial_lineage.edges) == 1
        assert initial_lineage.edges[0].dependency_object_id == foreign_id
        assert initial_lineage.edges[0].owner_worker_id == foreign_owner
        assert initial_lineage.edges[0].owner_address == foreign.owner_address
        assert initial_lineage.edges[0].borrower_worker_id == core.worker_id
        assert initial_lineage.edges[0].roles == ForeignLineageRole.TOP_LEVEL
        assert initial_lineage.edges[0].hold.kind is protocol.TaskReferenceHoldKind.RETAINED
        assert initial_lineage.edges[0].hold.origin_attempt_id == AttemptID(
            original_task_id, 0
        )

        assert _drop_before(consumer, deadline)
        with core._state_lock:
            lost = core.owner_table.snapshot(consumer_id)
            assert lost.state is ObjectState.LOST and not lost.locations
            assert lost.current_attempt == before.current_attempt
            assert lost.producer_task_spec == before.producer_task_spec
            assert core._foreign_lineage_registry.snapshot(original_task_id) == initial_lineage
            record = core._recovery.task_record(original_task_id)
            assert record.current_attempt == before.current_attempt and record.retries_started == 0
            assert record.state is TaskState.SUCCEEDED and record.retries_remaining == 1
            assert core._recovery.active_recovery(original_task_id) is None
        second = _get_before(consumer, deadline)
        assert second[:2] == first[:2]
        assert second[2] != first[2]
        assert second[3] == _PADDING
        assert consumer.object_id == consumer_id
        _wait_local(
            core, lambda: consumer_id not in core._task_finish_barriers,
            deadline, "foreign reconstruction finish exceeded observation bound",
        )
        after = _consumer_ready(core, consumer, foreign_id, foreign_owner, node.node_id, 1)
        assert after.producer_task_spec == before.producer_task_spec

        with replacement_lock:
            observed = tuple(replacements)
        matching = tuple(
            request for request in observed
            if request.object_id == foreign_id
            and request.expected_hold.task_id == original_task_id
        )
        assert matching
        assert all(request == matching[0] for request in matching)
        replacement = matching[0]
        assert len(matching) == len(observed)
        assert replacement.owner_worker_id == foreign_owner
        assert replacement.borrower_worker_id == core.worker_id
        assert replacement.expected_hold == initial_lineage.edges[0].hold
        assert replacement.replacement_hold.kind is protocol.TaskReferenceHoldKind.RETAINED
        assert replacement.replacement_hold.submitting_worker_id == core.worker_id
        assert replacement.replacement_hold.task_id == original_task_id
        assert replacement.expected_hold.origin_attempt_id == AttemptID(
            original_task_id, 0
        )
        assert replacement.replacement_hold.origin_attempt_id == AttemptID(
            original_task_id, 1
        )

        with core._state_lock:
            renewed = core._foreign_lineage_registry.snapshot(original_task_id)
        assert renewed is not None
        assert renewed.task_id == original_task_id and renewed.output_ids == (consumer_id,)
        assert len(renewed.edges) == 1
        assert renewed.edges[0].dependency_object_id == foreign_id
        assert renewed.edges[0].owner_worker_id == foreign_owner
        assert renewed.edges[0].owner_address == initial_lineage.edges[0].owner_address
        assert renewed.edges[0].borrower_worker_id == core.worker_id
        assert renewed.edges[0].roles == initial_lineage.edges[0].roles
        assert renewed.edges[0].hold == replacement.replacement_hold
        assert not observation_overflow.is_set() and not observation_error.is_set()
        _remaining(deadline)
        close_deadline = time.monotonic() + _CLEANUP_SECONDS
        _close(consumer, close_deadline)
        assert consumer.closed
        _wait_collected(core, (consumer, outer), consumer_id, foreign_key, close_deadline)
    finally:
        if close_deadline is None:
            close_deadline = time.monotonic() + _CLEANUP_SECONDS
        references = tuple(ref for ref in (consumer, foreign, outer) if isinstance(ref, ray.ObjectRef))
        try:
            for reference in references:
                try:
                    _close(reference, close_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
            if core is not None and references:
                try:
                    local = tuple(ref for ref in references if ref.owner_worker_id == core.worker_id)
                    _wait_collected(core, local, consumer_id, foreign_key, close_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
        finally:
            try:
                if core is not None and original_borrow_rpc is not None:
                    core._borrow_rpc = original_borrow_rpc
            finally:
                try:
                    report = ray.shutdown()
                finally:
                    surviving_pids = tuple(pid for pid in managed_pids if _pid_exists(pid))
                    surviving_children = tuple(
                        child.pid for child in mp.active_children() if child.pid in managed_pids
                    )
                    open_addresses = []
                    for address in managed_addresses:
                        try:
                            with socket.create_connection(address, timeout=0.1):
                                open_addresses.append(address)
                        except OSError:
                            pass
                    assert not surviving_pids, surviving_pids
                    assert not surviving_children, surviving_children
                    assert not open_addresses, open_addresses
                    assert not ray.is_initialized()
                    assert owner_service is None or not owner_service.is_running
                    assert not cleanup_errors, cleanup_errors
                    assert not observation_overflow.is_set() and not observation_error.is_set()
                    if context is not None:
                        assert report is not None and report.core_stopped
                        assert report.gcs_pid == context.gcs_pid
                        assert report.gcs_clean and report.gcs_exitcode == 0 and not report.gcs_forced
                        assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
                        assert report.node_exitcodes == (0,) and report.worker_exitcodes == (0, 0)
                        assert report.node_clean and report.worker_clean
                        assert report.resources_clean and report.finalized
                        assert report.shutdown_ack_clean and not report.forced
