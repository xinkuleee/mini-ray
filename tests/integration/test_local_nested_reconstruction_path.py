"""Bounded local nested-handle lineage reconstruction smoke.

``ray.put`` creates A and one ordinary task B receives A only inside an
inline container.  B's stored result loses its sole replica and is replayed
with the same ObjectID.  The smoke proves that A is a lifetime handle rather
than a readiness/DFS edge, while the replay still imports and dereferences A
through a fresh task hold and a fresh physical-attempt borrower.

Run only this exact node ID through ``scripts/run_bounded_test.py``.  Static
bounds are one GCS, one NodeManager, one ordinary Worker, one ordinary task
with one reconstruction attempt, a 1 MiB store, tiny values, no Actor or
tracing, no test-owned listener, and no sleeps. Driver operations/observations
share fifteen seconds after init; the original Worker get keeps its finite
ten-second timeout. The early source close stays within that work budget;
final public closes and owner GC share three seconds before unconditional
shutdown and three-PID/four-endpoint checks. The nested container is INLINE;
2 KiB source/result padding keeps both actual values store-backed.
At most eight Acquire records and 256 passive checks per observation are kept.
The outer 30-second runner bounds calls whose internal RPC policy is unchanged.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.core import _RPC_CALL_DEADLINE, _RPC_TOTAL_TIMEOUT_SECONDS, _lineage_hold_token
from miniray.ids import AttemptID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.reconstruction_runtime import ReconstructionGraphAction


pytestmark = pytest.mark.multiprocess_smoke

_TIMEOUT_SECONDS = 10.0
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_POLLS = 256
_MAX_ACQUISITIONS = 8
_POLL_SECONDS = 0.01
_INLINE_THRESHOLD = 1024
_SOURCE_VALUE = ("local-nested-reconstruction", 41, b"s" * 2048)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("local nested reconstruction exceeded its work deadline")
    return remaining


def _close(reference, deadline: float) -> None:
    if reference is None:
        return
    done = reference._release_done
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done is not None and done.is_set()


def _drop_before(reference, deadline: float) -> bool:
    _remaining(deadline)
    limit = min(deadline, time.monotonic() + _RPC_TOTAL_TIMEOUT_SECONDS)
    parent = _RPC_CALL_DEADLINE.get()
    if parent is not None:
        limit = min(limit, parent)
    token = _RPC_CALL_DEADLINE.set(limit)
    try:
        return ray.drop_object(reference)
    finally:
        _RPC_CALL_DEADLINE.reset(token)


@ray.remote(num_cpus=1, max_retries=1)
def _read_nested_handle(container: object) -> tuple[object, str, int, bytes]:
    nested = container["nested"]  # type: ignore[index]
    return (
        ray.get(nested, timeout=_TIMEOUT_SECONDS),
        str(nested.object_id),
        os.getpid(),
        b"r" * 2048,
    )


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _await_source_quiescent(core: object, object_id: object, deadline: float) -> object:
    """Wait for the attempt hold/borrower to retire, with no blind sleep."""

    wake = threading.Event()
    for _ in range(_MAX_POLLS):
        snapshot = core.owner_table.snapshot(object_id)
        if not snapshot.submitted_tokens and not snapshot.borrowed_tokens:
            return snapshot
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return snapshot
        wake.wait(min(_POLL_SECONDS, remaining))
    return snapshot


def _await_source_handle_release(core: object, object_id: object, deadline: float) -> object:
    """Wait until only lineage, rather than the Python source handle, owns A."""

    wake = threading.Event()
    for _ in range(_MAX_POLLS):
        snapshot = core.owner_table.snapshot(object_id)
        if not snapshot.local_tokens:
            return snapshot
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return snapshot
        wake.wait(min(_POLL_SECONDS, remaining))
    return snapshot


def _await_collection(core: object, object_id: object, deadline: float) -> ObjectCollectionState:
    wake = threading.Event()
    for _ in range(_MAX_POLLS):
        state = core.owner_table.collection_state(object_id)
        if state is ObjectCollectionState.COLLECTED:
            return state
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return state
        wake.wait(min(_POLL_SECONDS, remaining))
    return state


def test_local_nested_handle_survives_single_return_reconstruction() -> None:
    context = None
    source = result = None
    report = None
    core = None
    original_acquire = None
    acquisitions: list[tuple[bool, object, object, object]] = []
    acquisition_lock = threading.Lock()
    acquisition_error = threading.Event()
    acquisition_overflow = threading.Event()
    cleanup_errors = []
    close_deadline = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    source_id = result_id = None
    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=1,
            num_workers_per_node=1,
            inline_threshold=_INLINE_THRESHOLD,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        node = context.nodes[0]
        managed_pids.update(
            {context.gcs_pid, node.node_pid, node.worker_pid}
        )
        managed_addresses.update(
            {context.gcs_address, node.node_address, node.worker_address}
        )
        assert len(managed_pids) == 3
        assert len(node.worker_ids) == 1
        assert context.trace_address is None

        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_addresses) == 4 and os.getpid() not in managed_pids

        source = ray.put(_SOURCE_VALUE)
        source_id = source.object_id
        source_initial = core.owner_table.snapshot(source_id)
        assert source_initial.state is ObjectState.READY_STORED
        assert source_initial.producer_task_spec is None
        assert source_initial.current_attempt == AttemptID(source_id.task_id, 0)
        assert source_initial.canonical_stored_result.size_bytes > _INLINE_THRESHOLD
        original_acquire = core.owner_table.acquire_exported_reference

        def inspect_acquire(
            object_id: object, source_credential: object, borrower: object
        ) -> bool:
            changed = original_acquire(
                object_id, source_credential, borrower
            )
            if object_id == source_id and isinstance(
                source_credential, protocol.TaskHoldSource
            ):
                try:
                    snapshot = core.owner_table.snapshot(source_id)
                    with acquisition_lock:
                        if len(acquisitions) < _MAX_ACQUISITIONS:
                            acquisitions.append((changed, source_credential.hold, borrower, snapshot))
                        else:
                            acquisition_overflow.set()
                except Exception:
                    acquisition_error.set()
            return changed

        core.owner_table.acquire_exported_reference = inspect_acquire

        result = _read_nested_handle.remote({"nested": source})
        result_id = result.object_id
        # The container itself must stay inline for this negative DFS proof.
        # A threshold of one lifts it to a StoredArg whose real put dependency
        # correctly appears as READY_SKIP; that is a different teaching case.
        spec = core.owner_table.snapshot(result_id).producer_task_spec
        assert spec is not None and len(spec.args) == 1
        assert type(spec.args[0]) is protocol.InlineArg
        assert sum(len(argument.data) for argument in spec.args) <= _INLINE_THRESHOLD
        transfer, = spec.args[0].nested_refs
        assert transfer.object_id == source_id and transfer.owner_worker_id == core.worker_id
        expected = (
            _SOURCE_VALUE, str(source_id), node.worker_pid,
            b"r" * 2048,
        )
        assert ray.get(result, timeout=_remaining(deadline)) == expected
        assert core.owner_table.snapshot(result_id).state is ObjectState.READY_STORED
        assert core.owner_table.snapshot(result_id).current_attempt == AttemptID(
            result_id.task_id, 0
        )
        assert core.owner_table.snapshot(result_id).canonical_stored_result.size_bytes > _INLINE_THRESHOLD

        lineage_token = _lineage_hold_token(result_id.task_id, source_id)
        source_snapshot = _await_source_quiescent(core, source_id, deadline)
        assert source_snapshot.submitted_tokens == frozenset()
        assert source_snapshot.borrowed_tokens == frozenset()
        assert lineage_token in source_snapshot.lineage_tokens

        # Canonical B lineage, not this Python handle, must preserve A for the
        # later replay.  The reconstructed Worker will import a new handle.
        _close(source, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        assert source.closed
        source_snapshot = _await_source_handle_release(core, source_id, deadline)
        assert source_snapshot.local_tokens == frozenset()
        assert lineage_token in source_snapshot.lineage_tokens
        assert core.owner_table.contains(source_id)

        original_result_id = result.object_id
        assert _drop_before(result, deadline)
        assert core.owner_table.snapshot(result_id).state is ObjectState.LOST

        # This pure preflight is the direct negative proof: even though A is
        # present in B's nested manifest, only B occurs in the DFS.
        graph = core._reconstruction.preflight_graph(result_id)
        assert tuple(node.object_id for node in graph.nodes) == (result_id,)
        assert tuple(node.object_id for node in graph.steps) == (result_id,)
        root = graph.node_for(result_id)
        assert root.action is ReconstructionGraphAction.LOST_RECONSTRUCT
        assert root.dependency_ids == ()
        assert root.nested_local_holds == (source_id,)

        assert ray.get(result, timeout=_remaining(deadline)) == expected
        assert result.object_id == original_result_id
        reconstructed_attempt = AttemptID(result_id.task_id, 1)
        result_snapshot = core.owner_table.snapshot(result_id)
        assert result_snapshot.state is ObjectState.READY_STORED
        assert result_snapshot.current_attempt == reconstructed_attempt

        source_snapshot = _await_source_quiescent(core, source_id, deadline)
        assert source_snapshot.submitted_tokens == frozenset()
        assert source_snapshot.borrowed_tokens == frozenset()
        assert lineage_token in source_snapshot.lineage_tokens

        # The owner admission is linearized once for each physical attempt.
        # Each import must carry that attempt's hold origin and the Worker's
        # exact attempt-scoped borrower token.
        with acquisition_lock:
            observed = tuple(acquisitions)
        assert len(observed) == 2
        by_origin = {
            hold.origin_attempt_id: item
            for item in observed
            for hold in (item[1],)
        }
        initial_attempt = AttemptID(result_id.task_id, 0)
        assert set(by_origin) == {initial_attempt, reconstructed_attempt}
        for attempt_id, (changed, hold, borrower, snapshot) in by_origin.items():
            assert changed
            assert hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
            assert hold.submitting_worker_id == core.worker_id
            assert hold.task_id == result_id.task_id
            assert hold.origin_attempt_id == attempt_id
            expected_borrower = (
                node.worker_id,
                "task-borrow:{}:{}:{}:{}".format(
                    attempt_id, node.worker_id, core.worker_id, source_id
                ),
            )
            assert borrower == expected_borrower
            assert hold in snapshot.submitted_tokens
            assert borrower in snapshot.borrowed_tokens
            assert (
                borrower, protocol.TaskHoldSource(hold)
            ) in snapshot.borrowed_sources
        assert by_origin[initial_attempt][1] != by_origin[
            reconstructed_attempt
        ][1]
        assert {item[2] for item in by_origin.values()} <= (
            source_snapshot.released_borrowed_tokens
        )

        # Releasing B removes its lineage edge; A was already handle-free, so
        # both logical metadata entries and both stored replicas must converge.
        _remaining(deadline)
        close_deadline = time.monotonic() + _CLEANUP_SECONDS
        _close(result, close_deadline)
        assert result.closed
        assert _await_collection(
            core, result_id, close_deadline
        ) is ObjectCollectionState.COLLECTED
        assert _await_collection(
            core, source_id, close_deadline
        ) is ObjectCollectionState.COLLECTED
        for object_id in (result_id, source_id):
            assert not core.owner_table.contains(object_id)
            assert object_id not in core._stored_descriptors
            assert object_id not in core._objects
            assert object_id not in core._object_gc_obligations
        assert core._recovery.lineage_for_object(result_id) is None
        assert not acquisition_error.is_set() and not acquisition_overflow.is_set()
    finally:
        if close_deadline is None:
            close_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if core is not None and original_acquire is not None:
                core.owner_table.acquire_exported_reference = original_acquire
            for ref in (result, source):
                try:
                    _close(ref, close_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                if report is not None:
                    managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
                surviving_pids = tuple(pid for pid in managed_pids if _pid_exists(pid))
                surviving_children = tuple(child.pid for child in mp.active_children() if child.pid in managed_pids)
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
                assert not cleanup_errors, cleanup_errors
                assert not acquisition_error.is_set() and not acquisition_overflow.is_set()

    assert context is not None
    assert source_id is not None and result_id is not None
    assert report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.node_exitcode == 0
    assert report.worker_clean and report.worker_exitcode == 0
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
