"""Bounded three-level recursive lineage reconstruction smoke.

All three single-return results are forced into the local ObjectStore.  Their
only replicas are dropped while the public ObjectRefs and producer lineage stay
live; one final ``get(root)`` must therefore reconstruct leaf, middle, then root
with new physical attempts while preserving every logical ObjectID.

This is an explicitly reviewed composite experiment, not a single-fault smoke:
one two-CPU Node, two ordinary Workers, three physical drops, and one
reconstruction per producer (three initial plus three reconstructed executions).
Its exact two-CPU/three-drop/three-reconstruction exception is included in
the registered smoke. Run it through ``scripts/run_baseline.py --smoke EXACT``.

Four managed children, five runtime/owner endpoints, one 1 MiB store and three
tiny stored objects bound the workload. All get/drop/finish observations share
fifteen seconds after init; public closes and owner/lineage GC share three
seconds in finally, before unconditional shutdown and exact PID/port checks.
There is no Actor, tracing, GPU, external network, extra listener or test thread.
Startup/shutdown retain their own contracts under the 30-second tree runner.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.core import _RPC_CALL_DEADLINE, _RPC_TOTAL_TIMEOUT_SECONDS
from miniray.ids import AttemptID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.recovery import TaskState


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("recursive lineage exceeded its shared deadline")
    return remaining


def _drop_before(reference, deadline: float) -> bool:
    # Bound the existing public operation's real RPC, without inventing a
    # timeout-as-cancellation result or resetting the experiment's budget.
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


def _wait_finished(core, references, deadline: float) -> None:
    # READY alone precedes the final publication/adoption accounting barrier.
    with core._completion:
        for _ in range(256):
            if not any(ref.object_id in core._task_finish_barriers for ref in references):
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            core._completion.wait(min(0.1, remaining))
        # Observe authority once more after the final wake, including when the
        # transition completed without notifying this diagnostic waiter.
        if any(ref.object_id in core._task_finish_barriers for ref in references):
            raise TimeoutError("recursive lineage finish barriers exceeded observation bound")


def _ready_lineage(core, references, node_id, attempt_number: int):
    """Read the actual owner and recovery authorities, never drive them."""

    logical_ids = tuple(ref.object_id for ref in references)
    with core._state_lock:
        snapshots = tuple(core.owner_table.snapshot(object_id) for object_id in logical_ids)
        for index, (reference, snapshot) in enumerate(zip(references, snapshots)):
            object_id = logical_ids[index]
            attempt = AttemptID(object_id.task_id, attempt_number)
            assert reference.owner_worker_id == core.worker_id
            assert snapshot.current_attempt == attempt
            assert snapshot.state is ObjectState.READY_STORED
            assert snapshot.locations == frozenset({node_id})
            assert snapshot.local_tokens and not snapshot.submitted_tokens
            assert snapshot.inline_data is None and snapshot.error is None
            assert not snapshot.collection_pending and snapshot.output_retirement_id is None
            descriptor = snapshot.canonical_stored_result
            assert descriptor is not None and descriptor.object_id == object_id
            assert descriptor.storage is protocol.ResultStorage.OBJECT_STORE
            assert descriptor.owner_worker_id == core.worker_id and descriptor.node_id == node_id
            assert 1 < descriptor.size_bytes <= 1024
            spec = snapshot.producer_task_spec
            assert isinstance(spec, protocol.TaskSpec)
            assert spec.task_id == object_id.task_id and spec.return_ids() == (object_id,)
            assert spec.max_retries == 1 and spec.owner_worker_id == core.worker_id
            expected_args = (() if index == 0 else
                             (protocol.RefArg(logical_ids[index - 1], core.worker_id),))
            assert spec.args == expected_args and not spec.kwargs
            assert {edge.dependency_object_id for edge in snapshot.outgoing_lineage_edges} == (
                set() if index == 0 else {logical_ids[index - 1]}
            )
            assert len(snapshot.lineage_tokens) == (0 if index == 2 else 1)
            lineage = core._recovery.lineage_for_object(object_id)
            assert lineage is not None and lineage.task_spec == spec
            assert lineage.task_id == object_id.task_id and lineage.output_ids == (object_id,)
            record = core._recovery.task_record(object_id.task_id)
            assert record.state is TaskState.SUCCEEDED and record.current_attempt == attempt
            assert record.max_retries == 1 and record.retries_started == attempt_number
            assert record.retries_remaining == 1 - attempt_number
            assert core._recovery.active_recovery(object_id.task_id) is None
        assert core._accepted_task_count == 0 and not core._protocol_unresolved
    return snapshots


def _close(reference, deadline: float) -> None:
    done = reference._release_done
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done is not None and done.is_set()


def _wait_collected(core, references, deadline: float) -> None:
    with core._completion:
        for _ in range(256):
            if all(
                core.owner_table.collection_state(ref.object_id)
                is ObjectCollectionState.COLLECTED
                for ref in references
            ):
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            core._completion.wait(min(0.1, remaining))
        # Elapsed waiting is never a collection receipt. Missing notifications
        # do not prevent a bounded re-read of the actual owner GC state.
        if any(
            core.owner_table.collection_state(ref.object_id)
            is not ObjectCollectionState.COLLECTED
            for ref in references
        ):
            raise TimeoutError("recursive lineage collection exceeded observation bound")
        for reference in references:
            object_id = reference.object_id
            assert not core.owner_table.contains(object_id)
            assert not core.owner_table.task_lineage_edges(object_id.task_id)
            assert core._recovery.lineage_for_object(object_id) is None
            assert core._recovery.active_recovery(object_id.task_id) is None
            assert object_id not in core._objects and object_id not in core._stored_descriptors
            assert object_id not in core._task_finish_barriers
            assert object_id not in getattr(core, "_output_retirement_work", {})


@ray.remote(max_retries=1)
def _recursive_leaf() -> tuple[str, ...]:
    return ("leaf",)


@ray.remote(max_retries=1)
def _recursive_middle(value: tuple[str, ...]) -> tuple[str, ...]:
    return value + ("middle",)


@ray.remote(max_retries=1)
def _recursive_root(value: tuple[str, ...]) -> tuple[str, ...]:
    return value + ("root",)


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_recursive_lineage_reconstructs_leaf_to_root() -> None:
    context = core = owner_service = None
    report = None
    leaf = middle = root = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    cleanup_errors: list[Exception] = []
    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=2,
            num_workers_per_node=2,
            inline_threshold=1,
            object_store_bytes=1024 * 1024,
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

        leaf = _recursive_leaf.remote()
        middle = _recursive_middle.remote(leaf)
        root = _recursive_root.remote(middle)
        references = (leaf, middle, root)
        expected = ("leaf", "middle", "root")
        assert ray.get(root, timeout=_remaining(deadline)) == expected
        _wait_finished(core, references, deadline)

        logical_ids = (leaf.object_id, middle.object_id, root.object_id)
        assert len(set(logical_ids)) == 3
        before = _ready_lineage(core, references, node.node_id, 0)

        # drop_object does not start reconstruction.  Dropping dependency-first
        # leaves the complete DAG LOST before the one root get asks the owner
        # coordinator for a dependency-first recursive plan.
        assert _drop_before(leaf, deadline)
        assert _drop_before(middle, deadline)
        assert _drop_before(root, deadline)
        with core._state_lock:
            for reference, original in zip(references, before):
                lost = core.owner_table.snapshot(reference.object_id)
                assert lost.state is ObjectState.LOST and not lost.locations
                assert lost.current_attempt == original.current_attempt
                assert lost.producer_task_spec == original.producer_task_spec
                assert lost.lineage_tokens == original.lineage_tokens
                assert lost.outgoing_lineage_edges == original.outgoing_lineage_edges
                record = core._recovery.task_record(reference.object_id.task_id)
                assert record.state is TaskState.SUCCEEDED
                assert record.current_attempt == original.current_attempt
                assert record.retries_started == 0 and record.retries_remaining == 1
                assert core._recovery.active_recovery(reference.object_id.task_id) is None

        # No leaf/middle get can recover them first: this single root get must
        # traverse and reconstruct all three LOST producers through real K1.
        assert ray.get(root, timeout=_remaining(deadline)) == expected
        _wait_finished(core, references, deadline)
        assert (leaf.object_id, middle.object_id, root.object_id) == logical_ids
        after = _ready_lineage(core, references, node.node_id, 1)
        for original, reconstructed in zip(before, after):
            assert reconstructed.producer_task_spec == original.producer_task_spec
            assert reconstructed.lineage_tokens == original.lineage_tokens
            assert reconstructed.outgoing_lineage_edges == original.outgoing_lineage_edges
        assert ray.get(leaf, timeout=_remaining(deadline)) == ("leaf",)
        assert ray.get(middle, timeout=_remaining(deadline)) == (
            "leaf", "middle"
        )
        _remaining(deadline)
    finally:
        close_deadline = time.monotonic() + _CLEANUP_SECONDS
        live_references = tuple(ref for ref in (root, middle, leaf) if ref is not None)
        try:
            for ref in live_references:
                try:
                    _close(ref, close_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
            if core is not None and live_references:
                try:
                    _wait_collected(core, live_references, close_deadline)
                except Exception as exc:
                    cleanup_errors.append(exc)
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
                if context is not None:
                    assert report is not None and report.core_stopped
                    assert report.gcs_pid == context.gcs_pid
                    assert report.gcs_clean and report.gcs_exitcode == 0
                    assert report.node_pids == context.node_pids
                    assert report.worker_pids == context.worker_pids
                    assert report.node_exitcodes == (0,)
                    assert report.worker_exitcodes == (0, 0)
                    assert report.node_clean and report.worker_clean
                    assert report.resources_clean and report.finalized
                    assert report.shutdown_ack_clean and not report.forced
                    assert not report.gcs_forced
