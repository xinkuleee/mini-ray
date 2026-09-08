"""Bounded whole-manifest multi-return reconstruction smoke.

One two-return producer publishes two heterogeneous store-backed values.  The
test drops the sole replica of each sibling, asks for return slot one, and
observes one TaskID-scoped START plus a deterministic slot-zero JOIN before the
replay is queued.  Reconstruction must execute the producer once, republish the
complete immutable manifest at attempt one, and preserve both ObjectIDs.

Run only this exact node ID through ``scripts/run_bounded_test.py``.  Static
bounds are one GCS, one Node, one ordinary Worker, one logical task, two
physical attempts, two result objects of at most 32 KiB, a 1 MiB ObjectStore,
no tracing or sleeps, and the runner's 30-second hard deadline. Two explicit
drop injections are an explicitly reviewed composite setup, not one fault;
old-replica retirement replays and final new-replica GC are additional calls.
Gets/drop/finish observations share a fifteen-second waiting budget, not
cancellation of synchronous reconstruction RPCs; sibling close/GC shares one
final three-second epoch. Three child PIDs/four endpoints are checked in failure
finally. START/JOIN records are capped; the original two-byte invocation
journal and one injected sibling request at the real commit boundary remain
explicit; the observed JOIN is the coordinator's real result.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
from pathlib import Path
from threading import Lock
import time

import pytest

import miniray as ray
from miniray.api import _get_runtime
from miniray.core import _RPC_CALL_DEADLINE, _RPC_TOTAL_TIMEOUT_SECONDS
from miniray.ids import AttemptID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.reconstruction_runtime import (
    PreparedReconstruction,
    ReconstructionDisposition,
    ReconstructionOutcome,
)
from miniray.recovery import UnknownTaskError


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_POLLS = 256
_MAX_OBSERVATIONS = 8
_OBJECT_STORE_BYTES = 1024 * 1024
_LEFT_BYTES = 16 * 1024
_RIGHT_BYTES = 24 * 1024


@ray.remote(num_returns=2, max_retries=1)
def _produce_store_backed_pair(counter_path: str) -> tuple[bytes, dict[str, str]]:
    # The two-byte bounded journal is external to the Worker process, so it
    # counts user-code invocations rather than owner publications or RPCs.
    with open(counter_path, "ab") as counter:
        counter.write(b"X")
    return (
        b"L" * _LEFT_BYTES,
        {"kind": "mapping", "payload": "R" * _RIGHT_BYTES},
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
        raise TimeoutError("whole-manifest reconstruction exceeded its deadline")
    return remaining


def _close(reference, deadline: float) -> None:
    done = reference._release_done
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done is not None and done.is_set()


def _drop(reference, deadline: float) -> bool:
    limit = min(deadline, time.monotonic() + _RPC_TOTAL_TIMEOUT_SECONDS)
    enclosing = _RPC_CALL_DEADLINE.get()
    if enclosing is not None:
        limit = min(limit, enclosing)
    token = _RPC_CALL_DEADLINE.set(limit)
    try:
        return ray.drop_object(reference)
    finally:
        _RPC_CALL_DEADLINE.reset(token)


def _wait_for(core, predicate, deadline: float) -> None:
    with core._completion:
        for _ in range(_MAX_POLLS):
            if predicate():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            core._completion.wait(min(0.05, remaining))
        assert predicate(), "whole-manifest state observation did not converge"


def _drain_reference_events(core: object, object_ids, deadline: float) -> None:
    """Observe actual collection, not unbounded/transient Queue.join."""
    _wait_for(core, lambda: all(
        core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
        for object_id in object_ids
    ), deadline)


def test_multi_return_all_outputs_lost_reconstructs_once_from_nonzero_sibling(
    tmp_path: Path,
) -> None:
    context = None
    report = None
    core = None
    coordinator = None
    original_commit = None
    cleanup_deadline = None
    cleanup_errors = []
    observation_overflow = False
    output_refs: tuple[ray.ObjectRef, ...] = ()
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    counter_path = tmp_path / "producer-invocations.bin"
    try:
        context = ray.init(
            num_nodes=1,
            num_cpus=1,
            num_workers_per_node=1,
            inline_threshold=1024,
            object_store_bytes=_OBJECT_STORE_BYTES,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        assert context.trace_address is None
        managed_pids.update(
            {context.gcs_pid, context.node_pid, context.worker_pid}
        )
        managed_addresses.update(
            {
                context.gcs_address,
                context.node_address,
                context.worker_address,
                runtime.owner_service.address,
            }
        )
        assert len(managed_pids) == 3
        assert len(managed_addresses) == 4
        assert os.getpid() not in managed_pids

        submitted = _produce_store_backed_pair.remote(str(counter_path))
        output_refs = tuple(ref for ref in (submitted if isinstance(submitted, tuple) else (submitted,))
                            if isinstance(ref, ray.ObjectRef))
        assert isinstance(submitted, tuple) and len(submitted) == 2
        assert output_refs == submitted
        left_ref, right_ref = output_refs
        output_ids = tuple(ref.object_id for ref in output_refs)
        task_id = output_ids[0].task_id
        assert tuple(object_id.task_id for object_id in output_ids) == (
            task_id, task_id
        )
        assert tuple(object_id.return_index for object_id in output_ids) == (0, 1)

        expected = [
            b"L" * _LEFT_BYTES,
            {"kind": "mapping", "payload": "R" * _RIGHT_BYTES},
        ]
        assert ray.get(output_refs, timeout=_remaining(deadline)) == expected
        assert counter_path.read_bytes() == b"X"
        _wait_for(core, lambda: not core._task_finish_barriers and core._accepted_task_count == 0, deadline)

        initial_attempt = AttemptID(task_id, 0)
        initial_snapshots = tuple(
            core.owner_table.snapshot(object_id) for object_id in output_ids
        )
        assert all(
            snapshot.state is ObjectState.READY_STORED
            and snapshot.current_attempt == initial_attempt
            and snapshot.producer_task_spec is not None
            and snapshot.producer_task_spec.return_ids() == output_ids
            for snapshot in initial_snapshots
        )
        old_descriptors = tuple(
            core._stored_descriptors[object_id] for object_id in output_ids
        )

        # A whole-manifest replay is admitted only after every sibling is LOST.
        # The intermediate mixed state is observed but never passed to get/wait.
        assert _drop(left_ref, deadline)
        assert core.owner_table.snapshot(
            output_ids[0]
        ).state is ObjectState.LOST
        assert core.owner_table.snapshot(
            output_ids[1]
        ).state is ObjectState.READY_STORED
        assert core._recovery.task_record(task_id).retries_started == 0

        assert _drop(right_ref, deadline)
        lost_snapshots = tuple(
            core.owner_table.snapshot(object_id) for object_id in output_ids
        )
        assert all(
            snapshot.state is ObjectState.LOST
            and snapshot.current_attempt == initial_attempt
            and snapshot.locations == frozenset()
            for snapshot in lost_snapshots
        )
        assert core._recovery.task_record(task_id).retries_started == 0

        # Instrument only the pure START/JOIN authority.  Slot one remains the
        # public get trigger; immediately after its START transition, slot zero
        # performs a subsequent JOIN while the shared session is active.  This
        # avoids a timing race and cannot enqueue a second physical execution.
        coordinator = core._reconstruction_coordinator()
        original_commit = coordinator.commit_prepared
        observed: list[tuple[object, ReconstructionOutcome]] = []
        observed_lock = Lock()

        def observe_commit(prepared: PreparedReconstruction) -> ReconstructionOutcome:
            nonlocal observation_overflow
            outcome = original_commit(prepared)
            object_id = (
                outcome.plan.requested_object_id
                if outcome.plan is not None
                else outcome.decision.requested_object_id
            )
            with observed_lock:
                if len(observed) < _MAX_OBSERVATIONS:
                    observed.append((object_id, outcome))
                else:
                    observation_overflow = True
            if (
                object_id == output_ids[1]
                and outcome.disposition is ReconstructionDisposition.START
            ):
                # request itself invokes this commit wrapper. Its real JOIN
                # is recorded by that nested call, never appended twice.
                coordinator.request(output_ids[0])
            return outcome

        coordinator.commit_prepared = observe_commit

        assert ray.get(right_ref, timeout=_remaining(deadline)) == expected[1]
        assert ray.get(output_refs, timeout=_remaining(deadline)) == expected
        _wait_for(core, lambda: not core._task_finish_barriers and core._accepted_task_count == 0, deadline)
        assert tuple(ref.object_id for ref in output_refs) == output_ids
        assert counter_path.read_bytes() == b"XX"

        with observed_lock:
            decisions = tuple(observed)
        assert not observation_overflow
        assert tuple(item[0] for item in decisions) == (
            output_ids[1], output_ids[0]
        )
        assert tuple(item[1].disposition for item in decisions) == (
            ReconstructionDisposition.START, ReconstructionDisposition.JOIN
        )
        reconstructed_attempt = initial_attempt.next()
        assert tuple(item[1].decision.attempt_id for item in decisions) == (
            reconstructed_attempt, reconstructed_attempt
        )
        assert all(
            item[1].decision.output_ids == output_ids for item in decisions
        )

        reconstructed_snapshots = tuple(
            core.owner_table.snapshot(object_id) for object_id in output_ids
        )
        assert all(
            snapshot.state is ObjectState.READY_STORED
            and snapshot.current_attempt == reconstructed_attempt
            and snapshot.locations == frozenset({context.node_id})
            and snapshot.producer_task_spec is not None
            and snapshot.producer_task_spec.return_ids() == output_ids
            for snapshot in reconstructed_snapshots
        )
        new_descriptors = tuple(
            core._stored_descriptors[object_id] for object_id in output_ids
        )
        assert tuple(value.object_id for value in new_descriptors) == output_ids
        assert all(
            new is not old
            for old, new in zip(old_descriptors, new_descriptors)
        )
        record = core._recovery.task_record(task_id)
        assert record.current_attempt == reconstructed_attempt
        assert record.retries_started == 1
        assert core._recovery.active_recovery(task_id) is None
        assert task_id not in coordinator._sessions

        # Lineage is task-scoped: collecting one return does not erase the
        # producer record needed by its live sibling; the final sibling owns
        # the exactly-once manifest/lineage release.
        _remaining(deadline)
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        _close(right_ref, cleanup_deadline)
        assert right_ref.closed and not left_ref.closed
        _drain_reference_events(core, (output_ids[1],), cleanup_deadline)
        assert core.owner_table.collection_state(
            output_ids[1]
        ) is ObjectCollectionState.COLLECTED
        assert not core.owner_table.contains(output_ids[1])
        surviving_lineage = core._recovery.lineage_for_object(output_ids[0])
        assert surviving_lineage is not None
        assert surviving_lineage.task_id == task_id
        assert surviving_lineage.output_ids == output_ids
        assert core._recovery.task_record(
            task_id
        ).current_attempt == reconstructed_attempt

        _close(left_ref, cleanup_deadline)
        assert left_ref.closed
        _drain_reference_events(core, output_ids, cleanup_deadline)
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
            if coordinator is not None and original_commit is not None:
                coordinator.commit_prepared = original_commit
            for ref in output_refs:
                try:
                    _close(ref, cleanup_deadline)
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
                assert not observation_overflow

    assert context is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.node_exitcode == 0
    assert report.worker_clean and report.worker_exitcode == 0
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
