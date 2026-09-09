"""Finite pure foreign-input renewal/collection contracts for one output.

Two distinct input owners/roles remain independent. RPC callbacks supply
explicit reducer replies; collection receipts activate only after real local
owner and RecoveryManager commits. No runtime constructor, network, thread,
process, timer or wait is used.
"""

from __future__ import annotations

from dataclasses import dataclass
from threading import RLock

import pytest

from miniray.foreign_lineage import (
    ForeignLineageCollectionReceipt, ForeignLineageEdge,
    ForeignLineagePreparedCollectionReceipt, ForeignLineageRegistry,
    ForeignLineageRole,
)
from miniray.foreign_lineage_runtime import (
    ForeignLineageCollectionDisposition,
    ForeignLineageRenewalDisposition,
    ForeignLineageRuntime,
    ForeignLineageRuntimeError,
)
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.protocol import (
    GetRetainedOwnedObjectReply, OwnedObjectReconstructionDisposition,
    OwnedObjectReconstructionFailure,
    OwnedObjectState, ReleaseOwnedObjectForTaskReply,
    ReplaceRetainedObjectDisposition, ReplaceRetainedObjectFailure,
    ReplaceRetainedObjectForTaskReply,
    RequestOwnedObjectReconstructionReply, TaskReferenceHold,
    TaskReferenceHoldKind, TaskSpec, FunctionKey,
)
from miniray.recovery import RecoveryManager
from miniray.ownership import DeadWorkerReferenceRecord, ObjectOwnerTable
from miniray.resources import ResourceVector




@dataclass
class _Harness:
    registry: ForeignLineageRegistry
    runtime: ForeignLineageRuntime
    task_id: TaskID
    outputs: tuple[ObjectID, ...]
    edges: tuple[ForeignLineageEdge, ...]
    states: dict[ObjectID, OwnedObjectState]
    producer_attempts: dict[ObjectID, AttemptID]
    replace_calls: list[object]
    get_calls: list[ObjectID]
    reconstruct_calls: list[ObjectID]
    release_calls: list[object]
    dead: set[WorkerID]
    owner: ObjectOwnerTable
    recovery: RecoveryManager
    spec: TaskSpec
    composition_lock: object
    collection_receipt: ForeignLineageCollectionReceipt | None = None


def _harness(
    roles: tuple[ForeignLineageRole, ...] = (
        ForeignLineageRole.TOP_LEVEL, ForeignLineageRole.TOP_LEVEL,
    ),
) -> _Harness:
    registry = ForeignLineageRegistry()
    task_id = TaskID.random()
    borrower = WorkerID.random()
    outputs = (ObjectID.for_task(task_id, 0),)
    edges = tuple(
        ForeignLineageEdge(
            task_id, ObjectID.for_task(TaskID.random()), WorkerID.random(),
            ("127.0.0.1", 31000 + index), borrower,
            TaskReferenceHold(
                TaskReferenceHoldKind.RETAINED, borrower, task_id,
                AttemptID(task_id, 0),
            ),
            role,
        )
        for index, role in enumerate(roles)
    )
    registry.register(task_id, outputs, edges)
    states = {edge.dependency_object_id: OwnedObjectState.READY_INLINE for edge in edges}
    attempts = {
        edge.dependency_object_id: AttemptID(edge.dependency_object_id.task_id, 0)
        for edge in edges
    }
    replace_calls: list[object] = []
    get_calls: list[ObjectID] = []
    reconstruct_calls: list[ObjectID] = []
    release_calls: list[object] = []
    dead: set[WorkerID] = set()

    def replace_rpc(_address, request):
        replace_calls.append(request)
        return ReplaceRetainedObjectForTaskReply(
            request.object_id, request.owner_worker_id,
            request.borrower_worker_id, request.expected_hold,
            request.replacement_hold,
            ReplaceRetainedObjectDisposition.REPLACED,
        )

    def get_rpc(_address, request):
        get_calls.append(request.object_id)
        state = states[request.object_id]
        return GetRetainedOwnedObjectReply(
            request.object_id, request.owner_worker_id,
            request.borrower_worker_id, request.hold, True, state=state,
            data=(b"ready" if state is OwnedObjectState.READY_INLINE else None),
            current_attempt=attempts[request.object_id],
        )

    def reconstruct_rpc(_address, request):
        reconstruct_calls.append(request.object_id)
        return RequestOwnedObjectReconstructionReply(
            request.object_id, request.owner_worker_id,
            request.requester_worker_id, request.credential,
            request.borrower_token, request.expected_owner_attempt,
            OwnedObjectReconstructionDisposition.STARTED,
            reconstruction_attempt=request.expected_owner_attempt.next(),
        )

    def release_rpc(_address, request):
        release_calls.append(request)
        return ReleaseOwnedObjectForTaskReply(
            request.object_id, request.owner_worker_id,
            request.borrower_worker_id, request.hold, True, True,
        )

    runtime = ForeignLineageRuntime(
        registry, replace_retained=replace_rpc, get_retained=get_rpc,
        request_reconstruction=reconstruct_rpc, release_retained=release_rpc,
        owner_death_lookup=lambda worker_id: (
            DeadWorkerReferenceRecord(worker_id, "proof-{}".format(worker_id))
            if worker_id in dead else None
        ),
    )
    job = JobID.random()
    spec = TaskSpec(job, task_id, AttemptID(task_id, 0),
                    FunctionKey(job, __name__, "producer", "1"), (), 1,
                    ResourceVector(), borrower)
    owner = ObjectOwnerTable()
    owner.register_task_outputs(spec)
    assert owner.publish_inline(outputs[0], spec.attempt_id, b"completed-output")
    recovery = RecoveryManager()
    recovery.register_task(spec)
    recovery.record_task_success(task_id, spec.attempt_id)
    return _Harness(
        registry, runtime, task_id, outputs, edges, states, attempts,
        replace_calls, get_calls, reconstruct_calls, release_calls, dead,
        owner, recovery, spec, RLock(),
    )


def _receipt(harness: _Harness) -> ForeignLineageCollectionReceipt:
    if harness.collection_receipt is not None:
        return harness.collection_receipt
    output_id = harness.outputs[0]
    with harness.composition_lock:
        owner_plan = harness.owner.begin_collection(output_id, collection_id="actual-output-gc")
        assert owner_plan is not None
        recovery_plan = harness.recovery.validate_forget_collected_object(
            output_id, expected_task_spec=harness.spec,
            expected_attempt=harness.spec.attempt_id)
        harness.owner.validate_complete_collection(owner_plan)
        prepared = ForeignLineagePreparedCollectionReceipt(
            harness.task_id, output_id, owner_plan, recovery_plan)
        assert harness.owner.complete_collection(owner_plan).collected
        assert harness.recovery.commit_forget_collected_object(recovery_plan)
    harness.collection_receipt = ForeignLineageCollectionReceipt(prepared)
    return harness.collection_receipt


@pytest.mark.unit
def test_all_owner_acks_precede_dependency_gate_and_local_commit_permission() -> None:
    harness = _harness()
    attempt = AttemptID(harness.task_id, 1)

    result = harness.runtime.drive_renewal(harness.task_id, attempt)

    assert result.disposition is ForeignLineageRenewalDisposition.READY
    assert len(result.acknowledged) == result.total_edges == 2
    assert len(harness.replace_calls) == 2
    assert len(harness.get_calls) == 2
    record = harness.registry.snapshot(harness.task_id)
    assert record is not None
    assert {edge.hold.origin_attempt_id for edge in record.edges} == {attempt}
    assert harness.runtime.complete_renewal(harness.task_id, attempt)
    # TaskID-scoped lineage itself is a live shutdown obligation until the
    # complete output manifest has authoritative collection receipts.
    assert harness.runtime.has_pending_obligations()


@pytest.mark.unit
def test_lost_dependency_is_reconstructed_at_owner_before_parent_becomes_ready() -> None:
    harness = _harness((ForeignLineageRole.TOP_LEVEL,))
    edge = harness.edges[0]
    harness.states[edge.dependency_object_id] = OwnedObjectState.LOST
    attempt = AttemptID(harness.task_id, 1)

    first = harness.runtime.drive_renewal(harness.task_id, attempt)
    assert first.disposition is ForeignLineageRenewalDisposition.READY
    assert harness.reconstruct_calls == [edge.dependency_object_id]
    assert len(harness.replace_calls) == 1

    harness.states[edge.dependency_object_id] = OwnedObjectState.READY_INLINE
    harness.runtime.complete_renewal(harness.task_id, attempt)
    # Producer completion is observed later by the ordinary dependency gate.
    second = harness.states[edge.dependency_object_id]
    assert second is OwnedObjectState.READY_INLINE
    # Owner replacement was ACKed in the first pass and is never resent.
    assert len(harness.replace_calls) == 1


@pytest.mark.unit
def test_nested_only_edge_renews_but_does_not_gate_dependency_readiness() -> None:
    harness = _harness((ForeignLineageRole.NESTED,))
    edge = harness.edges[0]
    harness.states[edge.dependency_object_id] = OwnedObjectState.LOST

    result = harness.runtime.drive_renewal(
        harness.task_id, AttemptID(harness.task_id, 1)
    )

    assert result.disposition is ForeignLineageRenewalDisposition.READY
    assert harness.get_calls == []
    assert harness.reconstruct_calls == []


@pytest.mark.unit
def test_ambiguous_replace_preserves_exact_request_and_ack_bitmap() -> None:
    harness = _harness()
    first_edge, second_edge = harness.edges
    original_replace = harness.runtime._replace
    calls: list[object] = []
    fail_once = {second_edge.dependency_object_id}

    def flaky(address, request):
        calls.append(request)
        if request.object_id in fail_once:
            fail_once.remove(request.object_id)
            raise TimeoutError("ambiguous")
        return original_replace(address, request)

    harness.runtime._replace = flaky
    attempt = AttemptID(harness.task_id, 1)
    first = harness.runtime.drive_renewal(harness.task_id, attempt)

    assert first.disposition is ForeignLineageRenewalDisposition.WAITING
    assert first.obligations_pending
    assert len(first.acknowledged) == 1
    assert harness.runtime.has_pending_obligations()
    # No dependency query starts before the complete replacement ACK barrier.
    assert harness.get_calls == []

    second = harness.runtime.drive_renewal(harness.task_id, attempt)
    assert second.disposition is ForeignLineageRenewalDisposition.READY
    retries = [request for request in calls if request.object_id == second_edge.dependency_object_id]
    assert len(retries) == 2 and retries[0] is retries[1]
    assert len([request for request in calls if request.object_id == first_edge.dependency_object_id]) == 1


@pytest.mark.unit
def test_owner_stopped_after_ambiguous_send_keeps_convergence_obligation() -> None:
    harness = _harness((ForeignLineageRole.TOP_LEVEL,))
    edge = harness.edges[0]
    calls = 0

    def stopped_after_timeout(_address, request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("ACK lost")
        return ReplaceRetainedObjectForTaskReply(
            request.object_id, request.owner_worker_id,
            request.borrower_worker_id, request.expected_hold,
            request.replacement_hold, ReplaceRetainedObjectDisposition.FAILED,
            failure=ReplaceRetainedObjectFailure.OWNER_STOPPED,
            detail="owner protocol stopped",
        )

    harness.runtime._replace = stopped_after_timeout
    attempt = AttemptID(harness.task_id, 1)
    first = harness.runtime.drive_renewal(harness.task_id, attempt)
    second = harness.runtime.drive_renewal(harness.task_id, attempt)

    assert first.disposition is ForeignLineageRenewalDisposition.WAITING
    assert second.disposition is ForeignLineageRenewalDisposition.FAILED
    assert second.obligations_pending
    with pytest.raises(ForeignLineageRuntimeError, match="not ready"):
        harness.runtime.complete_renewal(harness.task_id, attempt)
    receipt = _receipt(harness)
    with pytest.raises(ForeignLineageRuntimeError, match="ambiguity"):
        harness.runtime.drive_collection(harness.task_id, receipt)
    assert not harness.runtime.drive_shutdown().complete
    assert harness.release_calls == []
    # Endpoint shutdown never installs a death tombstone.
    assert not harness.registry.owner_is_dead(edge.owner_worker_id)


@pytest.mark.unit
def test_later_already_replaced_ack_clears_prior_owner_stopped_failure() -> None:
    harness = _harness((ForeignLineageRole.TOP_LEVEL,))
    calls = 0

    def converges(_address, request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("ACK lost")
        if calls == 2:
            return ReplaceRetainedObjectForTaskReply(
                request.object_id, request.owner_worker_id,
                request.borrower_worker_id, request.expected_hold,
                request.replacement_hold,
                ReplaceRetainedObjectDisposition.FAILED,
                failure=ReplaceRetainedObjectFailure.OWNER_STOPPED,
                detail="temporarily fenced endpoint",
            )
        return ReplaceRetainedObjectForTaskReply(
            request.object_id, request.owner_worker_id,
            request.borrower_worker_id, request.expected_hold,
            request.replacement_hold,
            ReplaceRetainedObjectDisposition.ALREADY_REPLACED,
        )

    harness.runtime._replace = converges
    attempt = AttemptID(harness.task_id, 1)
    assert harness.runtime.drive_renewal(
        harness.task_id, attempt
    ).disposition is ForeignLineageRenewalDisposition.WAITING
    failed = harness.runtime.drive_renewal(harness.task_id, attempt)
    assert failed.disposition is ForeignLineageRenewalDisposition.FAILED
    ready = harness.runtime.drive_renewal(harness.task_id, attempt)
    assert ready.disposition is ForeignLineageRenewalDisposition.READY
    assert ready.failure is None


@pytest.mark.unit
def test_partial_ack_failure_collects_exact_mixed_holds_only_after_authority_commit() -> None:
    harness = _harness()
    failed = max(
        harness.edges,
        key=lambda edge: (edge.dependency_object_id, edge.owner_worker_id),
    )
    original = harness.runtime._replace

    def one_reject(address, request):
        if request.object_id == failed.dependency_object_id:
            return ReplaceRetainedObjectForTaskReply(
                request.object_id, request.owner_worker_id,
                request.borrower_worker_id, request.expected_hold,
                request.replacement_hold,
                ReplaceRetainedObjectDisposition.FAILED,
                failure=ReplaceRetainedObjectFailure.INACTIVE_OLD_HOLD,
                detail="old hold rejected",
            )
        return original(address, request)

    harness.runtime._replace = one_reject
    attempt = AttemptID(harness.task_id, 1)
    result = harness.runtime.drive_renewal(harness.task_id, attempt)

    assert result.disposition is ForeignLineageRenewalDisposition.FAILED
    assert len(result.acknowledged) == 1
    with pytest.raises(ForeignLineageRuntimeError, match="not ready"):
        harness.runtime.complete_renewal(harness.task_id, attempt)
    assert harness.runtime.has_pending_obligations()
    record = harness.registry.snapshot(harness.task_id)
    assert record is not None
    assert {edge.hold.origin_attempt_id for edge in record.edges} == {
        AttemptID(harness.task_id, 0), attempt,
    }
    receipt = _receipt(harness)
    collected = harness.runtime.drive_collection(harness.task_id, receipt)
    assert collected.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert {(request.object_id, request.hold) for request in harness.release_calls} == {
        (edge.dependency_object_id, edge.hold) for edge in record.edges}
    assert len(harness.release_calls) == 2
    assert harness.runtime.drive_collection(harness.task_id, receipt) == collected
    assert len(harness.release_calls) == 2 and not harness.runtime.has_pending_obligations()


@pytest.mark.unit
def test_remote_owner_dead_reply_does_not_install_local_death_or_discharge() -> None:
    harness = _harness((ForeignLineageRole.TOP_LEVEL,))
    edge = harness.edges[0]
    harness.states[edge.dependency_object_id] = OwnedObjectState.LOST

    def remote_death(_address, request):
        return RequestOwnedObjectReconstructionReply(
            request.object_id, request.owner_worker_id,
            request.requester_worker_id, request.credential,
            request.borrower_token, request.expected_owner_attempt,
            OwnedObjectReconstructionDisposition.FAILED,
            failure=OwnedObjectReconstructionFailure.OWNER_DEAD,
            detail="remote claims owner dead",
        )

    harness.runtime._reconstruct = remote_death
    result = harness.runtime.drive_renewal(
        harness.task_id, AttemptID(harness.task_id, 1)
    )

    assert result.disposition is ForeignLineageRenewalDisposition.FAILED
    assert not harness.registry.owner_is_dead(edge.owner_worker_id)
    with pytest.raises(ForeignLineageRuntimeError, match="not ready"):
        harness.runtime.complete_renewal(
            harness.task_id, AttemptID(harness.task_id, 1)
        )
    assert not harness.runtime.drive_shutdown().complete
    assert harness.release_calls == []
    collected = harness.runtime.drive_collection(harness.task_id, _receipt(harness))
    assert collected.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert len(harness.release_calls) == 1
    assert harness.release_calls[0].hold.origin_attempt_id == AttemptID(harness.task_id, 1)
    assert not harness.registry.owner_is_dead(edge.owner_worker_id)


@pytest.mark.unit
def test_committed_owner_death_clears_ambiguity_but_waits_for_collection_commit() -> None:
    harness = _harness((ForeignLineageRole.TOP_LEVEL,))
    edge = harness.edges[0]
    harness.runtime._replace = lambda *_args: (_ for _ in ()).throw(
        TimeoutError("ambiguous")
    )
    attempt = AttemptID(harness.task_id, 1)
    first = harness.runtime.drive_renewal(harness.task_id, attempt)
    assert first.obligations_pending

    harness.dead.add(edge.owner_worker_id)
    second = harness.runtime.drive_renewal(harness.task_id, attempt)

    assert second.disposition is ForeignLineageRenewalDisposition.FAILED
    assert not second.obligations_pending
    assert harness.registry.owner_is_dead(edge.owner_worker_id)
    with pytest.raises(ForeignLineageRuntimeError, match="not ready"):
        harness.runtime.complete_renewal(harness.task_id, attempt)
    assert not harness.runtime.drive_shutdown().complete
    collected = harness.runtime.drive_collection(harness.task_id, _receipt(harness))
    assert collected.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert harness.release_calls == []
    assert harness.registry.snapshot(harness.task_id) is None
    assert harness.runtime.drive_shutdown().complete


@pytest.mark.unit
def test_ready_result_is_fenced_when_death_mutates_session_before_commit() -> None:
    harness = _harness((ForeignLineageRole.TOP_LEVEL,))
    edge = harness.edges[0]
    original_get = harness.runtime._get

    def get_then_death(address, request):
        reply = original_get(address, request)
        harness.runtime.mark_owner_dead(
            DeadWorkerReferenceRecord(edge.owner_worker_id, "racing-death")
        )
        return reply

    harness.runtime._get = get_then_death
    result = harness.runtime.drive_renewal(
        harness.task_id, AttemptID(harness.task_id, 1)
    )

    assert result.disposition is ForeignLineageRenewalDisposition.FAILED
    assert "death" in (result.failure or "")
    assert harness.registry.owner_is_dead(edge.owner_worker_id)


@pytest.mark.unit
def test_single_output_collection_releases_all_current_input_holds_once() -> None:
    harness = _harness()
    attempt = AttemptID(harness.task_id, 1)
    assert harness.runtime.drive_renewal(
        harness.task_id, attempt
    ).disposition is ForeignLineageRenewalDisposition.READY
    harness.runtime.complete_renewal(harness.task_id, attempt)

    assert harness.release_calls == []
    receipt = _receipt(harness)
    final = harness.runtime.drive_collection(harness.task_id, receipt)
    replay = harness.runtime.drive_collection(harness.task_id, receipt)
    assert final.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert replay.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert replay == final
    assert {request.hold.origin_attempt_id for request in harness.release_calls} == {attempt}
    assert len(harness.release_calls) == 2
    assert harness.registry.snapshot(harness.task_id) is None


@pytest.mark.unit
def test_collection_ambiguity_replays_only_missing_release_and_blocks_shutdown() -> None:
    harness = _harness((ForeignLineageRole.TOP_LEVEL,))
    edge = harness.edges[0]
    calls: list[object] = []

    def flaky(_address, request):
        calls.append(request)
        if len(calls) == 1:
            raise TimeoutError("release ACK lost")
        return ReleaseOwnedObjectForTaskReply(
            request.object_id, request.owner_worker_id,
            request.borrower_worker_id, request.hold, True, False,
        )

    harness.runtime._release = flaky
    receipt = _receipt(harness)
    first = harness.runtime.drive_collection(harness.task_id, receipt)
    assert first.disposition is ForeignLineageCollectionDisposition.PENDING
    assert harness.runtime.has_pending_obligations()

    second = harness.runtime.drive_collection(harness.task_id, receipt)
    assert second.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert calls[0] == calls[1]
    assert not harness.runtime.has_pending_obligations()
    assert not harness.registry.owner_is_dead(edge.owner_worker_id)


@pytest.mark.unit
def test_dead_owner_discharges_final_release_without_rpc() -> None:
    harness = _harness((ForeignLineageRole.TOP_LEVEL,))
    edge = harness.edges[0]
    harness.dead.add(edge.owner_worker_id)

    result = harness.runtime.drive_collection(harness.task_id, _receipt(harness))

    assert result.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert harness.release_calls == []
    assert harness.registry.owner_is_dead(edge.owner_worker_id)


@pytest.mark.unit
def test_shutdown_converges_ambiguous_replacement_before_current_hold_release() -> None:
    harness = _harness((ForeignLineageRole.TOP_LEVEL,))
    edge = harness.edges[0]
    original_replace = harness.runtime._replace
    calls = 0

    def lost_first_ack(address, request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("replace ACK lost")
        return original_replace(address, request)

    harness.runtime._replace = lost_first_ack
    attempt = AttemptID(harness.task_id, 1)
    waiting = harness.runtime.drive_renewal(harness.task_id, attempt)
    assert waiting.obligations_pending

    shutdown_pass = harness.runtime.drive_shutdown()
    assert not shutdown_pass.complete
    assert harness.release_calls == []
    # Only the unique output's committed owner/recovery proof releases holds.
    final = harness.runtime.drive_collection(harness.task_id, _receipt(harness))

    assert final.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert calls == 2
    assert len(harness.release_calls) == 1
    assert harness.release_calls[0].hold.origin_attempt_id == attempt
    assert harness.registry.snapshot(harness.task_id) is None


@pytest.mark.unit
def test_shutdown_keeps_unresolved_replace_and_does_not_release_stale_hold() -> None:
    harness = _harness((ForeignLineageRole.TOP_LEVEL,))
    harness.runtime._replace = lambda *_args: (_ for _ in ()).throw(
        TimeoutError("still ambiguous")
    )
    attempt = AttemptID(harness.task_id, 1)
    harness.runtime.drive_renewal(harness.task_id, attempt)

    shutdown = harness.runtime.drive_shutdown()

    assert not shutdown.complete
    assert shutdown.pending_task_ids == (harness.task_id,)
    assert harness.release_calls == []
    assert harness.registry.snapshot(harness.task_id) is not None
