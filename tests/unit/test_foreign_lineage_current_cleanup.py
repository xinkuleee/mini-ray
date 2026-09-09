"""Finite single-output carriers for API-013's four retained assertions.

No servers, subprocesses, threads, waits or sockets are constructed. RPC
callbacks are explicit reducer inputs; owner/recovery collection receipts are
created only after both local authorities commit.
"""

from threading import RLock

import pytest

from miniray import protocol
from miniray.foreign_lineage import (
    ForeignLineageCollectionReceipt, ForeignLineageEdge,
    ForeignLineagePreparedCollectionReceipt, ForeignLineageRegistry, ForeignLineageRole,
)
from miniray.foreign_lineage_runtime import (
    ForeignLineageCollectionDisposition, ForeignLineageRenewalDisposition,
    ForeignLineageRuntime, ForeignLineageRuntimeError,
)
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.ownership import DeadWorkerReferenceRecord, ObjectOwnerTable
from miniray.recovery import RecoveryManager
from miniray.resources import ResourceVector

pytestmark = pytest.mark.unit


def _id(kind, number):
    return kind(bytes([number]) * 16)


class _Fixture:
    def __init__(self, count=1):
        job, task, borrower = _id(JobID, 1), _id(TaskID, 2), _id(WorkerID, 3)
        self.task, self.output = task, ObjectID.for_task(task)
        self.attempt = AttemptID(task, 1)
        self.spec = protocol.TaskSpec(
            job, task, AttemptID(task, 0),
            protocol.FunctionKey(job, __name__, 'fixture', '1'),
            (), 1, ResourceVector(), borrower,
        )
        self.edges = tuple(ForeignLineageEdge(
            task, ObjectID.for_task(_id(TaskID, 10 + index)),
            _id(WorkerID, 20 + index), ('owner.invalid', 12000 + index), borrower,
            protocol.TaskReferenceHold(protocol.TaskReferenceHoldKind.RETAINED,
                borrower, task, self.spec.attempt_id), ForeignLineageRole.TOP_LEVEL,
        ) for index in range(count))
        self.registry = ForeignLineageRegistry()
        self.registry.register(task, (self.output,), self.edges)
        self.replace_calls, self.release_calls = [], []
        self.dead = set()
        self.runtime = ForeignLineageRuntime(
            self.registry, replace_retained=self.replace, get_retained=self.get,
            request_reconstruction=self.reconstruct, release_retained=self.release,
            owner_death_lookup=lambda owner: (
                DeadWorkerReferenceRecord(owner, 'installed:' + owner.hex)
                if owner in self.dead else None),
        )
        self.owner = ObjectOwnerTable()
        self.owner.register_task_outputs(self.spec)
        self.owner.publish_inline(self.output, self.spec.attempt_id, b'completed output')
        self.recovery = RecoveryManager()
        self.recovery.register_task(self.spec)
        self.recovery.record_task_success(task, self.spec.attempt_id)
        self.composition_lock = RLock()

    def replace(self, _address, request):
        self.replace_calls.append(request)
        return protocol.ReplaceRetainedObjectForTaskReply(
            request.object_id, request.owner_worker_id, request.borrower_worker_id,
            request.expected_hold, request.replacement_hold,
            protocol.ReplaceRetainedObjectDisposition.REPLACED,
        )

    def get(self, _address, request):
        return protocol.GetRetainedOwnedObjectReply(
            request.object_id, request.owner_worker_id, request.borrower_worker_id,
            request.hold, True, state=protocol.OwnedObjectState.READY_INLINE,
            data=b'input', current_attempt=AttemptID(request.object_id.task_id, 0),
        )

    def reconstruct(self, _address, _request):
        raise AssertionError('ready inputs must not reconstruct')

    def release(self, _address, request):
        self.release_calls.append(request)
        return protocol.ReleaseOwnedObjectForTaskReply(
            request.object_id, request.owner_worker_id, request.borrower_worker_id,
            request.hold, True, True,
        )

    def collect_receipt(self):
        with self.composition_lock:
            owner_plan = self.owner.begin_collection(self.output, collection_id='collected-output')
            assert owner_plan is not None
            recovery_plan = self.recovery.validate_forget_collected_object(
                self.output, expected_task_spec=self.spec,
                expected_attempt=self.spec.attempt_id,
            )
            self.owner.validate_complete_collection(owner_plan)
            prepared = ForeignLineagePreparedCollectionReceipt(
                self.task, self.output, owner_plan, recovery_plan)
            assert self.owner.complete_collection(owner_plan).collected
            assert self.recovery.commit_forget_collected_object(recovery_plan)
        return ForeignLineageCollectionReceipt(prepared)


def _failed(request, failure):
    return protocol.ReplaceRetainedObjectForTaskReply(
        request.object_id, request.owner_worker_id, request.borrower_worker_id,
        request.expected_hold, request.replacement_hold,
        protocol.ReplaceRetainedObjectDisposition.FAILED, failure=failure,
        detail='explicit owner reply',
    )


def test_ambiguous_replace_cannot_commit_or_collect_and_blocks_shutdown():
    f = _Fixture()
    calls = []

    def stopped_after_timeout(_address, request):
        calls.append(request)
        if len(calls) == 1:
            raise TimeoutError('ACK lost')
        return _failed(request, protocol.ReplaceRetainedObjectFailure.OWNER_STOPPED)

    f.runtime._replace = stopped_after_timeout
    assert f.runtime.drive_renewal(f.task, f.attempt).obligations_pending
    failed = f.runtime.drive_renewal(f.task, f.attempt)
    assert failed.disposition is ForeignLineageRenewalDisposition.FAILED
    assert failed.obligations_pending
    with pytest.raises(ForeignLineageRuntimeError, match='not ready'):
        f.runtime.complete_renewal(f.task, f.attempt)
    receipt = f.collect_receipt()
    with pytest.raises(ForeignLineageRuntimeError, match='ambiguity'):
        f.runtime.drive_collection(f.task, receipt)
    shutdown = f.runtime.drive_shutdown()
    assert not shutdown.complete and shutdown.pending_task_ids == (f.task,)
    assert f.release_calls == []
    assert len(calls) == 3 and all(request is calls[0] for request in calls)
    assert not f.registry.owner_is_dead(f.edges[0].owner_worker_id)


def test_partial_ack_collects_exact_mixed_holds_after_local_collection_commit():
    f = _Fixture(count=2)
    rejected = max(f.edges, key=lambda edge: (edge.dependency_object_id, edge.owner_worker_id))

    def partial(address, request):
        if request.object_id == rejected.dependency_object_id:
            return _failed(request, protocol.ReplaceRetainedObjectFailure.INACTIVE_OLD_HOLD)
        return f.replace(address, request)

    f.runtime._replace = partial
    result = f.runtime.drive_renewal(f.task, f.attempt)
    assert result.disposition is ForeignLineageRenewalDisposition.FAILED
    assert len(result.acknowledged) == 1
    with pytest.raises(ForeignLineageRuntimeError, match='not ready'):
        f.runtime.complete_renewal(f.task, f.attempt)
    before = f.registry.snapshot(f.task)
    assert {edge.hold.origin_attempt_id for edge in before.edges} == {f.spec.attempt_id, f.attempt}
    receipt = f.collect_receipt()
    result = f.runtime.drive_collection(f.task, receipt)
    assert result.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert {(request.object_id, request.hold) for request in f.release_calls} == {
        (edge.dependency_object_id, edge.hold) for edge in before.edges}
    assert len(f.release_calls) == 2
    assert f.runtime.drive_collection(f.task, receipt) == result
    assert len(f.release_calls) == 2 and not f.runtime.has_pending_obligations()


def test_remote_death_claim_cannot_discharge_live_owner_hold():
    f = _Fixture()

    def lost(_address, request):
        return protocol.GetRetainedOwnedObjectReply(
            request.object_id, request.owner_worker_id, request.borrower_worker_id,
            request.hold, True, state=protocol.OwnedObjectState.LOST,
            current_attempt=AttemptID(request.object_id.task_id, 0))

    def remote_claim(_address, request):
        return protocol.RequestOwnedObjectReconstructionReply(
            request.object_id, request.owner_worker_id, request.requester_worker_id,
            request.credential, request.borrower_token, request.expected_owner_attempt,
            protocol.OwnedObjectReconstructionDisposition.FAILED,
            failure=protocol.OwnedObjectReconstructionFailure.OWNER_DEAD,
            detail='uncommitted remote claim')

    f.runtime._get, f.runtime._reconstruct = lost, remote_claim
    result = f.runtime.drive_renewal(f.task, f.attempt)
    assert result.disposition is ForeignLineageRenewalDisposition.FAILED
    with pytest.raises(ForeignLineageRuntimeError, match='not ready'):
        f.runtime.complete_renewal(f.task, f.attempt)
    assert not f.registry.owner_is_dead(f.edges[0].owner_worker_id)
    assert not f.runtime.drive_shutdown().complete
    assert not f.release_calls
    result = f.runtime.drive_collection(f.task, f.collect_receipt())
    assert result.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert len(f.release_calls) == 1 and f.release_calls[0].hold.origin_attempt_id == f.attempt
    assert not f.registry.owner_is_dead(f.edges[0].owner_worker_id)


def test_installed_owner_death_clears_ambiguity_only_after_collection_authority():
    f = _Fixture()

    def ambiguous(_address, _request):
        raise TimeoutError('unknown replace outcome')

    f.runtime._replace = ambiguous
    assert f.runtime.drive_renewal(f.task, f.attempt).obligations_pending
    f.dead.add(f.edges[0].owner_worker_id)
    failed = f.runtime.drive_renewal(f.task, f.attempt)
    assert failed.disposition is ForeignLineageRenewalDisposition.FAILED
    assert not failed.obligations_pending
    assert f.registry.owner_is_dead(f.edges[0].owner_worker_id)
    with pytest.raises(ForeignLineageRuntimeError, match='not ready'):
        f.runtime.complete_renewal(f.task, f.attempt)
    assert not f.runtime.drive_shutdown().complete
    result = f.runtime.drive_collection(f.task, f.collect_receipt())
    assert result.disposition is ForeignLineageCollectionDisposition.COMPLETE
    assert f.release_calls == [] and f.registry.snapshot(f.task) is None
    assert f.runtime.drive_shutdown().complete
