"""Pure admission contracts for a still-finalizing producer generation.

Deferral must be distinguishable from a broken local authority without
weakening capability or producer-attempt validation.  These tests use the
real owner/recovery reducers and no transport, threads, or processes.
"""

from __future__ import annotations

from dataclasses import replace
from queue import Queue

import pytest

from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.owner_reconstruction import (
    OwnedObjectReconstructionReducer, ReconstructionDeferred,
)
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.protocol import (
    BorrowedCredential, ContainedTransferSource, FunctionKey, InlineArg,
    OwnedObjectReconstructionDisposition as Disposition,
    OwnedObjectReconstructionFailure as Failure,
    RequestOwnedObjectReconstruction as Request,
    RetainedCredential, TaskReferenceHold, TaskReferenceHoldKind, TaskSpec,
)
from miniray.reconstruction_runtime import ReconstructionCoordinator
from miniray.recovery import RecoveryManager
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


class _Fixture:
    def __init__(self, credential_kind: str = "borrowed") -> None:
        self.owner_id = WorkerID.random()
        self.requester = WorkerID.random()
        job = JobID.random()
        task = TaskID.derive(job, TaskID.for_driver(job), 0)
        self.spec = TaskSpec(
            job, task, AttemptID(task, 0),
            FunctionKey(job, "reconstruction_deferred", "producer", "v1"),
            (InlineArg(b"argument"),), 1, ResourceVector({"CPU": 1}),
            self.owner_id, max_retries=1,
        )
        self.object_id = self.spec.return_ids()[0]
        self.owner = ObjectOwnerTable()
        self.owner.register(
            self.object_id, current_attempt=self.spec.attempt_id,
            producer_task_spec=self.spec,
        )
        self.owner.publish_stored(
            self.object_id, self.spec.attempt_id, NodeID.random()
        )
        self.owner.mark_lost(self.object_id, self.spec.attempt_id)
        self.recovery = RecoveryManager()
        self.recovery.register_task(self.spec, max_retries=1)
        self.recovery.record_task_success(task, self.spec.attempt_id)
        self.coordinator = ReconstructionCoordinator(self.recovery, self.owner)

        outer = ObjectID.for_task(TaskID.derive(job, task, 1))
        self.source = ContainedTransferSource(ContainedReferenceHold(
            outer, self.owner_id, "deferred-export-pin",
        ))
        self.token = "deferred-borrower"
        self.owner.add_contained_reference(
            self.object_id, self.source.hold
        )
        self.owner.acquire_exported_reference(
            self.object_id, self.source, (self.requester, self.token)
        )
        credential = BorrowedCredential(self.source, self.token)
        if credential_kind == "retained":
            consumer_task = TaskID.random()
            hold = TaskReferenceHold(
                TaskReferenceHoldKind.RETAINED, self.requester, consumer_task,
                AttemptID(consumer_task, 0),
            )
            self.owner.retain_borrowed_reference_for_task(
                self.object_id, (self.requester, self.token), hold
            )
            self.owner.release_borrowed_reference(
                self.object_id, (self.requester, self.token)
            )
            credential = RetainedCredential(hold)
        self.request = Request(
            self.object_id, self.owner_id, self.requester, credential,
            None if credential_kind == "retained" else self.token,
            self.spec.attempt_id,
        )
        self.admission_calls: list[ObjectID] = []
        self.queue = Queue()
        self.admission_mode = "deferred"

        def admit(object_id):
            self.admission_calls.append(object_id)
            if self.admission_mode == "deferred":
                raise ReconstructionDeferred(
                    "the prior logical task is still releasing submitted holds"
                )
            if self.admission_mode == "none":
                return None
            if self.admission_mode == "runtime_error":
                raise RuntimeError("the prior logical task is still finalizing")
            if self.admission_mode == "value_error":
                raise ValueError("invalid local reconstruction state")
            assert self.admission_mode == "start"
            return self.coordinator.handoff(
                self.coordinator.request(object_id), self.queue.put
            )

        self.reducer = OwnedObjectReconstructionReducer(
            self.owner_id, self.owner, self.recovery, admit
        )

    def release_credential(self) -> None:
        credential = self.request.credential
        if isinstance(credential, RetainedCredential):
            assert self.owner.release_retained_reference_for_task(
                self.object_id, credential.hold
            )
        else:
            assert self.owner.release_borrowed_reference(
                self.object_id, (self.requester, credential.borrower_token)
            )


@pytest.mark.parametrize("credential_kind", ["borrowed", "retained"])
def test_deferred_is_uncached_and_same_identity_can_later_start(
    credential_kind: str,
) -> None:
    fixture = _Fixture(credential_kind)
    request = fixture.request
    before_owner = fixture.owner.snapshot(fixture.object_id)
    before_recovery = fixture.recovery.reconstruction_snapshot(fixture.object_id)

    for _ in range(2):
        deferred = fixture.reducer.handle(request)
        assert deferred.disposition is Disposition.FAILED
        assert deferred.failure is Failure.NOT_LOST
        assert deferred.reconstruction_attempt is None
        assert deferred.detail
        assert (
            deferred.object_id, deferred.owner_worker_id,
            deferred.requester_worker_id, deferred.credential,
            deferred.borrower_token, deferred.expected_owner_attempt,
        ) == (
            request.object_id, request.owner_worker_id,
            request.requester_worker_id, request.credential,
            request.borrower_token, request.expected_owner_attempt,
        )
        assert fixture.owner.snapshot(fixture.object_id) == before_owner
        assert fixture.recovery.reconstruction_snapshot(
            fixture.object_id
        ) == before_recovery
    assert fixture.admission_calls == [fixture.object_id] * 2

    fixture.admission_mode = "start"
    started = fixture.reducer.handle(request)
    assert started.disposition is Disposition.STARTED
    assert started.failure is None
    assert started.reconstruction_attempt == fixture.spec.attempt_id.next()
    assert fixture.owner.snapshot(fixture.object_id).state is ObjectState.PENDING
    assert fixture.recovery.active_recovery(fixture.spec.task_id) == (
        started.reconstruction_attempt
    )
    assert fixture.admission_calls == [fixture.object_id] * 3

    # Only the accepted START is cached; its ACK remains replayable after the
    # capability closes and the producer epoch has advanced.
    fixture.release_credential()
    assert fixture.reducer.handle(request) is started
    assert fixture.admission_calls == [fixture.object_id] * 3


@pytest.mark.parametrize(
    ("invalid", "failure"),
    (
        ("source", Failure.CREDENTIAL_MISMATCH),
        ("inactive", Failure.INACTIVE_CREDENTIAL),
        ("released", Failure.RELEASED_CREDENTIAL),
        ("attempt", Failure.EXPECTED_ATTEMPT_MISMATCH),
        ("owner", Failure.WRONG_OWNER),
    ),
)
def test_capability_and_attempt_checks_precede_deferred_admission(
    invalid: str, failure: Failure,
) -> None:
    fixture = _Fixture()
    request = fixture.request
    if invalid == "source":
        request = replace(
            request, credential=BorrowedCredential(
                ContainedTransferSource(replace(fixture.source.hold, transfer_token="unbound-source")), fixture.token
            )
        )
    elif invalid == "inactive":
        request = replace(
            request, credential=BorrowedCredential(fixture.source, "inactive"),
            borrower_token="inactive",
        )
    elif invalid == "released":
        fixture.release_credential()
    elif invalid == "attempt":
        request = replace(
            request, expected_owner_attempt=fixture.spec.attempt_id.next()
        )
    else:
        request = replace(request, owner_worker_id=WorkerID.random())

    result = fixture.reducer.handle(request)
    assert result.disposition is Disposition.FAILED
    assert result.failure is failure
    assert fixture.admission_calls == []


@pytest.mark.parametrize("credential_kind", ["borrowed", "retained"])
def test_deferred_request_revalidates_capability_on_retry(
    credential_kind: str,
) -> None:
    fixture = _Fixture(credential_kind)
    assert fixture.reducer.handle(fixture.request).failure is Failure.NOT_LOST
    fixture.release_credential()
    fixture.admission_mode = "start"

    retry = fixture.reducer.handle(fixture.request)
    assert retry.failure is Failure.RELEASED_CREDENTIAL
    assert fixture.admission_calls == [fixture.object_id]
    assert fixture.recovery.active_recovery(fixture.spec.task_id) is None


def test_deferred_request_revalidates_expected_attempt_on_retry() -> None:
    fixture = _Fixture()
    assert fixture.reducer.handle(fixture.request).failure is Failure.NOT_LOST
    assert fixture.owner.advance_attempt(
        fixture.object_id, expected_attempt=fixture.spec.attempt_id,
        next_attempt=fixture.spec.attempt_id.next(),
    )
    fixture.admission_mode = "start"

    retry = fixture.reducer.handle(fixture.request)
    assert retry.failure is Failure.EXPECTED_ATTEMPT_MISMATCH
    assert fixture.admission_calls == [fixture.object_id]


def test_deferral_preserves_transaction_binding_without_caching_the_failure() -> None:
    fixture = _Fixture()
    exact = fixture.request
    assert fixture.reducer.handle(exact).failure is Failure.NOT_LOST
    rebound = replace(
        exact, credential=BorrowedCredential(
            ContainedTransferSource(replace(fixture.source.hold, transfer_token="changed-binding")), fixture.token
        )
    )
    assert fixture.reducer.handle(rebound).failure is Failure.REQUEST_CONFLICT
    assert fixture.admission_calls == [fixture.object_id]

    fixture.admission_mode = "start"
    assert fixture.reducer.handle(exact).disposition is Disposition.STARTED
    assert fixture.admission_calls == [fixture.object_id] * 2


@pytest.mark.parametrize("mode", ["none", "runtime_error", "value_error"])
def test_only_typed_deferral_maps_to_retryable_not_lost(mode: str) -> None:
    fixture = _Fixture()
    fixture.admission_mode = mode
    owner_before = fixture.owner.snapshot(fixture.object_id)
    result = fixture.reducer.handle(fixture.request)

    assert result.disposition is Disposition.FAILED
    assert result.failure is Failure.AUTHORITY_REJECTED
    assert result.reconstruction_attempt is None
    assert fixture.admission_calls == [fixture.object_id]
    assert fixture.owner.snapshot(fixture.object_id) == owner_before
    assert fixture.recovery.active_recovery(fixture.spec.task_id) is None
