"""Owner-routed reconstruction with explicit pure/concurrent classification.

Eleven functions (fourteen expanded cases) use only synchronous local reducers.
The original concurrent exact-request case is opt-in L1: two owned Threads
cross one three-party Barrier (1 s); normal joins share 2 s and failure cleanup
shares 1 s. Real reducer/coordinator locks and cached-reply identity remain
unchanged. Thread.start and ordinary locks still rely on the bounded runner's
outer deadline. This is the original metadata model, not canonical runtime
publication: one Task/slot, no physical store, Node, transport or user function.
Markers remain per function so the L1 case cannot inherit unit from the module.
"""

from __future__ import annotations

from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from queue import Queue

import pytest

from miniray.contained_edges import ContainedReferenceHold
from miniray.errors import ProtocolError
from miniray.ids import (
    AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID,
)
from miniray.owner_reconstruction import OwnedObjectReconstructionReducer
from miniray.ownership import ObjectOwnerTable
from miniray.protocol import (
    ContainedTransferSource,
    BorrowedCredential,
    FunctionKey,
    InlineArg,
    OwnedObjectReconstructionDisposition as Disposition,
    OwnedObjectReconstructionFailure as Failure,
    RequestOwnedObjectReconstruction as Request,
    RequestOwnedObjectReconstructionReply as Reply,
    RetainedCredential,
    TaskHoldSource,
    TaskReferenceHold,
    TaskReferenceHoldKind,
    TaskSpec,
)
from miniray.reconstruction_runtime import ReconstructionCoordinator
from miniray.recovery import RecoveryManager
from miniray.resources import ResourceVector


def _spec(owner_worker_id: WorkerID, *, max_retries: int = 1) -> TaskSpec:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), 0)
    return TaskSpec(
        job_id,
        task_id,
        AttemptID(task_id, 0),
        FunctionKey(job_id, "owner_reconstruction", "producer", "v1"),
        (InlineArg(b"argument"),),
        1,
        ResourceVector({"CPU": 1}),
        owner_worker_id,
        max_retries=max_retries,
    )


class _Fixture:
    def __init__(
        self, *, max_retries: int = 1, lost: bool = True, put: bool = False,
        death_proof: object | None = None,
    ) -> None:
        self.owner_id = WorkerID.random()
        self.first_requester = WorkerID.random()
        self.second_requester = WorkerID.random()
        self.spec = _spec(self.owner_id, max_retries=max_retries)
        outer = ObjectID.for_task(TaskID.derive(self.spec.job_id, self.spec.task_id, 1))
        self.source = ContainedTransferSource(ContainedReferenceHold(
            outer, self.owner_id, "export-pin",
        ))
        self.object_id = self.spec.return_ids()[0]
        self.owner = ObjectOwnerTable()
        self.recovery = RecoveryManager()
        self.owner.register(
            self.object_id,
            current_attempt=self.spec.attempt_id,
            producer_task_spec=None if put else self.spec,
        )
        self.owner.publish_stored(
            self.object_id, self.spec.attempt_id, NodeID.random()
        )
        if lost:
            self.owner.mark_lost(self.object_id, self.spec.attempt_id)
        if put:
            self.recovery.register_put(self.object_id)
        else:
            self.recovery.register_task(
                self.spec, max_retries=self.spec.max_retries
            )
            self.recovery.record_task_success(
                self.spec.task_id, self.spec.attempt_id
            )
        self.owner.add_contained_reference(
            self.object_id, self.source.hold
        )
        for requester, token in (
            (self.first_requester, "first-borrower"),
            (self.second_requester, "second-borrower"),
        ):
            self.owner.acquire_exported_reference(
                self.object_id, self.source, (requester, token)
            )
        self.coordinator = ReconstructionCoordinator(
            self.recovery, self.owner
        )
        self.admission_calls: list[ObjectID] = []
        self.queue = Queue()

        def admit(object_id: ObjectID):
            self.admission_calls.append(object_id)
            return self.coordinator.handoff(
                self.coordinator.request(object_id), self.queue.put
            )

        self.reducer = OwnedObjectReconstructionReducer(
            self.owner_id,
            self.owner,
            self.recovery,
            admit,
            worker_death_lookup=(
                None if death_proof is None else lambda _worker: death_proof
            ),
        )

    def request(
        self,
        *,
        requester: WorkerID | None = None,
        token: str = "first-borrower",
        source: ContainedTransferSource | None = None,
        expected: AttemptID | None = None,
        owner: WorkerID | None = None,
    ) -> Request:
        return Request(
            self.object_id,
            self.owner_id if owner is None else owner,
            self.first_requester if requester is None else requester,
            self.source if source is None else source,
            token,
            self.spec.attempt_id if expected is None else expected,
        )


@pytest.mark.unit
def test_protocol_echoes_full_capability_and_enforces_reply_shape() -> None:
    fixture = _Fixture()
    request = fixture.request()
    attempt = request.expected_owner_attempt.next()

    reply = Reply(
        request.object_id,
        request.owner_worker_id,
        request.requester_worker_id,
        request.source,
        request.borrower_token,
        request.expected_owner_attempt,
        Disposition.STARTED,
        reconstruction_attempt=attempt,
    )

    assert (
        reply.object_id,
        reply.owner_worker_id,
        reply.requester_worker_id,
        reply.source,
        reply.borrower_token,
        reply.expected_owner_attempt,
    ) == (
        request.object_id,
        request.owner_worker_id,
        request.requester_worker_id,
        request.source,
        request.borrower_token,
        request.expected_owner_attempt,
    )
    with pytest.raises(ProtocolError, match="requires an attempt"):
        replace(reply, reconstruction_attempt=None)
    with pytest.raises(ProtocolError, match="newer than expected"):
        replace(reply, reconstruction_attempt=request.expected_owner_attempt)
    with pytest.raises(ProtocolError, match="typed failure"):
        replace(
            reply, disposition=Disposition.FAILED,
            reconstruction_attempt=None, detail="no type",
        )
    with pytest.raises(ProtocolError, match="non-empty detail"):
        replace(
            reply, disposition=Disposition.FAILED,
            reconstruction_attempt=None, failure=Failure.NOT_LOST,
        )


@pytest.mark.unit
def test_start_is_owner_authoritative_and_exact_ack_replay_survives_release() -> None:
    fixture = _Fixture()
    request = fixture.request()

    started = fixture.reducer.handle(request)
    attempt = fixture.spec.attempt_id.next()

    assert started.disposition is Disposition.STARTED
    assert started.reconstruction_attempt == attempt
    assert fixture.admission_calls == [fixture.object_id]
    assert fixture.owner.snapshot(fixture.object_id).current_attempt == attempt
    assert fixture.recovery.active_recovery(fixture.spec.task_id) == attempt

    assert fixture.owner.release_borrowed_reference(
        fixture.object_id,
        (fixture.first_requester, request.borrower_token),
    )
    assert fixture.reducer.handle(request) is started
    assert fixture.admission_calls == [fixture.object_id]


@pytest.mark.unit
def test_second_active_capability_joins_the_same_reconstruction_attempt() -> None:
    fixture = _Fixture()
    first = fixture.reducer.handle(fixture.request())
    second_request = fixture.request(
        requester=fixture.second_requester, token="second-borrower"
    )

    joined = fixture.reducer.handle(second_request)

    assert first.disposition is Disposition.STARTED
    assert joined.disposition is Disposition.JOINED
    assert joined.reconstruction_attempt == first.reconstruction_attempt
    assert fixture.admission_calls == [fixture.object_id, fixture.object_id]


@pytest.mark.loopback_smoke
def test_concurrent_exact_requests_commit_once_and_replay_one_reply(monkeypatch) -> None:
    """Keep real concurrent callers; no claim about an OS lock schedule."""
    import math
    import multiprocessing.process
    import queue
    import socket
    import subprocess
    import threading
    import time

    from miniray import control, core, node, transport, worker
    from miniray.object_store import ObjectStore

    errors = queue.Queue(maxsize=16)
    outcomes = queue.Queue(maxsize=2)
    violations = queue.Queue(maxsize=16)
    overflow = False
    created, started = [], []
    joins = []
    constructing = False
    barrier = None
    real_init, real_start, real_join = (
        threading.Thread.__init__, threading.Thread.start, threading.Thread.join,
    )

    def retain(queue_, value):
        nonlocal overflow
        try:
            queue_.put_nowait(value)
        except queue.Full:
            overflow = True

    def forbidden(*args, **kwargs):
        retain(violations, (threading.current_thread(), args, kwargs))
        error = AssertionError("owner reconstruction L1 attempted runtime infrastructure")
        retain(errors, error)
        raise error

    def initialize(thread, *args, **kwargs):
        if not constructing or len(created) >= 2:
            forbidden("unowned thread construction")
        real_init(thread, *args, **kwargs)
        if not thread.daemon or thread.name != "miniray-owner-reconstruction-{}".format(len(created)):
            forbidden("owned request thread identity drift")
        created.append(thread)

    def start(thread):
        if thread not in created or thread in started or len(started) >= 2:
            forbidden("unowned or repeated request thread start")
        started.append(thread)
        return real_start(thread)

    def join(thread, timeout=None):
        if (thread not in created or len(joins) >= 4 or type(timeout) not in (int, float)
                or not math.isfinite(timeout) or not 0 <= timeout <= 2.0):
            forbidden("unowned or unbounded request thread join", timeout)
        joins.append((thread, timeout))
        return real_join(thread, timeout=timeout)

    try:
        monkeypatch.setattr(threading.Thread, "__init__", initialize)
        monkeypatch.setattr(threading.Thread, "start", start)
        monkeypatch.setattr(threading.Thread, "join", join)
        for kind in (core.CoreWorker, node.NodeServer, worker.WorkerServer,
                     control.GCSLite, transport.TCPServer, ObjectStore, ThreadPoolExecutor):
            monkeypatch.setattr(kind, "__init__", forbidden)
        monkeypatch.setattr(threading.Timer, "__init__", forbidden)
        for method in ("__init__", "start", "join"):
            monkeypatch.setattr(multiprocessing.process.BaseProcess, method, forbidden)
        for name in ("socket", "socketpair", "create_connection"):
            monkeypatch.setattr(socket, name, forbidden)
        monkeypatch.setattr(subprocess, "Popen", forbidden)
        monkeypatch.setattr(queue.Queue, "join", forbidden)
        monkeypatch.setattr(time, "sleep", forbidden)
        for module in (core, node, worker, control):
            monkeypatch.setattr(module, "rpc_request", forbidden)
        monkeypatch.setattr(transport, "request", forbidden)

        fixture = _Fixture()
        request = fixture.request()
        reducer_lock = fixture.reducer._lock
        coordinator_lock = fixture.coordinator._lock
        owner_lock = fixture.owner._lock
        barrier = Barrier(3, timeout=1.0)

        def invoke(index) -> None:
            try:
                barrier.wait(timeout=1.0)
                reply = fixture.reducer.handle(request)
                outcomes.put_nowait((index, threading.current_thread(), reply))
            except BaseException as exc:
                retain(errors, exc)

        constructing = True
        try:
            threads = tuple(threading.Thread(
                target=invoke, args=(index,),
                name="miniray-owner-reconstruction-{}".format(index), daemon=True,
            ) for index in range(2))
        finally:
            constructing = False
        for thread in threads:
            thread.start()
        barrier.wait(timeout=1.0)
        deadline = time.monotonic() + 2.0
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        assert not any(thread.is_alive() for thread in threads)
        assert not overflow and errors.empty() and violations.empty()
        assert created == started == list(threads)
        observed = tuple(outcomes.get_nowait() for _ in range(2))
        for _ in observed:
            outcomes.task_done()
        assert outcomes.empty() and outcomes.unfinished_tasks == 0
        assert {index for index, _, _ in observed} == {0, 1}
        assert all(thread is threads[index] for index, thread, _ in observed)
        replies = tuple(reply for _, _, reply in sorted(observed, key=lambda item: item[0]))
        assert replies[0] is replies[1]
        assert replies[0].disposition is Disposition.STARTED
        assert fixture.admission_calls == [fixture.object_id]
        assert fixture.reducer._lock is reducer_lock
        assert fixture.coordinator._lock is coordinator_lock
        assert fixture.owner._lock is owner_lock
        assert fixture.reducer._replies == {request: replies[0]}
        assert fixture.reducer._replies[request] is replies[0]
        attempt = fixture.spec.attempt_id.next()
        assert replies[0].reconstruction_attempt == attempt
        assert fixture.owner.snapshot(fixture.object_id).current_attempt == attempt
        assert fixture.recovery.active_recovery(fixture.spec.task_id) == attempt
        assert fixture.recovery.task_record(fixture.spec.task_id).retries_started == 1
    finally:
        # No owner/reducer/coordinator lock is acquired after a failed join.
        # Only our one barrier and exact created threads are cleanup targets;
        # no fabricated terminal state or metadata GC makes this test clean.
        if barrier is not None:
            barrier.abort()
        deadline = time.monotonic() + 1.0
        for thread in created:
            if thread.ident is not None:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
        assert not any(thread.is_alive() for thread in created)
        assert not any(thread in created for thread in threading.enumerate())
        assert not overflow and errors.empty() and violations.empty()


@pytest.mark.unit
def test_source_binding_is_verified_and_claim_drift_is_a_typed_conflict() -> None:
    fixture = _Fixture()
    unbound = fixture.request(source=ContainedTransferSource(replace(fixture.source.hold, transfer_token="forged")))

    mismatch = fixture.reducer.handle(unbound)

    assert mismatch.disposition is Disposition.FAILED
    assert mismatch.failure is Failure.CREDENTIAL_MISMATCH
    assert fixture.admission_calls == []

    exact = fixture.request()
    started = fixture.reducer.handle(exact)
    drift = fixture.reducer.handle(
        replace(
            exact, credential=BorrowedCredential(
                ContainedTransferSource(replace(fixture.source.hold, transfer_token="drift")), exact.borrower_token
            )
        )
    )
    assert started.disposition is Disposition.STARTED
    assert drift.disposition is Disposition.FAILED
    assert drift.failure is Failure.REQUEST_CONFLICT
    assert fixture.admission_calls == [fixture.object_id]


@pytest.mark.unit
def test_failed_forgery_does_not_poison_a_later_exact_valid_request() -> None:
    fixture = _Fixture()
    exact = fixture.request()
    forged = replace(
        exact, credential=BorrowedCredential(
            ContainedTransferSource(replace(fixture.source.hold, transfer_token="forged")), exact.borrower_token
        )
    )

    rejected = fixture.reducer.handle(forged)
    started = fixture.reducer.handle(exact)

    assert rejected.failure is Failure.CREDENTIAL_MISMATCH
    assert started.disposition is Disposition.STARTED
    assert fixture.admission_calls == [fixture.object_id]


@pytest.mark.unit
def test_released_and_inactive_credentials_never_reach_local_authority() -> None:
    fixture = _Fixture()
    request = fixture.request()
    fixture.owner.release_borrowed_reference(
        fixture.object_id,
        (fixture.first_requester, request.borrower_token),
    )

    released = fixture.reducer.handle(request)
    inactive = fixture.reducer.handle(
        fixture.request(token="never-acquired")
    )

    assert released.failure is Failure.RELEASED_CREDENTIAL
    assert inactive.failure is Failure.INACTIVE_CREDENTIAL
    assert fixture.admission_calls == []


@pytest.mark.unit
@pytest.mark.parametrize(
    ("fixture_kwargs", "failure"),
    (
        ({"put": True}, Failure.PUT_OBJECT),
        ({"max_retries": 0}, Failure.RETRY_EXHAUSTED),
        ({"lost": False}, Failure.NOT_LOST),
        ({"death_proof": object()}, Failure.OWNER_DEAD),
    ),
)
def test_put_exhausted_not_lost_and_committed_death_are_distinct(
    fixture_kwargs: dict[str, object], failure: Failure
) -> None:
    fixture = _Fixture(**fixture_kwargs)

    reply = fixture.reducer.handle(fixture.request())

    assert reply.disposition is Disposition.FAILED
    assert reply.failure is failure
    assert reply.reconstruction_attempt is None
    assert fixture.admission_calls == []


@pytest.mark.unit
def test_expected_attempt_is_a_cas_fence_unless_an_active_attempt_can_be_joined() -> None:
    fixture = _Fixture()
    next_attempt = fixture.spec.attempt_id.next()
    assert fixture.owner.advance_attempt(
        fixture.object_id,
        expected_attempt=fixture.spec.attempt_id,
        next_attempt=next_attempt,
    )

    reply = fixture.reducer.handle(fixture.request())

    assert reply.disposition is Disposition.FAILED
    assert reply.failure is Failure.EXPECTED_ATTEMPT_MISMATCH
    assert fixture.admission_calls == []


@pytest.mark.unit
def test_borrowed_compatibility_form_normalizes_to_typed_credential() -> None:
    fixture = _Fixture()
    request = fixture.request()

    assert request.credential == BorrowedCredential(
        fixture.source, "first-borrower"
    )
    assert request.source == fixture.source
    reply = fixture.reducer.handle(request)
    assert reply.credential == request.credential
    assert reply.source == fixture.source

    keyword = Request(
        object_id=fixture.object_id,
        owner_worker_id=fixture.owner_id,
        requester_worker_id=fixture.second_requester,
        source=fixture.source,
        borrower_token="second-borrower",
        expected_owner_attempt=fixture.spec.attempt_id,
    )
    assert keyword.credential == BorrowedCredential(
        fixture.source, "second-borrower"
    )


@pytest.mark.unit
def test_active_retained_credential_can_start_after_parent_borrower_release() -> None:
    fixture = _Fixture()
    task_id = TaskID.random()
    hold = TaskReferenceHold(
        TaskReferenceHoldKind.RETAINED, fixture.first_requester, task_id,
        AttemptID(task_id, 0),
    )
    parent = (fixture.first_requester, "first-borrower")
    assert fixture.owner.retain_borrowed_reference_for_task(
        fixture.object_id, parent, hold
    )
    assert fixture.owner.release_borrowed_reference(fixture.object_id, parent)
    request = Request(
        fixture.object_id, fixture.owner_id, fixture.first_requester,
        RetainedCredential(hold), None, fixture.spec.attempt_id,
    )

    reply = fixture.reducer.handle(request)

    assert reply.disposition is Disposition.STARTED
    assert reply.credential == RetainedCredential(hold)
    assert reply.borrower_token is None
    assert reply.source is None


@pytest.mark.unit
def test_released_retained_credential_is_rejected() -> None:
    fixture = _Fixture()
    task_id = TaskID.random()
    hold = TaskReferenceHold(
        TaskReferenceHoldKind.RETAINED, fixture.first_requester, task_id,
        AttemptID(task_id, 0),
    )
    parent = (fixture.first_requester, "first-borrower")
    assert fixture.owner.retain_borrowed_reference_for_task(
        fixture.object_id, parent, hold
    )
    assert fixture.owner.release_retained_reference_for_task(
        fixture.object_id, hold
    )
    request = Request(
        fixture.object_id, fixture.owner_id, fixture.first_requester,
        RetainedCredential(hold), None, fixture.spec.attempt_id,
    )

    reply = fixture.reducer.handle(request)

    assert reply.disposition is Disposition.FAILED
    assert reply.failure is Failure.RELEASED_CREDENTIAL
    assert fixture.admission_calls == []


@pytest.mark.unit
@pytest.mark.parametrize("after_handoff", ("success", "lost-again", "next-epoch", "released"))
def test_first_ack_uses_actual_queue_handoff_after_later_state_changes(after_handoff):
    from miniray.reconstruction_runtime import ReconstructionDisposition
    fixture = _Fixture(max_retries=2)
    request = fixture.request()
    calls = []

    def admit(object_id):
        outcome = fixture.coordinator.handoff(
            fixture.coordinator.request(object_id), fixture.queue.put,
        )
        calls.append(outcome)
        plan = fixture.queue.get_nowait()
        assert outcome.disposition is ReconstructionDisposition.START
        assert plan.attempt_id == outcome.decision.attempt_id
        if after_handoff == "released":
            fixture.owner.release_borrowed_reference(
                object_id, (fixture.first_requester, request.borrower_token),
            )
        else:
            fixture.owner.publish_stored(object_id, plan.attempt_id, NodeID.random())
            fixture.recovery.record_task_success(plan.task_id, plan.attempt_id)
            assert fixture.coordinator.complete(plan.task_id, plan.attempt_id)
            if after_handoff in ("lost-again", "next-epoch"):
                fixture.owner.mark_lost(object_id, plan.attempt_id)
            if after_handoff == "next-epoch":
                next_outcome = fixture.coordinator.handoff(
                    fixture.coordinator.request(object_id), fixture.queue.put,
                )
                assert next_outcome.decision.attempt_id == plan.attempt_id.next()
                assert fixture.queue.get_nowait().attempt_id == plan.attempt_id.next()
        return outcome

    fixture.reducer._admit = admit
    reply = fixture.reducer.handle(request)
    assert reply.disposition is Disposition.STARTED
    assert reply.reconstruction_attempt == fixture.spec.attempt_id.next()
    assert fixture.reducer.handle(request) is reply
    assert len(calls) == 1 and fixture.queue.empty()


@pytest.mark.unit
def test_preview_and_committed_but_unqueued_outcomes_cannot_authorize_ack():
    from miniray.reconstruction_runtime import ReconstructionRuntimeError
    fixture = _Fixture()
    preview = fixture.coordinator.preview(fixture.object_id)
    with pytest.raises(ReconstructionRuntimeError, match="actual committed"):
        fixture.coordinator.handoff(preview, fixture.queue.put)
    committed = fixture.coordinator.request(fixture.object_id)
    assert committed.admission is None and fixture.queue.empty()
    fixture.reducer._admit = lambda _object: committed
    # The owner is already PENDING but no queue accepted the task.
    reply = fixture.reducer.handle(fixture.request())
    assert reply.failure is Failure.AUTHORITY_REJECTED
    joined = fixture.coordinator.request(fixture.object_id)
    with pytest.raises(ReconstructionRuntimeError, match="already queued"):
        fixture.coordinator.handoff(joined, fixture.queue.put)
    assert fixture.queue.empty() and not fixture.reducer._replies


@pytest.mark.unit
def test_handoff_replay_queues_once_and_rejects_changed_attempt():
    from miniray.reconstruction_runtime import ReconstructionRuntimeError
    fixture = _Fixture()
    outcome = fixture.coordinator.request(fixture.object_id)
    admitted = fixture.coordinator.handoff(outcome, fixture.queue.put)
    replay = fixture.coordinator.handoff(outcome, fixture.queue.put)
    assert admitted.admission.matches(admitted, fixture.owner, fixture.recovery)
    assert replay.admission.matches(replay, fixture.owner, fixture.recovery)
    assert fixture.queue.qsize() == 1
    wrong = replace(outcome, decision=replace(
        outcome.decision, attempt_id=outcome.decision.attempt_id.next(),
    ))
    with pytest.raises(ReconstructionRuntimeError, match="actual committed"):
        fixture.coordinator.handoff(wrong, fixture.queue.put)
    fixture.reducer._admit = lambda _object: replace(
        admitted, decision=wrong.decision,
    )
    assert fixture.reducer.handle(fixture.request()).failure is Failure.AUTHORITY_REJECTED
    assert fixture.queue.qsize() == 1


@pytest.mark.unit
def test_failed_queue_acceptance_cannot_issue_receipt_or_authorize_join():
    from miniray.reconstruction_runtime import ReconstructionRuntimeError
    fixture = _Fixture()
    outcome = fixture.coordinator.request(fixture.object_id)

    def reject(_plan):
        raise RuntimeError("queue rejected before acceptance")

    with pytest.raises(RuntimeError, match="queue rejected"):
        fixture.coordinator.handoff(outcome, reject)
    assert not fixture.coordinator._handoffs and fixture.queue.empty()
    with pytest.raises(ReconstructionRuntimeError, match="already queued"):
        fixture.coordinator.handoff(
            fixture.coordinator.request(fixture.object_id), fixture.queue.put,
        )
    admitted = fixture.coordinator.handoff(outcome, fixture.queue.put)
    assert admitted.admission.matches(admitted, fixture.owner, fixture.recovery)
    assert fixture.queue.qsize() == 1


@pytest.mark.unit
def test_system_retry_join_requires_its_own_committed_queue_handoff():
    from miniray.reconstruction_runtime import ReconstructionRuntimeError
    from miniray.errors import SystemTaskError
    from miniray.task_outputs import TaskExecution
    fixture = _Fixture(max_retries=2)
    admitted = fixture.coordinator.handoff(
        fixture.coordinator.request(fixture.object_id), fixture.queue.put,
    )
    previous = admitted.decision.attempt_id
    assert fixture.queue.get_nowait().attempt_id == previous
    assert fixture.coordinator.preflight_retry(fixture.spec.task_id, previous)
    transition = fixture.recovery.validate_task_failure(
        fixture.spec.task_id, previous, SystemTaskError("retry"),
    )
    attempt = transition.decision.attempt_id
    plan = fixture.owner.validate_advance_task_outputs(
        TaskExecution.from_task_spec(fixture.spec).for_attempt(previous), attempt,
    )
    fixture.owner.commit_validated_advance_task_outputs(plan)
    fixture.recovery.commit_transition(transition)
    joined = fixture.coordinator.request(fixture.object_id)
    with pytest.raises(ReconstructionRuntimeError, match="already queued"):
        fixture.coordinator.handoff(joined, fixture.queue.put)
    assert fixture.coordinator.handoff_retry(
        fixture.spec.task_id, previous, attempt, lambda: fixture.queue.put(attempt),
    )
    assert not fixture.coordinator.handoff_retry(
        fixture.spec.task_id, previous, attempt, lambda: fixture.queue.put(attempt),
    )
    accepted_join = fixture.coordinator.handoff(joined, fixture.queue.put)
    assert accepted_join.admission.matches(accepted_join, fixture.owner, fixture.recovery)
    assert fixture.queue.get_nowait() == attempt and fixture.queue.empty()
    assert fixture.recovery.task_record(fixture.spec.task_id).retries_started == 2


@pytest.mark.unit
def test_request_binding_conflict_is_checked_before_retired_metadata_lookup():
    fixture = _Fixture()
    request = fixture.request()
    acknowledged = fixture.reducer.handle(request)
    assert acknowledged.disposition is Disposition.STARTED

    def unavailable(_object):
        raise AssertionError("exact replay/conflict must not consult current metadata")

    fixture.reducer._snapshot_reconstruction = unavailable
    assert fixture.reducer.handle(request) is acknowledged
    changed = replace(request, credential=BorrowedCredential(
        ContainedTransferSource(replace(fixture.source.hold, transfer_token="changed-source")), request.borrower_token,
    ))
    assert fixture.reducer.handle(changed).failure is Failure.REQUEST_CONFLICT
    assert fixture.admission_calls == [fixture.object_id]


@pytest.mark.unit
def test_join_first_ack_survives_completion_and_a_later_epoch():
    fixture = _Fixture(max_retries=2)
    started = fixture.coordinator.handoff(
        fixture.coordinator.request(fixture.object_id), fixture.queue.put,
    )
    first_plan = fixture.queue.get_nowait()
    assert first_plan.attempt_id == started.decision.attempt_id
    joins = []

    def join_then_finish(object_id):
        joined = fixture.coordinator.handoff(
            fixture.coordinator.request(object_id), fixture.queue.put,
        )
        joins.append(joined)
        assert fixture.queue.empty()
        fixture.owner.publish_stored(object_id, first_plan.attempt_id, NodeID.random())
        fixture.recovery.record_task_success(first_plan.task_id, first_plan.attempt_id)
        assert fixture.coordinator.complete(first_plan.task_id, first_plan.attempt_id)
        fixture.owner.mark_lost(object_id, first_plan.attempt_id)
        fixture.coordinator.handoff(
            fixture.coordinator.request(object_id), fixture.queue.put,
        )
        assert fixture.queue.get_nowait().attempt_id == first_plan.attempt_id.next()
        return joined

    fixture.reducer._admit = join_then_finish
    request = fixture.request()
    reply = fixture.reducer.handle(request)
    assert reply.disposition is Disposition.JOINED
    assert reply.reconstruction_attempt == first_plan.attempt_id
    assert fixture.reducer.handle(request) is reply
    assert len(joins) == 1 and fixture.queue.empty()
    assert fixture.owner.snapshot(fixture.object_id).current_attempt == first_plan.attempt_id.next()
