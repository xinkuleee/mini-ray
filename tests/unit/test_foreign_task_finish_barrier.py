"""Pure owner/borrower projections of a pending logical-task finish.

No Core lifecycle, threads, sockets, or wall-clock waits are started here.
"""

from __future__ import annotations

from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import core as core_module, protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.core import CoreWorker, ObjectRef
from miniray.errors import SystemTaskError, UnreconstructableObjectError
from miniray.ids import AttemptID, NodeID, ObjectID, TaskID, WorkerID
from miniray.owner_reconstruction import ReconstructionDeferred
from miniray.ownership import ObjectOwnerTable, ObjectState




class _Composition:
    def __init__(self):
        self.held = False
        self.on_exit = None

    def __enter__(self):
        assert not self.held
        self.held = True
        return self

    def __exit__(self, *_exc):
        self.held = False
        if self.on_exit is not None:
            self.on_exit()

    def notify_all(self):
        assert self.held


def _owner_fixture(state=ObjectState.LOST):
    core = object.__new__(CoreWorker)
    core.worker_id = WorkerID.random()
    core._owner_table = ObjectOwnerTable()
    core._completion = _Composition()
    core._state_lock = core._completion
    core._owner_protocol_open = True
    core._inflight_borrow_ops = 0
    core._task_finish_barriers = {}
    task = TaskID.random()
    object_id = ObjectID.for_task(task)
    attempt = AttemptID(task, 0)
    core._owner_table.register(object_id, current_attempt=attempt)
    if state is ObjectState.LOST:
        core._owner_table.publish_stored(object_id, attempt, NodeID.random())
        core._owner_table.mark_lost(object_id, attempt)
    elif state is ObjectState.READY_INLINE:
        core._owner_table.publish_inline(object_id, attempt, b"ready")
    borrower = WorkerID.random()
    token = (borrower, "active-borrower")
    core._owner_table.add_borrowed_reference(object_id, token)
    consumer = TaskID.random()
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, borrower, consumer,
        AttemptID(consumer, 0),
    )
    core._owner_table.retain_borrowed_reference_for_task(object_id, token, hold)
    return core, object_id, attempt, borrower, hold


def _owner_query(core, object_id, borrower, hold, retained):
    if retained:
        return core.get_retained_owned_object(protocol.GetRetainedOwnedObject(
            object_id, core.worker_id, borrower, hold
        ))
    return core.get_owned_object(protocol.GetOwnedObject(
        object_id, core.worker_id, borrower, "active-borrower"
    ))


@pytest.mark.parametrize("retained", (False, True))
@pytest.mark.unit
def test_lost_finish_gate_is_wire_pending_without_mutating_owner(retained):
    core, object_id, attempt, borrower, hold = _owner_fixture()
    core._task_finish_barriers[object_id] = object()

    pending = _owner_query(core, object_id, borrower, hold, retained)

    assert pending.accepted
    assert pending.state is protocol.OwnedObjectState.PENDING
    assert pending.current_attempt == attempt
    assert pending.data is pending.descriptor is pending.error is None
    assert core._owner_table.snapshot(object_id).state is ObjectState.LOST
    del core._task_finish_barriers[object_id]
    available = _owner_query(core, object_id, borrower, hold, retained)
    assert available.state is protocol.OwnedObjectState.LOST
    assert available.current_attempt == attempt
    assert core._inflight_borrow_ops == 0


@pytest.mark.parametrize("retained", (False, True))
@pytest.mark.unit
def test_projection_captures_snapshot_and_gate_in_one_composition(retained):
    core, object_id, _attempt, borrower, hold = _owner_fixture()
    core._task_finish_barriers[object_id] = object()
    snapshot = core._owner_table.snapshot

    def locked_snapshot(value):
        assert core._completion.held
        return snapshot(value)

    core._owner_table.snapshot = locked_snapshot
    core._completion.on_exit = core._task_finish_barriers.clear
    reply = _owner_query(core, object_id, borrower, hold, retained)
    assert reply.state is protocol.OwnedObjectState.PENDING
    assert not core._task_finish_barriers


@pytest.mark.parametrize("retained", (False, True))
@pytest.mark.unit
def test_finish_gate_never_hides_an_already_ready_inline_value(retained):
    core, object_id, attempt, borrower, hold = _owner_fixture(ObjectState.READY_INLINE)
    core._task_finish_barriers[object_id] = object()
    reply = _owner_query(core, object_id, borrower, hold, retained)
    assert reply.state is protocol.OwnedObjectState.READY_INLINE
    assert reply.current_attempt == attempt
    assert reply.data == b"ready"


@pytest.mark.unit
def test_finish_projection_does_not_bypass_borrower_validation():
    core, object_id, _attempt, borrower, _hold = _owner_fixture()
    core._task_finish_barriers[object_id] = object()
    reply = core.get_owned_object(protocol.GetOwnedObject(
        object_id, core.worker_id, borrower, "not-an-active-token"
    ))
    assert not reply.accepted
    assert reply.state is None


@pytest.mark.unit
def test_core_admission_distinguishes_deferred_from_started_outcome():
    core = object.__new__(CoreWorker)
    object_id = ObjectID.for_task(TaskID.random())
    waiter = object()
    core._object_waiter = lambda value: waiter
    calls = []
    outcome = None

    def admit(value, actual_waiter, *, return_requested_outcome):
        calls.append((value, actual_waiter, return_requested_outcome))
        return outcome

    core._start_or_join_reconstruction = admit
    with pytest.raises(ReconstructionDeferred):
        core._admit_owned_object_reconstruction(object_id)
    outcome = object()
    assert core._admit_owned_object_reconstruction(object_id) is outcome
    assert calls == [(object_id, waiter, True)] * 2


class _Clock:
    def __init__(self):
        self.now = 100.0
        self.waits = []

    def monotonic(self):
        return self.now

    def wait(self, seconds):
        assert 0 < seconds <= 0.01
        self.waits.append(seconds)
        self.now += seconds


def _borrower_fixture(monkeypatch, failure, *, eventually_ready):
    clock = _Clock()
    monkeypatch.setattr(core_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(core_module, "_BORROW_POLL_EVENT", clock)
    core = object.__new__(CoreWorker)
    core.worker_id = WorkerID.random()
    object_id = ObjectID.for_task(TaskID.random())
    attempt = AttemptID(object_id.task_id, 0)
    owner = WorkerID.random()
    ref = ObjectRef(object_id, owner, ("127.0.0.1", 39991))
    ref._borrower_token = "borrower-token"
    source_hold = ContainedReferenceHold(
        ObjectID.for_task(TaskID.random()), owner, "source"
    )
    core._active_borrower_capability = lambda _ref: SimpleNamespace(
        source=protocol.ContainedTransferSource(source_hold)
    )
    core._loads_owned_value = cloudpickle.loads
    core._blocking_scope = _Composition
    requests = []
    get_count = 0

    def rpc(_address, _handler, request, remaining):
        nonlocal get_count
        requests.append((request, remaining))
        if isinstance(request, protocol.GetOwnedObject):
            get_count += 1
            ready = eventually_ready and get_count > 1
            return protocol.GetOwnedObjectReply(
                object_id, owner, core.worker_id, ref.borrower_token, True,
                state=(protocol.OwnedObjectState.READY_INLINE if ready
                       else protocol.OwnedObjectState.LOST),
                current_attempt=attempt,
                data=cloudpickle.dumps(42) if ready else None,
            )
        assert isinstance(request, protocol.RequestOwnedObjectReconstruction)
        return protocol.RequestOwnedObjectReconstructionReply(
            request.object_id, request.owner_worker_id, request.requester_worker_id,
            request.credential, request.borrower_token, request.expected_owner_attempt,
            protocol.OwnedObjectReconstructionDisposition.FAILED,
            failure=failure, detail="injected exact owner admission outcome",
        )

    core._borrow_rpc_with_deadline = rpc
    return core, ref, clock, requests


@pytest.mark.parametrize("failure", (
    protocol.OwnedObjectReconstructionFailure.NOT_LOST,
    protocol.OwnedObjectReconstructionFailure.EXPECTED_ATTEMPT_MISMATCH,
    protocol.OwnedObjectReconstructionFailure.COLLECTION_IN_PROGRESS,
))
@pytest.mark.unit
def test_borrower_repolls_temporary_reconstruction_failure(monkeypatch, failure):
    core, ref, clock, requests = _borrower_fixture(
        monkeypatch, failure, eventually_ready=True
    )
    assert core._get_borrowed_object(ref, 1.0) == 42
    assert len(requests) == 3
    assert requests[0][0] == requests[2][0]
    assert clock.waits == [0.01]
    assert requests[2][1] < requests[0][1]


@pytest.mark.unit
def test_temporary_reconstruction_never_resets_public_timeout(monkeypatch):
    core, ref, clock, requests = _borrower_fixture(
        monkeypatch, protocol.OwnedObjectReconstructionFailure.NOT_LOST,
        eventually_ready=False,
    )
    with pytest.raises(TimeoutError):
        core._get_borrowed_object(ref, 0.025)
    assert len(clock.waits) == 3
    assert sum(clock.waits) == pytest.approx(0.025)
    deadlines = [clock_value for _request, clock_value in requests]
    assert deadlines == sorted(deadlines, reverse=True)
    reconstruction_requests = [
        request for request, _remaining in requests
        if isinstance(request, protocol.RequestOwnedObjectReconstruction)
    ]
    assert all(request == reconstruction_requests[0] for request in reconstruction_requests)


@pytest.mark.parametrize(("failure", "error"), (
    (protocol.OwnedObjectReconstructionFailure.AUTHORITY_REJECTED, SystemTaskError),
    (protocol.OwnedObjectReconstructionFailure.RETRY_EXHAUSTED, UnreconstructableObjectError),
))
@pytest.mark.unit
def test_permanent_reconstruction_failures_still_surface(monkeypatch, failure, error):
    core, ref, clock, requests = _borrower_fixture(
        monkeypatch, failure, eventually_ready=True
    )
    with pytest.raises(error):
        core._get_borrowed_object(ref, 1.0)
    assert clock.waits == []
    assert len(requests) == 2
