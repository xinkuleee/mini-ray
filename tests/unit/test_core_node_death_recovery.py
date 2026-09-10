"""Threadless Core Node-death and route/cancellation reducer contracts.

The bare Core helper builds only in-memory authorities, locks and finite
queues; it starts no reference consumer, dispatcher, socket or process. Effect
paths are stubbed or fenced before I/O. Guarded late-result cases use actual
Node publication and GCS loss reducers; cancellation uses actual Node inventory
and custody ACKs. At most two tiny outputs and two 1 KiB stores are involved.
"""

from __future__ import annotations

import hashlib
import queue
import threading
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.core import (
    CoreWorker, _ForeignDependencyGuard, _HomeRoute, _LeaseCancellationState,
    _LeaseRequestAmbiguous, _LeaseRequestState, _NodeDeathObserved,
    _PendingTask, _PushRequestState, _select_home_route,
)
from miniray.errors import NodeDiedError, PlacementGroupLostError
from miniray.ids import (
    AttemptID, JobID, LeaseID, NodeID, ObjectID, PlacementGroupID, TaskID,
    WorkerID,
)
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.reconstruction_runtime import ReconstructionCoordinator
from miniray.recovery import RecoveryManager
from miniray.resources import AllocationToken, ResourceVector
from miniray.trace import EventSink


pytestmark = pytest.mark.unit


def _core() -> CoreWorker:
    core = object.__new__(CoreWorker)
    core.job_id = JobID.random()
    core.worker_id = WorkerID.random()
    core.node_id = NodeID.random()
    core.node_address = ("127.0.0.1", 28101)
    core.owner_address = ("owner.invalid", 28102)
    core.gcs_address = ("127.0.0.1", 28100)
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core.event_sink = EventSink()
    core._owner_table = ObjectOwnerTable()
    core._recovery = RecoveryManager()
    core._reconstruction = ReconstructionCoordinator(
        core._recovery, core._owner_table
    )
    core._objects = {}
    core._stored_descriptors = {}
    core._registered_functions = set()
    core._state_lock = threading.RLock()
    core._installed_cluster_snapshot = None
    core._home_route = _HomeRoute(core.node_id, core.node_address, 0)
    core._completion = threading.Condition(core._state_lock)
    core._submissions = queue.Queue()
    core._ready_tasks = queue.Queue()
    core._protocol_unresolved = {}
    core._node_death_attempts = {}
    core._dead_nodes = {}
    core._membership_epoch = 0
    core._placement_group_states = {}
    core._placement_group_manifests = {}
    core._owner_protocol_open = True
    core._inflight_borrow_ops = 0
    core._object_gc_obligations = {}
    return core


def _pending(
    core: CoreWorker, *, max_retries: int = 1, scheduling_key=None,
    foreign: bool = False,
) -> _PendingTask:
    task_id = TaskID.derive(core.job_id, core.driver_task_id, 0)
    attempt_id = AttemptID(task_id, 0)
    object_id = ObjectID.for_task(task_id)
    spec = protocol.TaskSpec(
        core.job_id, task_id, attempt_id,
        protocol.FunctionKey(core.job_id, __name__, "task", "v1"),
        (), 1, ResourceVector({"CPU": 1}), core.worker_id,
        max_retries=max_retries, scheduling_key=scheduling_key,
    )
    guards = ()
    if foreign:
        hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, core.worker_id,
            task_id, attempt_id,
        )
        guards = (
            _ForeignDependencyGuard(
                ObjectID.for_task(core.driver_task_id, 1), WorkerID.random(),
                ("127.0.0.1", 28109), core.worker_id, "borrow", hold,
            ),
        )
    pending = _PendingTask(
        object_id, spec, foreign_dependency_guards=guards
    )
    core._owner_table.register(
        object_id, current_attempt=attempt_id, producer_task_spec=spec
    )
    core._recovery.register_task(
        spec, output_ids=(object_id,), max_retries=max_retries
    )
    core._objects[object_id] = type(
        "Waiter", (), {"event": threading.Event()}
    )()
    return pending


def _death(node_id: NodeID, epoch: int = 1) -> protocol.NodeDeathRecord:
    return protocol.NodeDeathRecord(
        "death-{}".format(epoch), node_id, 12345, 1, epoch, -9,
        protocol.NodeDeathReason.PROCESS_EXIT, "test process exited",
    )


def _node_info(
    node_id: NodeID, address: tuple[str, int], *, epoch: int = 1
) -> protocol.NodeInfo:
    resources = ResourceVector({"CPU": 1})
    return protocol.NodeInfo(
        node_id, 4000 + address[1], epoch, address, resources, resources
    )


def _snapshot(
    epoch: int, *nodes: protocol.NodeInfo, name: str | None = None
) -> protocol.InstallClusterSnapshot:
    return protocol.InstallClusterSnapshot(
        epoch, name or "snapshot-{}".format(epoch), nodes
    )


def test_home_route_selector_keeps_live_home_and_migrates_deterministically() -> None:
    first = _node_info(NodeID(bytes.fromhex("01" * 16)), ("127.0.0.1", 28111))
    second = _node_info(NodeID(bytes.fromhex("02" * 16)), ("127.0.0.1", 28112))
    third = _node_info(NodeID(bytes.fromhex("03" * 16)), ("127.0.0.1", 28113))
    current = _HomeRoute(second.node_id, second.address, 1)

    assert _select_home_route(current, _snapshot(2, third, second, first)) == (
        _HomeRoute(second.node_id, second.address, 2)
    )
    assert _select_home_route(current, _snapshot(2, third, first)) == (
        _HomeRoute(first.node_id, first.address, 2)
    )
    assert _select_home_route(current, _snapshot(2)) is None


def test_home_route_selector_rejects_stale_or_conflicting_route() -> None:
    node_id = NodeID.random()
    current = _HomeRoute(node_id, ("127.0.0.1", 28121), 2)
    with pytest.raises(ValueError, match="older"):
        _select_home_route(
            current,
            _snapshot(1, _node_info(node_id, current.address)),
        )
    with pytest.raises(ValueError, match="changed its physical address"):
        _select_home_route(
            current,
            _snapshot(3, _node_info(node_id, ("127.0.0.1", 28122))),
        )


def test_death_atomically_installs_snapshot_migrates_home_and_removes_locations() -> None:
    core = _core()
    old_home = core.node_id
    survivor = NodeID.random()
    initial = _snapshot(
        1,
        _node_info(old_home, core.node_address),
        _node_info(survivor, ("127.0.0.1", 28131)),
    )
    core._home_route = _HomeRoute(old_home, core.node_address, 1)
    core._installed_cluster_snapshot = initial
    core._membership_epoch = 1
    pending = _pending(core)
    core._owner_table.publish_stored(
        pending.object_id, pending.spec.attempt_id, old_home
    )

    next_snapshot = _snapshot(
        2, _node_info(survivor, ("127.0.0.1", 28131))
    )
    removal = core.handle_node_death(
        _death(old_home, 2), next_snapshot
    )

    assert removal.lost == (pending.object_id,)
    assert core._home_route_snapshot() == _HomeRoute(
        survivor, ("127.0.0.1", 28131), 2
    )
    assert core._installed_cluster_snapshot == next_snapshot
    event = core._submissions.get_nowait()
    assert isinstance(event, _NodeDeathObserved)
    assert event.membership_epoch == 2


def test_conflicting_installed_snapshot_is_rejected_before_death_mutation() -> None:
    core = _core()
    dead = NodeID.random()
    survivor = NodeID.random()
    core._home_route = _HomeRoute(core.node_id, core.node_address, 1)
    core._installed_cluster_snapshot = _snapshot(
        1,
        _node_info(core.node_id, core.node_address),
        _node_info(dead, ("127.0.0.1", 28141)),
        _node_info(survivor, ("127.0.0.1", 28142)),
    )
    core._membership_epoch = 1
    conflicting = _snapshot(
        2, _node_info(survivor, ("127.0.0.1", 28143))
    )

    with pytest.raises(ValueError, match="changed a live Node incarnation"):
        core.handle_node_death(_death(dead, 2), conflicting)

    assert dead not in core._dead_nodes
    assert core._membership_epoch == 1
    assert core._home_route.node_id == core.node_id
    assert core._submissions.empty()


def _stored_reply(
    core: CoreWorker, pending: _PendingTask, node_id: NodeID
) -> protocol.TaskReply:
    payload = b"stored"
    return protocol.TaskReply(
        pending.spec.task_id, pending.spec.attempt_id, WorkerID.random(),
        protocol.TaskReplyStatus.SUCCEEDED,
        (
            protocol.ResultDescriptor(
                pending.object_id, protocol.ResultStorage.OBJECT_STORE,
                len(payload), core.worker_id, node_id,
                hashlib.sha256(payload).hexdigest(),
            ),
        ),
    )


def _next_pending(core: CoreWorker) -> _PendingTask:
    for _ in range(32):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if isinstance(item, _PendingTask):
            return item
    pytest.fail("Node-death fixture exceeded its finite queued-work bound")


def test_death_replay_is_exact_and_stale_or_malformed_proof_mutates_nothing() -> None:
    core = _core()
    node = NodeID.random()
    death = _death(node, 2)

    first = core.handle_node_death(death, _snapshot(2, _node_info(core.node_id, core.node_address)))
    replay = core.handle_node_death(death, _snapshot(2, _node_info(core.node_id, core.node_address)))
    assert first.node_id == replay.node_id == node
    assert core._dead_nodes == {node: death}

    other = NodeID.random()
    with pytest.raises(ValueError, match="stale membership epoch"):
        core.handle_node_death(_death(other, 1), _snapshot(1, _node_info(core.node_id, core.node_address)))
    assert other not in core._dead_nodes

    malformed = object.__new__(protocol.NodeDeathRecord)
    object.__setattr__(malformed, "detection_id", "bad")
    object.__setattr__(malformed, "node_id", NodeID.random())
    object.__setattr__(malformed, "node_pid", 0)
    object.__setattr__(malformed, "registration_epoch", 1)
    object.__setattr__(malformed, "death_epoch", 3)
    object.__setattr__(malformed, "exit_code", -9)
    object.__setattr__(malformed, "reason", protocol.NodeDeathReason.PROCESS_EXIT)
    object.__setattr__(malformed, "detail", "bad")
    with pytest.raises(Exception):
        core.handle_node_death(malformed, _snapshot(3, _node_info(core.node_id, core.node_address)))
    assert malformed.node_id not in core._dead_nodes

    pending = _pending(core)
    target = NodeID.random()
    core._mark_protocol_unresolved(
        pending, "lease_send", target_node_id=target
    )
    core._classify_node_death(_NodeDeathObserved(_death(target, 3), 3))
    assert pending.object_id not in core._node_death_attempts


def test_death_removes_only_dead_replica_then_final_replica_becomes_lost() -> None:
    core = _core()
    pending = _pending(core)
    dead = NodeID.random()
    survivor = NodeID.random()
    core._owner_table.publish_stored(
        pending.object_id, pending.spec.attempt_id, dead
    )
    core._owner_table.add_location(
        pending.object_id, pending.spec.attempt_id, survivor
    )
    core._stored_descriptors[pending.object_id] = (
        _stored_reply(core, pending, dead).results[0]
    )

    removal = core.handle_node_death(_death(dead, 1), _snapshot(1, _node_info(core.node_id, core.node_address), _node_info(survivor, ("replica.invalid", 28132))))
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert removal.surviving == (pending.object_id,)
    assert snapshot.state is ObjectState.READY_STORED
    assert snapshot.locations == frozenset({survivor})
    assert core._stored_descriptors[pending.object_id].node_id == survivor

    removal = core.handle_node_death(_death(survivor, 2), _snapshot(2, _node_info(core.node_id, core.node_address)))
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert removal.lost == (pending.object_id,)
    assert snapshot.state is ObjectState.LOST
    assert not snapshot.locations
    assert pending.object_id not in core._stored_descriptors


def test_promoted_route_does_not_change_canonical_task_reply_replay(monkeypatch) -> None:
    """One output/two tiny stores; real grant, adoption and Node-loss reducers."""
    from tests.unit.test_core_output_surviving_replica import _Fixture, _no_runtime

    _no_runtime.__wrapped__(monkeypatch)
    fixture = _Fixture(monkeypatch)
    core, pending = fixture.core, fixture.pending
    original = (fixture.envelope.result)
    output, survivor = fixture.output, fixture.target.node_id
    try:
        fixture.add_secondary()
        obligation = fixture.lose_publisher()
        assert core._drive_output_node_loss(pending, obligation)
        fixture.assert_kept()
        snapshot = core.owner_table.snapshot(output)
        assert snapshot.canonical_stored_result == original
        assert snapshot.current_attempt == pending.spec.attempt_id
        assert core._stored_descriptors[output] == replace(original, node_id=survivor)

        # Complete supplied the envelope before its publisher died. The
        # original canonical descriptor remains valid while only its mutable
        # fetch route moves to the grant-backed survivor.
        core._preflight_stored_result_replays(pending, (original,))
        with pytest.raises(Exception, match="canonical descriptor"):
            core._preflight_stored_result_replays(
                pending, (replace(original, checksum="00" * 32),)
            )
        reply = protocol.TaskReply(
            pending.task_id, pending.spec.attempt_id, fixture.publication.values.executor,
            protocol.TaskReplyStatus.SUCCEEDED, ((fixture.envelope.result,)),
            output_publication=fixture.envelope,
        )
        before_calls = tuple(fixture.calls)
        assert not core._publish_reply(
            pending, reply, expected_node_id=fixture.source.node_id,
            expected_lease_id=fixture.identity.lease_id,
        )
        assert tuple(fixture.calls) == before_calls
        after = core.owner_table.snapshot(output)
        assert after == snapshot and after.canonical_stored_result == original
        assert after.locations == frozenset({survivor})
        assert core._stored_descriptors[output] == replace(original, node_id=survivor)
        assert fixture.target.object_store.get(output) == (fixture.publication.values.payload)
    finally:
        fixture.close()


def test_dead_location_and_late_stored_result_cannot_resurrect(monkeypatch) -> None:
    """Real Complete may arrive late, but cannot reverse a latched DROP."""
    from miniray import output_protocol as wire
    from miniray.core import _OutputNodeLossObligation
    from miniray.output_handoff import OutputHandoffPhase
    from tests.unit._pure_core import close_pure_core
    from tests.unit.test_core_output_publication import _fixture as _publication
    from tests.unit.test_core_output_surviving_replica import _no_runtime

    _no_runtime.__wrapped__(monkeypatch)
    core = _core()
    pending = _pending(core)
    dead = NodeID.random()
    source = NodeID.random()
    result = _stored_reply(core, pending, source).results[0]
    core._owner_table.publish_stored(
        pending.object_id, pending.spec.attempt_id, source
    )
    core._stored_descriptors[pending.object_id] = result
    core._owner_table.add_borrowed_reference(
        pending.object_id, (core.worker_id, "borrow")
    )
    hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.RETAINED, core.worker_id,
        pending.spec.task_id, pending.spec.attempt_id,
    )
    core._owner_table.retain_borrowed_reference_for_task(
        pending.object_id, (core.worker_id, "borrow"),
        hold,
    )
    core.handle_node_death(_death(dead, 1), _snapshot(1, _node_info(core.node_id, core.node_address)))
    descriptor = protocol.ObjectStoreDescriptor(
        pending.object_id, core.worker_id, pending.spec.attempt_id, dead,
        result.size_bytes, result.checksum,
    )
    request = protocol.ReportRetainedObjectLocation(
        pending.object_id, core.worker_id, core.worker_id, hold, descriptor
    )
    assert request.hold == hold
    reply = core.report_retained_object_location(request)
    assert not reply.accepted
    assert dead not in core.owner_table.snapshot(pending.object_id).locations

    # A real Node Complete and two real child promotions exist, while the
    # owner has only registration. The first child Release loses its ACK,
    # allowing a late envelope to arrive after UNKNOWN/DISCARD was selected.
    publication, publisher, lost_core, lost, late_reply, _calls, _rpc = _publication(
        refs=True, stored=True, report_complete=False)
    survivor = NodeID(bytes(value ^ 1 for value in publisher.node_id.value))
    survivor_address = ("survivor.invalid", 1)
    lost_core.node_id, lost_core.node_address = survivor, survivor_address
    resources = ResourceVector({"CPU": 1})
    live_info = protocol.NodeInfo(survivor, 1702, 1, survivor_address, resources, resources)
    frozen = protocol.NodeDeathRecord("late-real-complete", publisher.node_id, publisher._node_pid,
        publisher._registration_epoch, 3, 1, protocol.NodeDeathReason.PROCESS_EXIT,
        "publisher exit before owner terminal delivery")
    lost_core.handle_node_death(frozen, _snapshot(3, live_info))
    obligation = _OutputNodeLossObligation(publication.id, frozen)
    releases, arrivals = [], []
    original_borrow = lost_core._borrow_rpc

    def release_then_lost_ack(address, handler, request):
        result = original_borrow(address, handler, request)
        releases.append(request)
        if len(releases) == 1:
            raise TimeoutError("first child release effect before ACK loss")
        return result

    lost_core._borrow_rpc = release_then_lost_ack
    try:
        assert publication.journal.snapshot(publication.id).complete == late_reply.output_publication.complete
        assert publication.ledger.available == publication.ledger.total
        assert publication.handoffs.query(publication.id).complete is None
        assert not lost_core._drive_output_node_loss(lost, obligation)
        assert lost_core._output_loss_choices[publication.id] is False
        before = lost_core.owner_table.snapshot(lost.object_id)
        assert before.state is ObjectState.PENDING and not before.locations
        assert publication.handoffs.query(publication.id).phase is OutputHandoffPhase.ABORTED
        arrivals.append(late_reply)
        assert not lost_core._drive_output_node_loss(lost, replace(obligation, envelope=late_reply.output_publication))
        retried = _next_pending(lost_core)
        assert retried.spec.attempt_id == lost.spec.attempt_id.next()
        assert lost_core._recovery.task_record(lost.task_id).retries_started == 1
        history = publication.handoffs.query(publication.id)
        assert history.complete is None and history.adoption is None
        assert history.phase is OutputHandoffPhase.ABORTED and arrivals == [late_reply]
        current = lost_core.owner_table.snapshot(lost.object_id)
        assert current.state is ObjectState.PENDING and not current.locations
        assert not lost_core._publish_reply(lost, late_reply, expected_node_id=publisher.node_id,
                                           expected_lease_id=publication.id.lease_id)
        assert lost_core.owner_table.snapshot(lost.object_id) == current
        assert not lost_core._stored_descriptors
        assert publication.id not in lost_core._output_result_custody
        for transfer in (publication.manifest.value).transfers:
            child = publication.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
            assert transfer.final_hold not in child.contained_holds
            assert transfer.provisional_hold not in child.contained_holds
    finally:
        for output in tuple(lost_core._objects):
            for token in tuple(lost_core.owner_table.snapshot(output).local_tokens):
                lost_core.owner_table.release_local_reference(output, token)
        close_pure_core(lost_core)


def test_push_replay_consumes_death_before_rpc_and_uses_new_attempt_and_lease() -> None:
    core = _core()
    pending = _pending(core, max_retries=1)
    dead = NodeID.random()
    old_lease = LeaseID.random()
    worker = WorkerID.random()
    grant = protocol.GrantWorkerLease(
        old_lease, pending.spec.task_id, pending.spec.attempt_id, dead, worker,
        ("127.0.0.1", 28102), AllocationToken("old-dead-grant"),
    )
    state = _PushRequestState(
        protocol.PushTask(old_lease, worker, pending.spec), grant,
        ("127.0.0.1", 28103), grant.worker_address, round=1, ambiguous=True,
    )
    core._mark_protocol_unresolved(
        pending, "push_replay_wait", target_node_id=dead
    )
    death = _death(dead, 1)
    core.handle_node_death(death, _snapshot(1, _node_info(core.node_id, core.node_address)))
    core._classify_node_death(_NodeDeathObserved(death, 1))

    core._push_task_rpc = lambda *_args: (_ for _ in ()).throw(
        AssertionError("dead Worker RPC must be fenced")
    )
    assert core._replay_push(pending, state) is False
    retried = _next_pending(core)
    assert retried.object_id == pending.object_id
    assert retried.spec.task_id == pending.spec.task_id
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    assert pending.object_id not in core._protocol_unresolved
    assert pending.object_id not in core._node_death_attempts

    leases = []

    def rpc(_address, handler, request):
        assert handler == "request_worker_lease"
        leases.append(request.lease_id)
        return protocol.RejectWorkerLease(
            request.lease_id, request.task_id, request.attempt_id,
            protocol.LeaseRejectReason.INFEASIBLE, "stop after lease identity",
            request.scheduling_key,
        )

    core._rpc = rpc
    assert core._execute(retried, retried.spec)
    assert len(leases) == 1
    assert leases[0] != old_lease


def test_cancellation_consumes_death_before_rpc(monkeypatch) -> None:
    """A selected cancellation error survives target death without retry."""
    from miniray.core import _DelayedReadyTask, _ReadyTask, _WAKE_COORDINATOR
    from miniray.output_publication import OutputPublicationID
    from miniray.ownership import ObjectCollectionState
    from tests.unit._pure_core import close_pure_core, make_pure_core
    from tests.unit.test_core_output_surviving_replica import _no_runtime

    # Apply the existing runtime prohibition only inside this exact test. A
    # module-level autouse import would silently change the legacy neighbors.
    _no_runtime.__wrapped__(monkeypatch)
    core = make_pure_core()
    core._ready_tasks = queue.Queue()
    dependency = output = None
    calls = []

    def forbidden_rpc(*args, **kwargs):
        calls.append((args, kwargs))
        pytest.fail("committed target death must fence cancellation and Push RPCs")

    def close_synchronously(reference):
        if reference is None or reference.closed:
            return
        assert reference._finalizer is not None and reference._release_done is not None
        reference._closed = True
        reference._finalizer()
        # The pure mailbox applies the actual token release synchronously; do
        # not call ObjectRef.close's Event.wait, even on an already-set event.
        assert reference._release_done.is_set()

    core._rpc = core._push_task_rpc = forbidden_rpc
    try:
        dependency = core.put(b"inline input protected until task finish")
        # Publishing the real put wakes the coordinator; consume that exact
        # event instead of mistaking it for the later task admission.
        assert core._submissions.get_nowait() is _WAKE_COORDINATOR
        core._submissions.task_done()
        assert core._submissions.empty()
        pending, output = core._register_submission(
            core.define_remote_function(lambda value: value), (dependency,), {},
            ResourceVector({"CPU": 1}), max_retries=1, _enqueue=True,
        )
        admitted = core._submissions.get_nowait()
        core._submissions.task_done()
        assert admitted is pending and core._submissions.empty()
        prepared, dependencies, protected = core._prepare_task_dependencies(pending.spec)
        assert dependencies == () and protected == (dependency.object_id,)
        assert pending.protected_dependencies == protected
        assert pending.dependency_hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert pending.dependency_hold in core.owner_table.snapshot(dependency.object_id).submitted_tokens

        dead = NodeID.random()
        lease_request = protocol.RequestWorkerLease(
            LeaseID.random(), pending.task_id, pending.spec.attempt_id, pending.spec.resources,
            core.node_id, core.worker_id, target_node_id=dead, dependencies=dependencies,
            return_ids=pending.output_ids,
        )
        request = protocol.CancelWorkerLease(
            lease_request.lease_id, lease_request.task_id, lease_request.attempt_id,
            lease_request.requester_node_id, lease_request.requester_worker_id, lease_request.scheduling_key,
        )
        original_error = RuntimeError("cancellation selected this failure before target death")
        cancellation = _LeaseCancellationState(
            request, ("dead-node.invalid", 2), original_error, target_node_id=dead,
            lease_request=lease_request,
        )
        core._mark_protocol_unresolved(
            pending, "lease_cancel", cancellation, target_node_id=dead,
            output_candidate=OutputPublicationID(lease_request.lease_id, pending.execution),
        )
        death = _death(dead, 1)
        survivors = _snapshot(1, _node_info(core.node_id, core.node_address))
        core.handle_node_death(death, survivors)
        assert core._dead_nodes[dead] == death and core._installed_cluster_snapshot == survivors
        assert core._home_route.node_id == core.node_id

        assert core._resolve_lease_cancellation(
            pending, prepared, dependencies, cancellation,
        ) is True
        snapshot = core.owner_table.snapshot(output.object_id)
        assert snapshot.state is ObjectState.ERROR and snapshot.error is original_error
        assert snapshot.current_attempt == pending.spec.attempt_id
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == pending.spec.attempt_id and record.retries_started == 0
        assert core._recovery.active_recovery(pending.task_id) is None
        assert not calls and not core._protocol_unresolved and core._ready_tasks.empty()
        assert cancellation.terminal_error is original_error and cancellation.lease_request == lease_request
        assert cancellation.reply is None and cancellation.known_grant is None
        assert core._task_finish_barriers[output.object_id] == pending
        assert pending.dependency_hold in core.owner_table.snapshot(dependency.object_id).submitted_tokens
        assert core._accepted_task_count == 1
        assert core._finish_pending_task(pending)
        assert pending.dependency_hold not in core.owner_table.snapshot(dependency.object_id).submitted_tokens
        assert core._accepted_task_count == 0 and not core._task_finish_barriers

        count = core._submissions.qsize()
        assert count <= 8
        for _ in range(count):
            item = core._submissions.get_nowait()
            core._submissions.task_done()
            assert not isinstance(item, (_PendingTask, _ReadyTask, _DelayedReadyTask))
            assert item is _WAKE_COORDINATOR or item == _NodeDeathObserved(death, survivors.membership_epoch)
        close_synchronously(dependency)
        close_synchronously(output)
        assert core._reference_mailbox.pending.qsize() <= 8
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(dependency.object_id) is ObjectCollectionState.COLLECTED
        assert core.owner_table.collection_state(output.object_id) is ObjectCollectionState.COLLECTED
        assert not core._objects and not core._object_gc_obligations and not calls
        assert core._reference_mailbox.pending.empty() and core._reference_mailbox.pending.unfinished_tasks == 0
    finally:
        close_synchronously(output)
        close_synchronously(dependency)
        close_pure_core(core)


def test_new_lease_uses_one_migrated_home_snapshot() -> None:
    core = _core()
    pending = _pending(core, max_retries=0)
    migrated = _HomeRoute(
        NodeID.random(), ("127.0.0.1", 28105), 2
    )
    core._home_route = migrated
    requests = []

    def rpc(address, handler, request):
        assert address == migrated.address
        assert handler == "request_worker_lease"
        requests.append(request)
        return protocol.RejectWorkerLease(
            request.lease_id, request.task_id, request.attempt_id,
            protocol.LeaseRejectReason.INFEASIBLE, "done",
            request.scheduling_key,
        )

    core._rpc = rpc
    assert core._execute(pending, pending.spec)
    assert len(requests) == 1
    assert requests[0].requester_node_id == migrated.node_id
    assert requests[0].preferred_node_id == migrated.node_id


def test_known_grant_cancel_keeps_frozen_requester_after_home_migration(monkeypatch) -> None:
    """One tiny Node: real Grant/Cancel/inventory ACK, no input owners."""
    from tests.unit.test_core_owner_integration import _cancellation_node as _node
    from tests.unit.test_core_output_surviving_replica import _no_runtime

    _no_runtime.__wrapped__(monkeypatch)
    core = _core()
    pending = _pending(core, max_retries=0)
    original_requester = core.node_id
    node = _node(original_requester, WorkerID.random())
    request = protocol.RequestWorkerLease(
        LeaseID.random(), pending.spec.task_id, pending.spec.attempt_id,
        pending.spec.resources, original_requester, core.worker_id,
        preferred_node_id=original_requester, target_node_id=original_requester,
        return_ids=pending.output_ids,
    )
    grant = node._handle_request_lease(request)
    assert type(grant) is protocol.GrantWorkerLease
    assert node.resource_ledger.available.is_zero()
    core._home_route = _HomeRoute(
        NodeID.random(), ("127.0.0.1", 28107), 2
    )
    calls, cancellations, custody = [], [], []
    address = ("127.0.0.1", 28108)

    def rpc(target, handler, message):
        assert target == address
        if handler == "cancel_worker_lease":
            calls.append(message)
            result = node._handle_cancel_worker_lease(message)
            assert result.accepted and result.cancelled
            assert result.retired_grant == grant
            assert result.dependency_inventory.lease_request == request
            assert result.dependency_inventory.descriptors == ()
            cancellations.append(result)
            return result
        assert handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        assert cancellations and not custody
        assert message.inventory == cancellations[0].dependency_inventory
        result = node._handle_ack_lease_dependency_custody(message)
        assert result.accepted and result.request == message
        custody.append(result)
        return result

    core._rpc = rpc
    error = NodeDiedError("terminal")
    assert core._begin_known_grant_cancellation(
        pending, pending.spec, (), address, grant, request, error,
    )
    assert len(calls) == 1
    assert calls[0].requester_node_id == original_requester
    assert calls[0].requester_node_id != core._home_route.node_id
    assert len(custody) == 1 and cancellations[0].released
    assert core.owner_table.snapshot(pending.object_id).error is error
    assert not core._protocol_unresolved
    assert node.resource_ledger.available == node.resource_ledger.total
    assert not node._dependency_custody_registry_locked().has_pending()


def test_running_remote_push_is_not_rebound_when_home_changes() -> None:
    core = _core()
    pending = _pending(core, max_retries=1)
    remote_node = NodeID.random()
    request = protocol.RequestWorkerLease(
        LeaseID.random(), pending.spec.task_id, pending.spec.attempt_id,
        pending.spec.resources, core.node_id, core.worker_id,
        preferred_node_id=core.node_id, target_node_id=remote_node,
    )
    worker = WorkerID.random()
    grant = protocol.GrantWorkerLease(
        request.lease_id, request.task_id, request.attempt_id, remote_node,
        worker, ("127.0.0.1", 28109), AllocationToken.random(),
    )
    push = protocol.PushTask(grant.lease_id, worker, pending.spec)
    state = _PushRequestState(
        push, grant, ("127.0.0.1", 28110), grant.worker_address,
        round=1, ambiguous=True, lease_request=request,
    )
    core._home_route = _HomeRoute(
        NodeID.random(), ("127.0.0.1", 28111), 2
    )
    seen = []
    core._push_task_rpc = lambda address, _handler, message: (
        seen.append((address, message.lease_id))
        or protocol.TaskReply(
            pending.spec.task_id, pending.spec.attempt_id, worker,
            protocol.TaskReplyStatus.APPLICATION_ERROR, (),
            protocol.RemoteErrorInfo("ValueError", "done", ""),
        )
    )

    assert core._replay_push(pending, state)
    assert seen == [(grant.worker_address, request.lease_id)]


@pytest.mark.parametrize("mode", ("pg", "foreign"))
def test_pg_is_terminal_but_foreign_attempt_uses_normal_system_retry(mode: str) -> None:
    core = _core()
    dead = NodeID.random()
    key = None
    if mode == "pg":
        key = protocol.PlacementGroupSchedulingKey(
            PlacementGroupID.random(), 0, 0, dead, "0" * 64
        )
    pending = _pending(
        core, max_retries=3, scheduling_key=key, foreign=mode == "foreign"
    )
    if mode == "foreign":
        assert pending.foreign_dependency_guards[0].hold == (
            protocol.TaskReferenceHold(
                protocol.TaskReferenceHoldKind.RETAINED, core.worker_id,
                pending.spec.task_id, pending.spec.attempt_id,
            )
        )
    core._mark_protocol_unresolved(
        pending, "push_replay_wait", target_node_id=dead
    )
    core.handle_node_death(_death(dead, 1), _snapshot(1, _node_info(core.node_id, core.node_address)))

    terminal = core._consume_node_death_at_lane(pending, dead)
    snapshot = core.owner_table.snapshot(pending.object_id)
    if mode == "pg":
        assert terminal is True
        assert snapshot.state is ObjectState.ERROR
        assert isinstance(snapshot.error, PlacementGroupLostError)
        assert not any(
            isinstance(item, _PendingTask)
            for item in tuple(core._submissions.queue)
        )
    else:
        assert terminal is False
        assert snapshot.state is ObjectState.PENDING
        retried = next(
            item for item in tuple(core._submissions.queue)
            if isinstance(item, _PendingTask)
        )
        assert retried.spec.attempt_id == pending.spec.attempt_id.next()
        assert retried.foreign_dependency_guards == (
            pending.foreign_dependency_guards
        )


def test_survivor_ambiguous_lease_cancels_before_terminal_pg_loss(monkeypatch) -> None:
    """Unknown Grant becomes a real no-replica cancellation before PG error."""
    from tests.unit.test_core_owner_integration import _cancellation_node as _node
    from tests.unit.test_core_output_surviving_replica import _no_runtime

    _no_runtime.__wrapped__(monkeypatch)
    core = _core()
    dead = NodeID.random()
    survivor = NodeID.random()
    pg_id = PlacementGroupID.random()
    survivor_key = protocol.PlacementGroupSchedulingKey(
        pg_id, 0, 1, survivor, "b" * 64
    )
    pending = _pending(
        core, max_retries=3, scheduling_key=survivor_key
    )
    identity = pg_id, 0
    core._placement_group_states[identity] = (
        protocol.PlacementGroupPhaseStatus.LOST
    )
    core._placement_group_manifests[identity] = (
        protocol.PlacementGroupSchedulingKey(
            pg_id, 0, 0, dead, "a" * 64
        ),
        survivor_key,
    )
    lease = protocol.RequestWorkerLease(
        LeaseID.random(), pending.spec.task_id, pending.spec.attempt_id,
        pending.spec.resources, core.node_id, core.worker_id,
        preferred_node_id=survivor, target_node_id=survivor,
        scheduling_key=survivor_key, return_ids=pending.output_ids,
    )
    state = _LeaseRequestState(
        lease, ("127.0.0.1", 28104), survivor, False
    )
    core._mark_protocol_unresolved(
        pending, "lease_replay_wait", target_node_id=survivor
    )
    node = _node(survivor, WorkerID.random())
    calls: list[protocol.CancelWorkerLease] = []
    cancellations, custody = [], []

    def rpc(address, handler, request):
        assert address == state.address
        if handler == "cancel_worker_lease":
            calls.append(request)
            result = node._handle_cancel_worker_lease(request)
            assert result.accepted and result.cancelled and not result.released
            assert result.retired_grant is None
            assert result.dependency_inventory.lease_request == lease
            assert result.dependency_inventory.descriptors == ()
            cancellations.append(result)
            return result
        assert handler == protocol.ACK_LEASE_DEPENDENCY_CUSTODY_HANDLER
        assert cancellations and not custody
        assert request.inventory == cancellations[0].dependency_inventory
        result = node._handle_ack_lease_dependency_custody(request)
        assert result.accepted and result.request == request
        custody.append(result)
        return result

    core._rpc = rpc
    terminal = core._handle_ambiguous_lease(
        pending, pending.spec, (),
        _LeaseRequestAmbiguous(state, TimeoutError("ambiguous")), 1,
    )

    assert terminal and len(calls) == 1
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert isinstance(snapshot.error, PlacementGroupLostError)
    assert snapshot.current_attempt == pending.spec.attempt_id
    assert pending.object_id not in core._protocol_unresolved
    assert pending.task_key not in core._protocol_unresolved
    assert len(custody) == 1
    assert core._recovery.task_record(pending.task_id).retries_started == 0
    assert not node._leases and node.resource_ledger.available == node.resource_ledger.total
    assert not node._dependency_custody_registry_locked().has_pending()
