"""Pure pre-Push loss uses the current owner-local output handoff domain.

Four synchronous cases each admit one ordinary task and one output. A real
NodeRegistry commits death and Core installs its complete survivor snapshot.
Lease replies are boundary doubles; no Worker executes and no runtime starts.
One retry is allowed, followed by one explicit stale-obligation replay.
"""

from dataclasses import replace
import queue

import pytest

from miniray import protocol
from miniray.control import NodeRegistry
from miniray.core import (
    _HomeRoute, _LeaseCancellationState, _LocationReportState,
    _NodeDeathObserved, _WAKE_COORDINATOR,
)
from miniray.ids import LeaseID, WorkerID
from miniray.output_handoff import OutputHandoffPhase
from miniray.output_publication import OutputPublicationID
from miniray.ownership import ObjectState
from miniray.recovery import TaskState
from miniray.resources import AllocationToken, ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    import multiprocessing.process
    import socket
    import subprocess
    import threading
    import time
    from miniray import control, core as core_module, transport
    from miniray.core import CoreWorker
    from miniray.node import NodeServer
    from miniray.worker import WorkerServer

    def forbidden(*args, **kwargs):
        pytest.fail("pure lease-domain test attempted runtime or unmodelled work")

    for kind, method in ((CoreWorker, "__init__"), (NodeServer, "__init__"),
                         (WorkerServer, "__init__"), (control.GCSLite, "__init__"),
                         (threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Condition, "wait"),
                         (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
                         (multiprocessing.process.BaseProcess, "start"),
                         (multiprocessing.process.BaseProcess, "join")):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)

    def settled_receipt_only(event, timeout=None):
        # The actual synchronous release must settle before close inspects it.
        assert event.is_set(), "pure release receipt attempted a blocking wait"
        return True

    monkeypatch.setattr(threading.Event, "wait", settled_receipt_only)


def _fixture():
    core = make_pure_core()
    core._ready_tasks = queue.Queue()
    core._registered_functions = set()
    nodes = NodeRegistry()
    registration = nodes.register_message(protocol.RegisterNode(
        core.node_id, 1201, core.node_address,
        ResourceVector({"CPU": 1}), ResourceVector({"CPU": 1}),
    ))
    assert registration.accepted
    epoch, live = nodes.live_snapshot()
    core._membership_epoch = epoch
    core._installed_cluster_snapshot = protocol.InstallClusterSnapshot(
        epoch, "pre-push-live", live,
    )
    core._home_route = _HomeRoute(core.node_id, core.node_address, epoch)
    pending, ref = core._register_submission(
        core.define_remote_function(lambda: 7), (), {},
        ResourceVector({"CPU": 1}), max_retries=1, _enqueue=True,
    )
    assert core._submissions.get_nowait() is pending
    core._submissions.task_done()
    assert core._submissions.empty()
    assert core._accepted_task_count == 1
    assert core._task_finish_barriers == {ref.object_id: pending}
    return core, pending, ref, nodes, registration


def _install_death(core, nodes, registration):
    reply = nodes.report_death(protocol.ReportNodeDeath(
        "pre-push-exit", registration.node_id, registration.node_pid,
        registration.registration_epoch, 5,
        protocol.NodeDeathReason.PROCESS_EXIT, "confirmed Node exit",
    ))
    assert reply.disposition is protocol.NodeDeathDisposition.APPLIED
    assert nodes.get(registration.node_id).death == reply.death
    epoch, live = nodes.live_snapshot()
    snapshot = protocol.InstallClusterSnapshot(epoch, "pre-push-dead", live)
    assert live == () and epoch == reply.membership_epoch
    removal = core.handle_node_death(reply.death, snapshot)
    assert removal.lost == removal.surviving == removal.collecting == ()
    observed = core._submissions.get_nowait()
    core._submissions.task_done()
    assert observed == _NodeDeathObserved(reply.death, epoch)
    assert core._submissions.empty()
    core._classify_node_death(observed)
    assert core._submissions.get_nowait() is _WAKE_COORDINATOR
    core._submissions.task_done()
    assert core._submissions.empty()
    assert core._dead_nodes == {registration.node_id: reply.death}
    assert core._home_route is None
    return reply.death


@pytest.mark.parametrize(
    "phase", ("lease-send", "grant-before-push", "location-replay", "cancel"),
)
def test_pre_push_loss_queries_one_output_domain_before_budgeted_retry(phase, monkeypatch):
    core, pending, ref, nodes, registration = _fixture()
    before_owner = core.owner_table.snapshot(ref.object_id)
    before_record = replace(core._recovery.task_record(pending.task_id))
    lease = LeaseID.random()
    request = protocol.RequestWorkerLease(
        lease, pending.task_id, pending.spec.attempt_id, pending.spec.resources,
        core.node_id, core.worker_id, preferred_node_id=core.node_id,
        return_ids=pending.output_ids, requester_owner_address=core.owner_address,
    )
    grant = protocol.GrantWorkerLease(
        lease, pending.task_id, pending.spec.attempt_id, core.node_id,
        WorkerID.random(), ("worker.invalid", 1), AllocationToken("pre-push-grant"),
    )
    table = core._output_handoff_table()
    real_query = table.query
    queries, received_leases, deaths = [], [], []

    def query(identity):
        queries.append(identity)
        expected_lease = received_leases[0].lease_id if received_leases else lease
        assert identity == OutputPublicationID(expected_lease, pending.execution)
        assert identity.output_ids == (ref.object_id,)
        assert core.owner_table.snapshot(ref.object_id) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        assert core._task_finish_barriers == {ref.object_id: pending}
        assert core._accepted_task_count == 1 and len(queries) == 1
        result = real_query(identity)
        assert result is None  # No owner registration can authorize child effects.
        return result

    monkeypatch.setattr(table, "query", query)

    def lease_rpc(state):
        received_leases.append(state.request)
        assert len(received_leases) == 1
        assert state.request == replace(request, lease_id=state.request.lease_id)
        assert state.expected_node_id == core.node_id
        assert state.address == core.node_address and state.allow_spillback
        marker = core._protocol_unresolved[pending.task_key]
        assert marker.output_candidate == OutputPublicationID(state.request.lease_id, pending.execution)
        assert marker.phase == "lease_send"
        deaths.append(_install_death(core, nodes, registration))
        assert core.owner_table.snapshot(ref.object_id) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        if phase == "lease-send":
            raise TimeoutError("lease reply lost after committed Node exit")
        return replace(grant, lease_id=state.request.lease_id)

    core._request_lease_hop = lease_rpc
    try:
        if phase in ("location-replay", "cancel"):
            deaths.append(_install_death(core, nodes, registration))
        if phase == "cancel":
            terminal_error = RuntimeError("unresolved lease was already cancelled")
            cancellation = _LeaseCancellationState(
                protocol.CancelWorkerLease(lease, pending.task_id, pending.spec.attempt_id,
                                           core.node_id, core.worker_id),
                core.node_address, terminal_error, target_node_id=core.node_id,
                lease_request=request, known_grant=grant,
            )
            # Death discharges this exact cancellation; it cannot rerun user code.
            assert core._resolve_lease_cancellation(pending, pending.spec, (), cancellation)
            owner = core.owner_table.snapshot(ref.object_id)
            record = core._recovery.task_record(pending.task_id)
            assert owner.state is ObjectState.ERROR and owner.error is terminal_error
            assert owner.current_attempt == pending.spec.attempt_id
            assert owner.local_tokens == before_owner.local_tokens
            assert record.current_attempt == before_record.current_attempt
            assert record.retries_started == 0 and record.retries_remaining == 1
            assert record.state is TaskState.SYSTEM_FAILED
            assert not queries and not received_leases and table.snapshots() == ()
            assert not core._protocol_unresolved and core._ready_tasks.empty()
            assert core._submissions.get_nowait() is _WAKE_COORDINATOR
            core._submissions.task_done()
            assert core._submissions.empty()
            assert core._finish_pending_task(pending)
            assert core._accepted_task_count == 0 and not core._task_finish_barriers
            assert core._submissions.get_nowait() is _WAKE_COORDINATOR
            core._submissions.task_done()
            assert core._submissions.empty()
            return

        location = (_LocationReportState(grant, core.node_address, (), lease_request=request)
                    if phase == "location-replay" else None)
        assert not core._execute(pending, pending.spec, location_state=location)
        marker = core._protocol_unresolved[pending.task_key]
        expected_lease = received_leases[0].lease_id if received_leases else lease
        identity = OutputPublicationID(expected_lease, pending.execution)
        assert marker.output_candidate == identity and marker.phase == "output_node_loss"
        assert not hasattr(marker, "inline_candidate") and not hasattr(marker, "stored_candidate")
        ready = core._ready_tasks.get_nowait()
        core._ready_tasks.task_done()
        assert core._ready_tasks.empty()
        assert ready.output_node_loss is marker.obligation
        assert ready.output_node_loss.publication_id == identity
        assert ready.output_node_loss.node_death == deaths[0]
        assert ready.output_node_loss.envelope is None
        assert not queries and table.snapshots() == ()
        assert core.owner_table.snapshot(ref.object_id) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        assert not core._finish_pending_task(pending)
        assert core._accepted_task_count == 1 and core._submissions.empty()

        assert not core._execute(pending, pending.spec, output_node_loss=ready.output_node_loss)
        assert queries == [identity]
        aborted = real_query(identity)
        assert aborted.phase is OutputHandoffPhase.ABORTED
        assert aborted.manifest is aborted.complete is aborted.adoption is None
        assert not core._protocol_unresolved
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == pending.spec.attempt_id.next()
        assert record.retries_started == 1 and record.retries_remaining == 0
        assert record.state is TaskState.RETRY_PENDING
        owner = core.owner_table.snapshot(ref.object_id)
        assert owner.current_attempt == record.current_attempt
        assert owner.state is ObjectState.PENDING and owner.local_tokens == before_owner.local_tokens
        retried = core._submissions.get_nowait()
        core._submissions.task_done()
        assert core._submissions.empty()
        assert retried.spec == replace(pending.spec, attempt_id=record.current_attempt)
        assert retried.execution.manifest == pending.execution.manifest
        assert retried.dependency_hold == pending.dependency_hold
        assert core._task_finish_barriers == {ref.object_id: retried}
        assert core._accepted_task_count == 1 and not core._finish_pending_task(pending)

        # An old loss replay must not query, charge again, or clear a successor.
        successor_id = OutputPublicationID(LeaseID.random(), retried.execution)
        core._mark_protocol_unresolved(retried, "lease_send", output_candidate=successor_id)
        successor = core._protocol_unresolved[retried.task_key]
        before_replay = replace(record)
        assert core._execute(pending, pending.spec, output_node_loss=ready.output_node_loss)
        assert queries == [identity] and real_query(identity) == aborted
        assert core._recovery.task_record(pending.task_id) == before_replay
        assert core.owner_table.snapshot(ref.object_id) == owner
        assert core._protocol_unresolved[retried.task_key] is successor
        assert core._task_finish_barriers == {ref.object_id: retried}
        assert core._accepted_task_count == 1 and core._submissions.empty()
        assert bool(received_leases) is (phase != "location-replay")
    finally:
        ref.close(timeout=0)
        close_pure_core(core)
