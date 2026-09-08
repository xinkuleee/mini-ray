"""Bounded unified stored-output publication Node-loss acceptance.

Four semantic gates distinguish INTENT before effects, acknowledged promotions
before ARM, ARM without known Complete, and successful Complete before delivery.
The first three require cleanup before a budgeted system retry; the fourth
must first become LOST/SUCCEEDED with no retry, then reconstruct on explicit get.
GCS stores immutable metadata, not per-effect bitmaps or output bytes. Gate
identity, Node replica reads, graph manifests, frozen recovery work, and exact
cleanup resolution replace assumptions about a separate stored-only journal.

Static bounds per exact 30-second runner invocation: one GCS, two Nodes and one
Worker per Node (five startup children); one local blocker, one producer with
at most two physical attempts, two tiny executor-owned puts, 64 KiB padding,
and a 1 MiB ObjectStore per Node. One test-owned loopback listener, no tracing,
Actor, placement group, extra test thread, or fabricated death/Complete. Work
shares a ten-second monotonic deadline; all sockets, polling and reference
close waits are bounded. Finally releases gates, closes references within
three seconds, and always calls runtime shutdown, including partial startup.

Graph queries intentionally retain metadata after cleanup. Resolution is the
causal cleanup barrier, and clean survivor/GCS drain proves no active graph or
hold work remains. No test-side graph mutation is used to repair the result.
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass, replace
from enum import Enum
import hashlib
import multiprocessing as mp
import os
import signal
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import output_protocol as wire, protocol
from miniray.api import _get_runtime, _test_crash_node
from miniray.control import GET_CONTAINED_GRAPH_HANDLER, GET_WORKER_STATE_HANDLER
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import GET_OBJECT_HANDLER, REQUEST_LEASE_HANDLER
from miniray.output_publication import OutputPublicationEnvelope, OutputPublicationID
from miniray.output_recovery import OutputRecoveryAction, OutputRecoveryOwnerDecision
from miniray.ownership import ObjectState
from miniray.publication_gate import (
    OUTPUT_PUBLICATION_GATE_RELEASE, OutputPublicationGateConfig,
    OutputPublicationGatePhase, recv_output_publication_gate_arrival,
)
from miniray.recovery import TaskState
from miniray.stored_publication import OwnedContainedSource
from miniray.transport import request as rpc_request


pytestmark = pytest.mark.multiprocess_smoke

_SURVIVOR_RESOURCE = "stored_node_loss_survivor"
_BOUND_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_POLL_SECONDS = 0.01
_BLOCKER_RELEASE = b"B"
_INLINE_THRESHOLD = 1024
_OBJECT_STORE_BYTES = 1024 * 1024
_OUTER_PADDING = b"N" * (64 * 1024)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("stored-output Node-loss work exceeded its deadline")
    return remaining


def _poll_until(predicate, deadline: float, detail: str):
    wake = threading.Event()
    while True:
        _remaining(deadline)
        value = predicate()
        if value:
            return value
        remaining = deadline - time.monotonic()
        assert remaining > 0, detail
        wake.wait(min(_POLL_SECONDS, remaining))


@ray.remote(num_cpus=1, resources={_SURVIVOR_RESOURCE: 1}, max_retries=0)
def _occupy_survivor(gate_address: tuple[str, int], deadline: float) -> int:
    with socket.create_connection(gate_address, timeout=_remaining(deadline)) as connection:
        connection.settimeout(_remaining(deadline))
        connection.sendall(os.getpid().to_bytes(8, "big"))
        connection.settimeout(_remaining(deadline))
        if connection.recv(1) != _BLOCKER_RELEASE:
            raise RuntimeError("Driver closed the survivor blocker gate")
    return os.getpid()


@ray.remote(num_cpus=1, max_retries=1)
def _large_outer_with_owned_child(value: int) -> tuple[object, bytes, int]:
    child = ray.put(("stored-node-loss-child", value + 1, os.getpid()))
    return child, _OUTER_PADDING, os.getpid()


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    payload = bytearray()
    while len(payload) < size:
        connection.settimeout(_remaining(deadline))
        chunk = connection.recv(size - len(payload))
        if not chunk:
            raise RuntimeError("Worker closed the blocker gate")
        payload.extend(chunk)
    return bytes(payload)


def _assert_metadata_only(value: object) -> None:
    if isinstance(value, (AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID)):
        return
    assert not isinstance(value, (
        bytes, bytearray, memoryview, OutputPublicationEnvelope,
        protocol.ResultDescriptor, protocol.ObjectStoreDescriptor,
    )), "GCS output recovery carried data-plane bytes or descriptors"
    if is_dataclass(value) and not isinstance(value, type):
        for field in fields(value):
            _assert_metadata_only(getattr(value, field.name))
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _assert_metadata_only(item)
    else:
        assert value is None or isinstance(value, (str, int, float, bool, Enum))


def _query(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, request, connect_timeout=min(0.5, remaining),
        request_timeout=min(2.0, remaining), deadline=deadline,
    )


def _recovery(context, publication, deadline):
    request = wire.GetOutputPublicationRecovery(publication)
    reply = _query(
        context.gcs_address, wire.GET_OUTPUT_PUBLICATION_RECOVERY_HANDLER,
        request, deadline,
    )
    assert type(reply) is wire.GetOutputPublicationRecoveryReply
    assert reply.request == request and reply.found and reply.snapshot is not None
    _assert_metadata_only(reply)
    return reply.snapshot


def _node_loss(context, publication, owner_id, death, deadline):
    request = wire.GetOutputNodeLoss(publication, owner_id, death)
    reply = _query(context.gcs_address, wire.GET_OUTPUT_NODE_LOSS_HANDLER, request, deadline)
    assert type(reply) is wire.GetOutputNodeLossReply
    assert reply.request == request and reply.found
    _assert_metadata_only(reply)
    return reply


def _graph(context, publication, deadline):
    request = protocol.GetContainedGraph(publication.graph_transaction_id)
    reply = _query(context.gcs_address, GET_CONTAINED_GRAPH_HANDLER, request, deadline)
    assert type(reply) is protocol.GetContainedGraphReply and reply.request == request
    assert reply.disposition in (
        protocol.StoredPublicationQueryDisposition.FOUND,
        protocol.StoredPublicationQueryDisposition.NOT_FOUND,
    )
    _assert_metadata_only(reply)
    return reply


def _release_connection(connection, marker, deadline):
    if connection is None:
        return
    try:
        remaining = deadline - time.monotonic()
        if remaining > 0:
            connection.settimeout(min(0.2, remaining))
            connection.sendall(marker)
    except OSError:
        pass
    finally:
        connection.close()


def _close_reference(reference, deadline):
    if reference is None:
        return
    # Local and foreign ObjectRefs share the real nonblocking finalizer API;
    # only ObjectRef.close adds an unbounded wait. Preserve its release event.
    reference._closed = True
    if reference._finalizer is not None:
        reference._finalizer()
    if reference._release_done is not None:
        assert reference._release_done.wait(max(0.0, deadline - time.monotonic()))
    assert reference.closed


def test_precomplete_stored_outer_node_loss_rolls_back_then_retries_survivor() -> None:
    _run_node_loss(OutputPublicationGatePhase.AFTER_INTENT_ACK)


def test_post_effect_precomplete_stored_outer_node_loss_compensates_then_retries() -> None:
    _run_node_loss(OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK)


def test_postcomplete_stored_outer_node_loss_retires_then_reconstructs() -> None:
    _run_node_loss(OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY)


def test_armed_unknown_stored_outer_node_loss_cleans_then_retries() -> None:
    _run_node_loss(OutputPublicationGatePhase.AFTER_ARM_ACK_BEFORE_COMPLETE)


def _run_node_loss(gate_phase: OutputPublicationGatePhase) -> None:
    listener = blocker_connection = publication_connection = None
    context = runtime = core = report = death = None
    blocker = outer = child = None
    original_rpc = original_retry = None
    managed_pids, managed_addresses = set(), set()
    close_errors = []
    observations_lock = threading.Lock()
    requests, grants, cleanup_acks, retries = {}, {}, {}, {}
    grant_conflicts = set()
    postcomplete = gate_phase is OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY
    unknown = gate_phase is OutputPublicationGatePhase.AFTER_ARM_ACK_BEFORE_COMPLETE
    effects_existed = gate_phase is not OutputPublicationGatePhase.AFTER_INTENT_ACK
    expected_action = (
        OutputRecoveryAction.POSTCOMPLETE_RESOLVE if postcomplete else
        OutputRecoveryAction.COMPLETION_UNKNOWN if unknown else
        OutputRecoveryAction.PRECOMPLETE_ROLLBACK
    )
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _SURVIVOR_RESOURCE: 1}, {"CPU": 1}),
            inline_threshold=_INLINE_THRESHOLD, object_store_bytes=_OBJECT_STORE_BYTES,
            _test_output_publication_gate=OutputPublicationGateConfig(
                1, gate_address, gate_phase, _BOUND_SECONDS,
            ),
            enable_tracing=False,
        )
        deadline = time.monotonic() + _BOUND_SECONDS
        survivor, victim = context.nodes
        assert len(survivor.worker_ids) == len(victim.worker_ids) == 1
        runtime = _get_runtime()
        core = runtime.core_worker
        original_rpc, original_retry = core._rpc, core._retry_system_failure
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, runtime.owner_service.address))
        for node in context.nodes:
            managed_addresses.update((node.node_address, node.worker_address))
        assert len(managed_pids) == 5 and len(managed_addresses) == 7
        assert os.getpid() not in managed_pids and context.trace_address is None

        def inspect_rpc(address, handler, message):
            reply = original_rpc(address, handler, message)
            with observations_lock:
                if handler == REQUEST_LEASE_HANDLER and isinstance(message, protocol.RequestWorkerLease):
                    key = message.task_id, message.attempt_id.attempt_number
                    requests[key] = message
                    if isinstance(reply, protocol.GrantWorkerLease):
                        pair = message, reply
                        if grants.setdefault(key, pair) != pair:
                            grant_conflicts.add(key)
                snapshot = (
                    reply.snapshot if type(reply) is wire.OutputNodeLossReply else
                    reply.snapshot if type(reply) is wire.GetOutputNodeLossReply and reply.found else None
                )
                if snapshot is not None and snapshot.resolution is not None:
                    identity = snapshot.publication_id
                    cleanup_acks[(identity.task_id, identity.attempt_id)] = snapshot.resolution
            return reply

        def inspect_retry(pending, error):
            key = pending.task_id, pending.spec.attempt_id
            with core._state_lock:
                record = core._recovery.task_record(pending.task_id)
                owner = core.owner_table.snapshot(pending.output_ids[0])
                with observations_lock:
                    # This ACK was delivered through Core's actual RPC path,
                    # not a test query or fabricated cleanup receipt.
                    retries.setdefault(key, (
                        cleanup_acks.get(key), record.current_attempt,
                        record.retries_started, owner,
                    ))
            return original_retry(pending, error)

        core._rpc, core._retry_system_failure = inspect_rpc, inspect_retry
        blocker = _occupy_survivor.remote(gate_address, deadline)
        listener.settimeout(_remaining(deadline))
        blocker_connection, _ = listener.accept()
        assert int.from_bytes(_recv_exact(blocker_connection, 8, deadline), "big") == survivor.worker_pid

        outer = _large_outer_with_owned_child.remote(41)
        object_id = outer.object_id
        listener.settimeout(_remaining(deadline))
        publication_connection, _ = listener.accept()
        publication_connection.settimeout(_remaining(deadline))
        arrival = recv_output_publication_gate_arrival(publication_connection)
        assert arrival.phase is gate_phase
        publication = arrival.publication_id
        assert type(publication) is OutputPublicationID
        assert arrival.node_id == victim.node_id and arrival.node_pid == victim.node_pid
        assert arrival.registration_epoch == runtime.nodes[1].registration_epoch
        assert publication.task_id == object_id.task_id
        assert publication.output_ids == publication.full_output_ids == (object_id,)
        assert publication.attempt_id == AttemptID(object_id.task_id, 0)
        _assert_metadata_only(arrival)

        before = _recovery(context, publication, deadline)
        manifest = before.manifest
        assert manifest.manifest_digest == arrival.manifest_digest
        assert manifest.header.owner_worker_id == core.worker_id
        assert manifest.header.executor_worker_id == victim.worker_id
        assert manifest.header.node_incarnation.node_id == victim.node_id
        assert manifest.header.node_incarnation.node_pid == victim.node_pid
        assert manifest.header.node_incarnation.registration_epoch == arrival.registration_epoch
        assert before.armed is (postcomplete or unknown)
        assert (before.complete is not None) is postcomplete
        if postcomplete:
            assert before.complete.status is protocol.TaskReplyStatus.SUCCEEDED
            assert before.complete.publication_id == publication
            assert before.complete.manifest_digest == arrival.manifest_digest
        assert before.recovery_action is expected_action
        assert before.adopted is None and before.rollback is None
        assert before.owner_decision is None and before.resolution is None
        assert before.frozen_node_death is None and before.owner_death is None
        assert before.slot_collections == ()
        assert len(manifest.slots) == 1
        slot = manifest.slots[0]
        assert slot.object_id == object_id and slot.tier is protocol.ResultStorage.OBJECT_STORE
        assert len(_OUTER_PADDING) < slot.size_bytes < 128 * 1024
        assert len(slot.transfers) == 1
        transfer = slot.transfers[0]
        assert type(transfer.source) is OwnedContainedSource
        assert transfer.source.owner_worker_id == victim.worker_id
        assert transfer.contained_owner_worker_id == victim.worker_id
        assert transfer.contained_owner_address == victim.worker_address
        assert transfer.provisional_hold.container_owner_worker_id == victim.worker_id
        assert transfer.final_hold.container_owner_worker_id == core.worker_id
        assert transfer.final_hold.container_object_id == object_id
        graph_before = _graph(context, publication, deadline)
        if effects_existed:
            assert graph_before.disposition is protocol.StoredPublicationQueryDisposition.FOUND
            assert graph_before.manifest == manifest.to_graph_manifest()
        else:
            assert graph_before.disposition is protocol.StoredPublicationQueryDisposition.NOT_FOUND
            assert graph_before.manifest is None

        # Promotion gates are after actual replica/child effects. The INTENT
        # gate is before the first effect; metadata alone is not that proof.
        physical = _query(
            victim.node_address, GET_OBJECT_HANDLER,
            protocol.GetObject(object_id, survivor.node_id), deadline,
        )
        assert type(physical) is protocol.GetObjectReply
        assert physical.object_id == object_id and physical.node_id == victim.node_id
        if effects_existed:
            assert physical.found and physical.sealed
            assert physical.producer_attempt_id == publication.attempt_id
            assert physical.owner_worker_id == core.worker_id
            assert physical.size_bytes == slot.size_bytes and physical.checksum == slot.checksum
            assert len(physical.data) == slot.size_bytes
            assert hashlib.sha256(physical.data).hexdigest() == slot.checksum
        else:
            assert not physical.found and not physical.sealed and physical.data is None
        owner_before = core.owner_table.snapshot(object_id)
        assert owner_before.state is ObjectState.PENDING
        assert owner_before.current_attempt == publication.attempt_id
        assert owner_before.output_publication is None
        assert owner_before.canonical_stored_result is None
        assert publication not in getattr(core, "_output_result_custody", {})
        ready, remaining = ray.wait([outer], num_returns=1, timeout=0)
        assert ready == [] and remaining == [outer]

        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert death.node_id == victim.node_id and death.node_pid == victim.node_pid
        assert death.registration_epoch == arrival.registration_epoch
        assert death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        publication_connection.close()
        publication_connection = None
        _poll_until(
            lambda: not _pid_exists(victim.node_pid) and not _pid_exists(victim.worker_pid),
            deadline, "dead Node/Worker remained alive",
        )
        child_owner = _query(
            context.gcs_address, GET_WORKER_STATE_HANDLER,
            protocol.GetWorkerState(transfer.contained_owner_worker_id), deadline,
        )
        assert type(child_owner) is protocol.GetWorkerStateReply and child_owner.found
        assert child_owner.worker_id == victim.worker_id
        assert child_owner.state is protocol.WorkerMembershipState.DEAD
        assert child_owner.death.reason is protocol.WorkerDeathReason.NODE_EXIT
        assert child_owner.death.worker_pid == victim.worker_pid
        assert child_owner.death.node_pid == victim.node_pid
        assert child_owner.death.node_registration_epoch == arrival.registration_epoch

        def cleanup_finished():
            reply = _node_loss(context, publication, core.worker_id, death, deadline)
            return reply if reply.snapshot.resolution is not None else None

        terminal = _poll_until(cleanup_finished, deadline, "exact output cleanup did not converge")
        assert terminal.work.action is expected_action
        assert terminal.work.snapshot == replace(before, frozen_node_death=death)
        resolved = terminal.snapshot
        resolution = resolved.resolution
        assert resolved.manifest == manifest and resolved.frozen_node_death == death
        assert resolved.armed == before.armed and resolved.complete == before.complete
        assert resolved.adopted is None and resolved.rollback is None
        assert resolved.owner_death is None and resolved.owner_cleaned is None
        assert resolution.publication_id == publication
        assert resolution.manifest_digest == manifest.manifest_digest
        assert resolution.node_death == death and resolution.owner_worker_id == core.worker_id
        assert resolution.kept_slots == ()
        assert resolution.complete == before.complete
        assert not resolved.terminal_report_allowed
        if postcomplete or unknown:
            decision = resolved.owner_decision
            assert decision is not None and decision.complete == before.complete
            assert len(decision.slots) == 1
            assert decision.slots[0].object_id == object_id and decision.slots[0].slot_index == 0
            assert decision.slots[0].decision is OutputRecoveryOwnerDecision.DROP
        else:
            assert resolved.owner_decision is None
        assert _graph(context, publication, deadline).manifest == manifest.to_graph_manifest()
        assert _node_loss(context, publication, core.worker_id, death, deadline) == terminal
        assert _recovery(context, publication, deadline) == resolved

        if postcomplete:
            # No public get/wait after the crash: observe successful-but-lost
            # output without allowing this check to initiate reconstruction.
            def completed_as_lost():
                with core._completion:
                    owner = core.owner_table.snapshot(object_id)
                    record = core._recovery.task_record(object_id.task_id)
                    if (owner.state is ObjectState.LOST
                            and record.state is TaskState.SUCCEEDED
                            and object_id not in core._task_finish_barriers
                            and object_id.task_id not in core._protocol_unresolved):
                        return owner
                return None

            retired = _poll_until(completed_as_lost, deadline, "known Complete did not settle as LOST")
            assert retired.current_attempt == publication.attempt_id
            assert retired.canonical_stored_result is None and retired.output_publication is None
            assert not retired.locations and not retired.outgoing_contained_edges
            assert object_id not in core._stored_descriptors
            record = core._recovery.task_record(object_id.task_id)
            assert record.current_attempt == publication.attempt_id and record.retries_started == 0
            assert core._recovery.active_recovery(object_id.task_id) is None
            with observations_lock:
                assert not any(task_id == object_id.task_id for task_id, _ in retries)
                assert {attempt for task_id, attempt in requests if task_id == object_id.task_id} == {0}
        else:
            # This admission occurs before any get and while the blocker
            # still owns the only survivor CPU. UNKNOWN is not task success.
            def retry_admitted():
                with core._completion:
                    record = core._recovery.task_record(object_id.task_id)
                    return (record.current_attempt == AttemptID(object_id.task_id, 1)
                            and record.retries_started == 1
                            and record.state is TaskState.RETRY_PENDING)

            _poll_until(retry_admitted, deadline, "cleaned unsuccessful attempt did not consume one retry")
            with observations_lock:
                sample = retries[(object_id.task_id, publication.attempt_id)]
            ack, prior_attempt, prior_retries, pending_owner = sample
            assert ack == resolution
            assert prior_attempt == publication.attempt_id and prior_retries == 0
            assert pending_owner.state is ObjectState.PENDING
            assert pending_owner.current_attempt == publication.attempt_id
            assert pending_owner.output_publication is None and not pending_owner.locations
            assert pending_owner.canonical_stored_result is None
            if unknown:
                assert terminal.work.action is OutputRecoveryAction.COMPLETION_UNKNOWN
                assert resolved.complete is None and resolved.owner_decision.complete is None
                assert resolution.complete is None

        assert publication not in getattr(core, "_output_result_custody", {})
        with observations_lock:
            assert {attempt for task_id, attempt in grants if task_id == object_id.task_id} == {0}
        blocker_connection.settimeout(_remaining(deadline))
        blocker_connection.sendall(_BLOCKER_RELEASE)
        assert ray.get(blocker, timeout=_remaining(deadline)) == survivor.worker_pid
        blocker_connection.close()
        blocker_connection = None

        # Only this explicit get may reconstruct the known-successful case.
        child, padding, retry_pid = ray.get(outer, timeout=_remaining(deadline))
        assert isinstance(child, ray.ObjectRef) and padding == _OUTER_PADDING
        assert retry_pid == survivor.worker_pid
        assert child.owner_worker_id == survivor.worker_id
        assert child.owner_address == survivor.worker_address and child.borrower_token is not None
        assert child.object_id != transfer.contained_object_id
        assert ray.get(child, timeout=_remaining(deadline)) == (
            "stored-node-loss-child", 42, survivor.worker_pid,
        )
        _poll_until(
            lambda: object_id not in core._task_finish_barriers,
            deadline, "replacement output adoption did not finish",
        )
        owner = core.owner_table.snapshot(object_id)
        assert owner.state is ObjectState.READY_STORED
        assert owner.current_attempt == AttemptID(object_id.task_id, 1)
        assert owner.output_publication is not None
        new_publication = owner.output_publication.publication_id
        assert new_publication != publication
        assert new_publication.output_ids == new_publication.full_output_ids == (object_id,)
        assert owner.output_publication.manifest.header.node_incarnation.node_id == survivor.node_id
        assert owner.output_publication.slot.transfers[0].contained_object_id == child.object_id
        record = core._recovery.task_record(object_id.task_id)
        assert record.state is TaskState.SUCCEEDED and record.retries_started == 1
        assert record.retries_remaining == 0
        assert record.current_attempt == owner.current_attempt
        assert core._recovery.active_recovery(object_id.task_id) is None
        # The immutable old history remains PRECOMPLETE/UNKNOWN/Complete even
        # after another physical attempt has successfully published new bytes.
        assert _recovery(context, publication, deadline) == resolved
        assert _node_loss(context, publication, core.worker_id, death, deadline).work == terminal.work
        with observations_lock:
            assert not grant_conflicts
            task_grants = {attempt: pair for (task_id, attempt), pair in grants.items()
                           if task_id == object_id.task_id}
        assert set(task_grants) == {0, 1}
        first_request, first_grant = task_grants[0]
        next_request, next_grant = task_grants[1]
        assert first_request.task_id == next_request.task_id == object_id.task_id
        assert first_request.return_ids == next_request.return_ids == (object_id,)
        assert first_request.lease_id != next_request.lease_id
        assert (first_grant.node_id, next_grant.node_id) == (victim.node_id, survivor.node_id)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            _release_connection(publication_connection, OUTPUT_PUBLICATION_GATE_RELEASE, cleanup_deadline)
            _release_connection(blocker_connection, _BLOCKER_RELEASE, cleanup_deadline)
            if listener is not None:
                listener.close()
            for reference in (child, outer, blocker):
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            if core is not None and original_rpc is not None:
                core._rpc = original_rpc
            if core is not None and original_retry is not None:
                core._retry_system_failure = original_retry
            report = ray.shutdown()

    assert not close_errors, "bounded reference cleanup failed: {!r}".format(close_errors)
    assert context is not None and death is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert not report.gcs_forced and not report.forced
    assert report.node_exitcodes == (0, death.exit_code)
    assert report.node_cleans == (True, False) and report.node_forced == (False, False)
    assert report.node_finalized == (True, False)
    assert report.worker_exitcodes == (0, None)
    assert report.worker_cleans == (True, False) and report.worker_forced == (False, False)
    assert report.node_deaths[1] == death
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert not report.node_clean and not report.worker_clean
    assert not report.finalized and not report.shutdown_ack_clean and not report.resources_clean
    _poll_until(
        lambda: all(not _pid_exists(pid) for pid in managed_pids),
        time.monotonic() + 2.0, "managed process survived shutdown",
    )
    assert all(process.pid not in managed_pids for process in mp.active_children())
    for address in managed_addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
