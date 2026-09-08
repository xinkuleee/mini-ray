"""A live Worker owner consumes certified publisher death and retries locally.

Five startup children: one GCS, two Nodes, one Worker each. A resource-pinned
factory occupies Node A's CPU and Worker; its unconstrained child must spill
to B. The factory first opens a bounded socket handshake, then submits, making
the same listener's second connection B's real AFTER_ARM publication gate.
After observing that gate the Driver lets the factory return an A-owned child
ObjectRef, releases its outer result, and crashes the exact managed Node B.

The embedded owner on A must obtain the existing Driver-certified death view
from its local Node, resolve UNKNOWN/DROP, and retry the same child once on A.
The Driver only reads owner metadata until retry is READY; it never initiates
reconstruction or injects owner state, a death, a retry, or a cleanup receipt.

One tiny Driver put, one factory plus one child (three physical executions),
8 KiB result padding, two 1 MiB stores, one listener/two connections, one Node
crash, no extra test thread, Actor/PG or tracing. Work after init shares 15 s;
the publication gate is capped at 10 s, final reference/gate cleanup at 3 s.
Only this exact ID may run through the 30 s process-tree runner/reap grace.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import multiprocessing as mp
import os
import signal
import socket
import time

import pytest

import miniray as ray
from miniray import output_protocol as wire, protocol
from miniray.api import _get_runtime, _test_crash_node
from miniray.control import GET_WORKER_STATE_HANDLER
from miniray.ids import AttemptID, TaskID
from miniray.node import GET_OBJECT_HANDLER, SHUTDOWN_STATUS_HANDLER
from miniray.node_death_view import GET_NODE_DEATH_VIEW, GetInstalledNodeDeaths, GetInstalledNodeDeathsReply
from miniray.output_recovery import OutputRecoveryAction, OutputRecoveryOwnerDecision
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_gate import (
    OUTPUT_PUBLICATION_GATE_RELEASE, OutputPublicationGateConfig,
    OutputPublicationGatePhase, recv_output_publication_gate_arrival,
)
from miniray.publication_sources import BorrowedContainedSource
from miniray.runtime_binding import current_execution_context
from miniray.worker import GET_OWNED_OBJECT_HANDLER
from tests.integration.test_borrowed_output_unknown_path import _wait
from tests.integration.test_multi_contained_output_path import _close_local
from tests.integration.test_stored_outer_node_loss_path import (
    _assert_metadata_only, _node_loss, _pid_exists, _poll_until, _query,
    _recovery, _recv_exact, _release_connection, _remaining,
)
from tests.integration.test_stored_outer_publication_path import _close_reference


pytestmark = pytest.mark.multiprocess_smoke
_OWNER_RESOURCE = "worker_owner_node_loss_home"
_SOURCE_VALUE = ("surviving-embedded-owner-child", 42)
_PADDING = b"W" * (8 * 1024)
_START_CHILD = b"S"
_RELEASE_FACTORY = b"F"
_WORK_SECONDS = 15.0


@ray.remote(num_cpus=1, max_retries=1)
def _stored_child_of_worker_owner(container):
    child, = container
    assert isinstance(child, ray.ObjectRef) and child.borrower_token is not None
    execution = current_execution_context()
    assert execution is not None
    return {
        "child": child, "padding": _PADDING, "executor_pid": os.getpid(),
        "attempt": execution.parent_attempt_id,
    }


@ray.remote(num_cpus=1, resources={_OWNER_RESOURCE: 1}, max_retries=0)
def _worker_owner_factory(container, gate_address, deadline):
    # CPU=1 matters: Hybrid placement reads the ledger, not Worker-slot count.
    # Ordinary socket waiting does not issue a blocking-get CPU yield.
    with socket.create_connection(gate_address, timeout=_remaining(deadline)) as connection:
        connection.settimeout(_remaining(deadline))
        connection.sendall(os.getpid().to_bytes(8, "big"))
        if connection.recv(1) != _START_CHILD:
            raise RuntimeError("Driver did not start the child submission")
        child = _stored_child_of_worker_owner.remote(container)
        connection.settimeout(_remaining(deadline))
        if connection.recv(1) != _RELEASE_FACTORY:
            raise RuntimeError("Driver did not release the owner factory")
    return child


def _worker(context, worker_id, deadline):
    reply = _query(context.gcs_address, GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(worker_id), deadline)
    assert type(reply) is protocol.GetWorkerStateReply and reply.found and reply.worker_id == worker_id
    return reply


def _physical(node, object_id, deadline):
    reply = _query(node.node_address, GET_OBJECT_HANDLER, protocol.GetObject(object_id, node.node_id), deadline)
    assert type(reply) is protocol.GetObjectReply and reply.object_id == object_id and reply.node_id == node.node_id
    return reply


def test_live_worker_owner_retries_armed_child_after_certified_remote_node_death():
    listener = factory_connection = publication_connection = None
    context = core = report = death = None
    source = outer = foreign = restored_child = None
    pids, addresses = set(), set()
    close_errors = []
    phase = OutputPublicationGatePhase.AFTER_ARM_ACK_BEFORE_COMPLETE
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        address = listener.getsockname()
        addresses.add(address)
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _OWNER_RESOURCE: 1}, {"CPU": 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
            _test_output_publication_gate=OutputPublicationGateConfig(1, address, phase, 10.0),
        )
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        owner_node, publisher = context.nodes
        assert core.node_id == owner_node.node_id and context.trace_address is None
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, runtime.owner_service.address))
        for node in context.nodes:
            assert len(node.worker_ids) == 1
            addresses.update((node.node_address, node.worker_address))
        assert len(pids) == 5 and len(addresses) == 7 and os.getpid() not in pids
        source = ray.put(_SOURCE_VALUE)
        source_id = source.object_id
        initial_source = core.owner_table.snapshot(source_id)
        assert initial_source.state is ObjectState.READY_INLINE and source.owner_worker_id == core.worker_id
        owner_before = _worker(context, owner_node.worker_id, deadline)
        assert owner_before.state is protocol.WorkerMembershipState.ALIVE and owner_before.death is None
        assert owner_before.incarnation.worker_pid == owner_node.worker_pid
        assert owner_before.incarnation.node_id == owner_node.node_id and owner_before.incarnation.node_pid == owner_node.node_pid

        outer = _worker_owner_factory.remote([source], address, deadline)
        listener.settimeout(_remaining(deadline))
        factory_connection, _ = listener.accept()
        assert int.from_bytes(_recv_exact(factory_connection, 8, deadline), "big") == owner_node.worker_pid
        factory_connection.settimeout(_remaining(deadline))
        factory_connection.sendall(_START_CHILD)
        listener.settimeout(_remaining(deadline))
        publication_connection, _ = listener.accept()
        publication_connection.settimeout(_remaining(deadline))
        arrival = recv_output_publication_gate_arrival(publication_connection)
        publication = arrival.publication_id
        assert arrival.phase is phase
        assert (arrival.node_id, arrival.node_pid, arrival.registration_epoch) == (
            publisher.node_id, publisher.node_pid, runtime.nodes[1].registration_epoch,
        )
        _assert_metadata_only(arrival)
        output_id, = publication.output_ids
        assert publication.full_output_ids == (output_id,) and publication.attempt_id == AttemptID(output_id.task_id, 0)
        # The child belongs to the factory attempt's deterministic child namespace.
        expected_task = TaskID.derive(core.job_id, TaskID.derive(core.job_id, outer.object_id.task_id, 0), 0)
        assert output_id.task_id == expected_task
        before = _recovery(context, publication, deadline)
        manifest = before.manifest
        assert manifest.manifest_digest == arrival.manifest_digest
        assert manifest.header.owner_worker_id == owner_node.worker_id != core.worker_id
        assert manifest.header.executor_worker_id == publisher.worker_id
        assert before.armed and before.complete is None and before.recovery_action is OutputRecoveryAction.COMPLETION_UNKNOWN
        assert before.adopted is before.owner_decision is before.resolution is before.frozen_node_death is before.owner_death is None
        slot, = manifest.slots
        assert slot.tier is protocol.ResultStorage.OBJECT_STORE and len(_PADDING) < slot.size_bytes < 32 * 1024
        transfer, = slot.transfers
        assert type(transfer.source) is BorrowedContainedSource
        assert (transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.contained_owner_address) == (
            source_id, core.worker_id, runtime.owner_service.address,
        )
        assert transfer.source.borrower_worker_id == publisher.worker_id
        assert type(transfer.source.original_source) is protocol.TaskHoldSource
        hold = transfer.source.original_source.hold
        assert hold.kind is protocol.TaskReferenceHoldKind.RETAINED
        assert (hold.submitting_worker_id, hold.task_id, hold.origin_attempt_id) == (
            owner_node.worker_id, expected_task, publication.attempt_id,
        )
        assert transfer.final_hold.container_owner_worker_id == owner_node.worker_id
        assert transfer.provisional_hold.container_owner_worker_id == publisher.worker_id
        at_arm = core.owner_table.snapshot(source_id)
        assert transfer.final_hold in at_arm.contained_holds and transfer.source.owner_table_token in at_arm.borrowed_tokens
        assert hold in at_arm.retained_tokens and at_arm.inline_data == initial_source.inline_data
        stored = _physical(publisher, output_id, deadline)
        assert stored.found and stored.sealed and stored.producer_attempt_id == publication.attempt_id
        assert stored.owner_worker_id == owner_node.worker_id and stored.size_bytes == slot.size_bytes
        assert stored.checksum == slot.checksum == hashlib.sha256(stored.data).hexdigest()

        factory_connection.settimeout(_remaining(deadline))
        factory_connection.sendall(_RELEASE_FACTORY)
        foreign = ray.get(outer, timeout=_remaining(deadline))
        factory_connection.close()
        factory_connection = None
        assert isinstance(foreign, ray.ObjectRef) and foreign.object_id == output_id
        assert foreign.owner_worker_id == owner_node.worker_id and foreign.owner_address == owner_node.worker_address
        assert foreign.borrower_token is not None and not core.owner_table.contains(output_id)
        _wait(core, lambda: outer.object_id not in core._task_finish_barriers, deadline)
        owned_request = protocol.GetOwnedObject(output_id, owner_node.worker_id, core.worker_id, foreign.borrower_token)

        def owned():
            reply = _query(owner_node.worker_address, GET_OWNED_OBJECT_HANDLER, owned_request, deadline)
            assert type(reply) is protocol.GetOwnedObjectReply and reply.accepted
            assert (reply.object_id, reply.owner_worker_id, reply.borrower_worker_id, reply.borrower_token) == (
                owned_request.object_id, owned_request.owner_worker_id, owned_request.borrower_worker_id, owned_request.borrower_token,
            )
            return reply

        pending = owned()
        assert pending.state is protocol.OwnedObjectState.PENDING and pending.current_attempt == publication.attempt_id
        assert pending.data is pending.descriptor is None
        # The independent Driver borrower now protects the Worker-owned output.
        # Closing the factory container neither waits for nor completes it.
        _close_local(outer, deadline)
        _wait(core, lambda: core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED, deadline)
        assert _worker(context, owner_node.worker_id, deadline).incarnation == owner_before.incarnation

        death = _test_crash_node(publisher.node_id, timeout=_remaining(deadline))
        assert (death.node_id, death.node_pid, death.registration_epoch) == (arrival.node_id, arrival.node_pid, arrival.registration_epoch)
        assert death.exit_code == -signal.SIGKILL and death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        publication_connection.close()
        publication_connection = None
        query = GetInstalledNodeDeaths(owner_node.node_id)
        certified = _query(owner_node.node_address, GET_NODE_DEATH_VIEW, query, deadline)
        assert type(certified) is GetInstalledNodeDeathsReply and certified.request == query and certified.view is not None
        certified = replace(certified)
        assert certified.view.deaths == (death,)
        assert tuple(info.node_id for info in certified.view.snapshot.nodes) == (owner_node.node_id,)
        assert len(certified.view.survivor_acks) == 1 and certified.view.survivor_acks[0].installed is True
        assert certified.view.snapshot == runtime.latest_snapshot

        def resolved_loss():
            reply = _node_loss(context, publication, owner_node.worker_id, death, deadline)
            return reply if reply.snapshot.resolution is not None else None

        terminal = _poll_until(resolved_loss, deadline, "embedded owner never resolved certified publisher death")
        assert terminal.work.snapshot == replace(before, frozen_node_death=death)
        assert terminal.work.action is OutputRecoveryAction.COMPLETION_UNKNOWN
        resolved = terminal.snapshot
        assert resolved.complete is None and resolved.resolution.complete is None and resolved.resolution.kept_slots == ()
        assert resolved.owner_decision.owner_worker_id == resolved.resolution.owner_worker_id == owner_node.worker_id
        assert resolved.resolution.node_death == death and resolved.resolution.publication_id == publication
        decision, = resolved.owner_decision.slots
        assert (decision.slot_index, decision.object_id, decision.decision) == (0, output_id, OutputRecoveryOwnerDecision.DROP)
        assert resolved.owner_death is resolved.owner_cleaned is None
        assert all(core.owner_table.contained_release_was_seen(source_id, pin)
                   for pin in (transfer.final_hold, transfer.provisional_hold))

        def retry_ready():
            reply = owned()  # Read-only owner query; no ray.get/wait reconstruction.
            assert reply.current_attempt in (publication.attempt_id, publication.attempt_id.next())
            assert reply.state is not protocol.OwnedObjectState.ERROR
            return reply if reply.state is protocol.OwnedObjectState.READY_STORED else None

        recovered = _poll_until(retry_ready, deadline, "live Worker owner did not retry on its surviving home Node")
        assert recovered.current_attempt == publication.attempt_id.next()
        assert recovered.descriptor.node_id == owner_node.node_id
        assert recovered.descriptor.owner_worker_id == owner_node.worker_id
        assert recovered.descriptor.producer_attempt_id == recovered.current_attempt and recovered.data is None
        owner_after = _worker(context, owner_node.worker_id, deadline)
        assert owner_after.state is protocol.WorkerMembershipState.ALIVE and owner_after.death is None
        assert owner_after.incarnation == owner_before.incarnation and _pid_exists(owner_node.worker_pid)
        assert not core.owner_table.contains(output_id)  # Ownership never migrates to Driver.
        value = ray.get(foreign, timeout=_remaining(deadline))
        restored_child = value["child"]
        assert value["padding"] == _PADDING and value["executor_pid"] == owner_node.worker_pid
        assert value["attempt"] == recovered.current_attempt
        assert isinstance(restored_child, ray.ObjectRef) and restored_child.object_id == source_id
        assert restored_child.owner_worker_id == core.worker_id and restored_child.borrower_token is None
        assert ray.get(restored_child, timeout=_remaining(deadline)) == _SOURCE_VALUE
        physically_recovered = _physical(owner_node, output_id, deadline)
        descriptor = recovered.descriptor
        assert physically_recovered.found and physically_recovered.sealed
        assert physically_recovered.producer_attempt_id == descriptor.producer_attempt_id
        assert physically_recovered.owner_worker_id == descriptor.owner_worker_id
        assert physically_recovered.size_bytes == descriptor.size_bytes
        assert physically_recovered.checksum == descriptor.checksum == hashlib.sha256(physically_recovered.data).hexdigest()
        surviving_source = core.owner_table.snapshot(source_id)
        assert hold in surviving_source.retained_tokens  # Foreign lineage spans the SYSTEM retry.
        assert transfer.final_hold not in surviving_source.contained_holds
        assert len(surviving_source.contained_holds) == 1
        (retry_hold,) = surviving_source.contained_holds
        assert retry_hold.container_object_id == output_id and retry_hold.container_owner_worker_id == owner_node.worker_id
        assert retry_hold != transfer.final_hold
        assert _recovery(context, publication, deadline) == resolved
        assert _node_loss(context, publication, owner_node.worker_id, death, deadline).work == terminal.work

        _close_local(restored_child, deadline)
        _close_reference(foreign, deadline)

        def source_holds_drained():
            snapshot = core.owner_table.snapshot(source_id)
            return snapshot if (not snapshot.contained_holds and not snapshot.borrowed_tokens
                                and not snapshot.retained_tokens and not snapshot.submitted_tokens) else None

        released_source = _wait(core, source_holds_drained, deadline)
        assert released_source.local_tokens == initial_source.local_tokens and released_source.inline_data == initial_source.inline_data
        absent = _poll_until(lambda: reply if not (reply := _physical(owner_node, output_id, deadline)).found else None,
                             deadline, "Worker-owned retry output did not physically collect")
        assert absent.data is None and not absent.sealed
        rejected = _query(owner_node.worker_address, GET_OWNED_OBJECT_HANDLER, owned_request, deadline)
        assert type(rejected) is protocol.GetOwnedObjectReply and not rejected.accepted
        _close_local(source, deadline)
        _wait(core, lambda: core.owner_table.collection_state(source_id) is ObjectCollectionState.COLLECTED, deadline)
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations

        def survivor_clean():
            status = _query(owner_node.node_address, SHUTDOWN_STATUS_HANDLER,
                            protocol.ShutdownStatusRequest("inspect-worker-owner-node-loss"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested
            return status if status.resources_clean else None

        assert _poll_until(survivor_clean, deadline, "Worker owner Node cleanup did not settle").child_pids == (owner_node.worker_pid,)
        assert not _pid_exists(publisher.node_pid) and not _pid_exists(publisher.worker_pid)
    finally:
        cleanup_deadline = time.monotonic() + 3.0
        try:
            _release_connection(publication_connection, OUTPUT_PUBLICATION_GATE_RELEASE, cleanup_deadline)
            _release_connection(factory_connection, _RELEASE_FACTORY, cleanup_deadline)
            if listener is not None:
                listener.close()
            for reference in (restored_child, foreign, outer, source):
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            report = ray.shutdown()

    assert not close_errors and context is not None and death is not None and report is not None
    assert not ray.is_initialized() and report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert not report.gcs_forced and not report.forced
    assert report.node_exitcodes == (0, death.exit_code) and report.node_cleans == (True, False)
    assert report.node_forced == (False, False) and report.node_finalized == (True, False)
    assert report.worker_exitcodes == (0, None) and report.worker_cleans == (True, False)
    assert report.worker_forced == (False, False) and report.node_deaths[1] == death
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert not report.node_clean and not report.worker_clean
    assert not report.finalized and not report.shutdown_ack_clean and not report.resources_clean
    _poll_until(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0,
                "managed process survived Worker owner Node-loss shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
