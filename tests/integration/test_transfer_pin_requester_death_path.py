"""Bounded real source-pin cleanup after one requester Node dies.

Five startup children: GCS, two Nodes, and one idle Worker per Node. One
Driver-owned 8 KiB put lives on A in a 1 MiB store; B also has a 1 MiB
store. The Driver explicitly sends two real source Pin/Chunk protocols,
with requester A and requester B. This is a Node-protocol test, not a
claim that B ran a dependency-reader pipeline. No user task or Actor runs.

The existing managed-Node crash helper kills only B's isolated Node/Worker
group and obtains the actual GCS death. A's existing supervisor must close
only B's session. Observe rejected dead-session Chunk and late new Pin
before replaying its exact Release: released=False proves the test did
not perform the first unpin. A's same-object session must remain readable
and prevent Drop until its own exact release. All handlers/replies are real.

No new test thread/listener, production failpoint, replacement or tracing.
18-second post-init work and three-second finally budgets; shutdown has
its own bounded drain. The outer runner starts process-tree termination
at 30 seconds plus bounded TERM/KILL/reap grace. Internal deadlines alone
do not bound startup and shutdown.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import socket
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime, _test_crash_node
from miniray.control import GET_NODE_STATE_HANDLER
from miniray.node import (
    DROP_OBJECT_REPLICA_HANDLER, GET_OBJECT_CHUNK_HANDLER, PIN_OBJECT_HANDLER,
    RELEASE_OBJECT_PIN_HANDLER, SHUTDOWN_STATUS_HANDLER,
)
from miniray.ownership import ObjectCollectionState, ObjectState
from tests.integration.test_local_replica_handoff_failure_path import (
    _node_state, _poll, _remaining, _replica, _rpc, _wait,
)
from tests.integration.test_multi_contained_output_path import _close_local
from tests.integration.test_output_owner_death_path import _pid_exists


pytestmark = pytest.mark.multiprocess_smoke
_PAYLOAD = b"P" * (8 * 1024)


def test_requester_node_death_closes_only_its_source_transfer_pin():
    context = core = source = victim = reference = death = report = None
    pids, addresses, closed_transfers = set(), set(), set()
    releases, cleanup_errors = [], []
    try:
        context = ray.init(
            num_nodes=2, num_workers_per_node=1, num_cpus=1,
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
        )
        deadline = time.monotonic() + 18.0
        runtime = _get_runtime()
        core = runtime.core_worker
        source, victim = context.nodes
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, runtime.owner_service.address, source.node_address,
                          source.worker_address, victim.node_address, victim.worker_address))
        assert len(pids) == 5 and len(addresses) == 6 and os.getpid() not in pids
        assert context.trace_address is None and core.node_id == source.node_id
        before_source = _node_state(context, source, deadline)
        before_victim = _node_state(context, victim, deadline)
        assert all(_pid_exists(pid) for pid in pids)
        reference = ray.put(_PAYLOAD)
        assert reference.owner_worker_id == core.worker_id and reference.borrower_token is None
        before = core.owner_table.snapshot(reference.object_id)
        assert before.state is ObjectState.READY_STORED and before.locations == frozenset((source.node_id,))
        result = before.canonical_stored_result
        descriptor = protocol.ObjectStoreDescriptor(
            reference.object_id, core.worker_id, before.current_attempt, source.node_id, result.size_bytes, result.checksum,
        )
        physical = _replica(source, reference.object_id, deadline)
        assert physical.found and physical.sealed and physical.size_bytes == descriptor.size_bytes
        assert (physical.owner_worker_id, physical.producer_attempt_id, physical.checksum) == (
            descriptor.owner_worker_id, descriptor.producer_attempt_id, descriptor.checksum,
        )
        assert not _replica(victim, reference.object_id, deadline).found
        assert not core._task_finish_barriers and not core._protocol_unresolved
        live_pin = protocol.PinObjectForTransfer(
            "live-requester:{}".format(reference.object_id), descriptor, source.node_id,
        )
        dead_pin = protocol.PinObjectForTransfer(
            "dead-requester:{}".format(reference.object_id), descriptor, victim.node_id,
        )
        for pin in (live_pin, dead_pin):
            # Retain exact cleanup identity before sending an effectful RPC.
            releases.append(protocol.ReleaseObjectPin(pin.transfer_id, reference.object_id, pin.requester_node_id))
            reply = _rpc(source.node_address, PIN_OBJECT_HANDLER, pin, deadline)
            assert type(reply) is protocol.PinObjectForTransferReply and reply.pinned and reply.error is None
            assert reply.transfer_id == pin.transfer_id and reply.descriptor == descriptor

        def chunk(pin):
            request = protocol.GetObjectChunk(pin.transfer_id, reference.object_id, pin.requester_node_id, 0, 128)
            reply = _rpc(source.node_address, GET_OBJECT_CHUNK_HANDLER, request, deadline)
            assert type(reply) is protocol.GetObjectChunkReply
            assert (reply.transfer_id, reply.object_id, reply.node_id, reply.offset) == (
                request.transfer_id, request.object_id, source.node_id, request.offset,
            )
            if reply.ok:
                assert reply.error is None and reply.data == physical.data[:128]
            else:
                assert reply.data == b"" and reply.error
            return reply

        def drop_is_pinned():
            request = protocol.DropObjectReplica(
                descriptor.object_id, descriptor.producer_attempt_id, descriptor.owner_worker_id,
                descriptor.node_id, descriptor.checksum,
            )
            reply = _rpc(source.node_address, DROP_OBJECT_REPLICA_HANDLER, request, deadline)
            assert type(reply) is protocol.DropObjectReplicaReply and reply.status is protocol.DropObjectReplicaStatus.PINNED
            assert (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum) == (
                request.object_id, request.producer_attempt_id, request.owner_worker_id, request.node_id, request.checksum,
            )
            assert not reply.dropped and reply.error

        def release_pin(request, *, released, bound):
            reply = _rpc(source.node_address, RELEASE_OBJECT_PIN_HANDLER, request, bound)
            assert type(reply) is protocol.ReleaseObjectPinReply and reply.accepted and reply.error is None
            assert (reply.transfer_id, reply.object_id, reply.node_id) == (
                request.transfer_id, request.object_id, source.node_id,
            )
            if released is not None:
                assert reply.released is released
            closed_transfers.add(request.transfer_id)
            return reply

        def source_status():
            status = _rpc(source.node_address, SHUTDOWN_STATUS_HANDLER,
                          protocol.ShutdownStatusRequest("inspect-requester-death-pins-only"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested and not status.finalized
            assert status.child_pids == (source.worker_pid,)
            return status

        assert chunk(live_pin).ok and chunk(dead_pin).ok
        drop_is_pinned()
        assert not source_status().resources_clean
        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert type(death) is protocol.NodeDeathRecord and death.node_id == victim.node_id
        assert death.node_pid == victim.node_pid and death.registration_epoch == before_victim.registration_epoch
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT and death.exit_code == -signal.SIGKILL
        dead_state = _rpc(context.gcs_address, GET_NODE_STATE_HANDLER, protocol.GetNodeState(victim.node_id), deadline)
        assert type(dead_state) is protocol.GetNodeStateReply and dead_state.found
        assert dead_state.node_id == victim.node_id and dead_state.node_pid == victim.node_pid
        assert dead_state.state is protocol.NodeMembershipState.DEAD and dead_state.death == death
        assert dead_state.registration_epoch == before_victim.registration_epoch
        assert _node_state(context, source, deadline).registration_epoch == before_source.registration_epoch

        def automatically_closed():
            reply = chunk(dead_pin)
            return reply if not reply.ok else None

        # Chunk only reads session state. No Driver Release has been sent, so
        # this refusal must precede any test-side attempt to close the dead pin.
        closed_reply = _poll(automatically_closed, deadline, "source supervisor did not close the dead requester session")
        assert not closed_reply.ok and not closed_transfers
        late_pin = protocol.PinObjectForTransfer(
            "late-dead-requester:{}".format(reference.object_id), descriptor, victim.node_id,
        )
        late_release = protocol.ReleaseObjectPin(late_pin.transfer_id, reference.object_id, victim.node_id)
        releases.append(late_release)
        late_reply = _rpc(source.node_address, PIN_OBJECT_HANDLER, late_pin, deadline)
        assert type(late_reply) is protocol.PinObjectForTransferReply
        assert late_reply.transfer_id == late_pin.transfer_id and late_reply.descriptor == descriptor
        assert not late_reply.pinned and late_reply.error
        closed_transfers.add(late_pin.transfer_id)
        live_release, dead_release = releases[:2]
        # released=True here would reveal that this call, not the supervisor,
        # performed the first unpin. An accepted replay alone is insufficient.
        dead_replay = release_pin(dead_release, released=False, bound=deadline)
        assert dead_replay.accepted and not dead_replay.released
        assert not chunk(dead_pin).ok and chunk(live_pin).ok
        assert _replica(source, reference.object_id, deadline).data == physical.data
        drop_is_pinned()
        assert not source_status().resources_clean
        assert core.owner_table.snapshot(reference.object_id).locations == before.locations
        release_pin(live_release, released=True, bound=deadline)
        assert not chunk(live_pin).ok
        _poll(lambda: source_status().resources_clean, deadline, "source transfer pins did not reach quiescence")
        assert ray.get(reference, timeout=_remaining(deadline)) == _PAYLOAD
        after = core.owner_table.snapshot(reference.object_id)
        assert after.state is ObjectState.READY_STORED and after.locations == before.locations
        assert after.current_attempt == before.current_attempt and after.canonical_stored_result == result
        _close_local(reference, deadline)
        _wait(core, lambda: core.owner_table.collection_state(reference.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _poll(lambda: not _replica(source, reference.object_id, deadline).found, deadline, "collected source bytes survived")
        _poll(lambda: source_status().resources_clean, deadline, "source failed to finish resource cleanup")
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations
        assert all(_pid_exists(pid) for pid in (context.gcs_pid, source.node_pid, source.worker_pid))
        assert not _pid_exists(victim.node_pid)
    finally:
        cleanup_deadline = time.monotonic() + 3.0
        try:
            for request in releases:
                if request.transfer_id in closed_transfers:
                    continue
                try:
                    reply = _rpc(source.node_address, RELEASE_OBJECT_PIN_HANDLER, request, cleanup_deadline)
                    assert type(reply) is protocol.ReleaseObjectPinReply and reply.accepted
                    assert (reply.transfer_id, reply.object_id, reply.node_id) == (
                        request.transfer_id, request.object_id, source.node_id,
                    )
                    closed_transfers.add(request.transfer_id)
                except Exception as exc:
                    cleanup_errors.append(exc)
            try:
                _close_local(reference, cleanup_deadline)
            except Exception as exc:
                cleanup_errors.append(exc)
        finally:
            report = ray.shutdown()

    assert not cleanup_errors and context is not None and report is not None and death is not None
    assert not ray.is_initialized() and report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert not report.forced and not report.gcs_forced
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.node_exitcodes == (0, death.exit_code) and report.node_cleans == (True, False)
    assert report.node_forced == (False, False) and report.node_finalized == (True, False)
    assert report.node_resources_clean == (True, False) and report.node_shutdown_ack_clean == (True, False)
    assert report.worker_exitcodes == (0, None) and report.worker_cleans == (True, False)
    assert report.worker_forced == (False, False) and report.node_deaths[1] == death
    survivor_exit = report.node_deaths[0]
    assert survivor_exit is not None and survivor_exit.node_id == source.node_id and survivor_exit.node_pid == source.node_pid
    assert survivor_exit.reason is protocol.NodeDeathReason.EXPECTED and survivor_exit.exit_code == 0
    assert not report.node_clean and not report.worker_clean and not report.resources_clean
    assert not report.finalized and not report.shutdown_ack_clean and len(pids) == 5
    _poll(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0, "managed process survived shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
