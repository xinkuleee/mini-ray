"""Bounded foreign ``wait``/``drop_object``/reconstruction smoke.

The owner task runs on the first Node and returns a still-live ObjectRef whose
producer is resource-pinned to the second Node.  The Driver waits using owner
metadata only, drops the sole replica through the Worker owner, loses the first
drop acknowledgement, and exact-replays the same operation before ``get`` asks
that owner to reconstruct the stable ObjectID.

After review and registration, run only this exact node ID through ``scripts/run_baseline.py --case EXACT``.  Static
bounds are one GCS, two NodeManagers, one ordinary Worker each, two logical
tasks and at most three physical task executions. Each Node has a 1-MiB store;
the child's stored value contains 64 KiB plus small tuple/pickle metadata.
There is no Actor, tracing, test-owned listener/thread or sleep.

Public get/wait, <=64 LOST observations and <=64 physical-GC observations
share fifteen seconds after init. Raw drop/_borrow_rpc retain their own finite
three-attempt RPC policy under the outer 30-second process-tree bound; the test
does not pretend a caller deadline cancels those inner operations. A passive
observer stores <=256 relevant exchanges, except for the original single ACK
loss after a real successful owner drop. Reference close uses a three-second
shared limit, and failure-finally always reaches shutdown and five-PID/six-
endpoint checks, including the Driver owner. True GC evidence is limited to
the local outer's collection, borrower Release convergence and absence of the
previously observed attempt-1 stored bytes, not foreign owner metadata GC.

This file has no registered smoke or migration selector in the current
manifest. Review and register the exact selector and its input closure
before using the current runner's --case route.
"""

from __future__ import annotations

from dataclasses import replace
import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
import miniray.core as core_module
from miniray.api import _get_runtime
from miniray.ids import AttemptID
from miniray.ownership import ObjectCollectionState
from miniray.transport import TransportTimeout, request as rpc_request


pytestmark = pytest.mark.multiprocess_smoke

_OWNER = "foreign_wait_drop_owner"
_PRODUCER = "foreign_wait_drop_producer"
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_OBSERVATIONS = 256
_MAX_POLLS = 64
_MAX_LOCAL_POLLS = 256
_PAYLOAD_BYTES = 64 * 1024
_PAYLOAD_BYTE = b"W"


@ray.remote(num_cpus=1, resources={_PRODUCER: 1}, max_retries=1)
def _wait_drop_child(value: int) -> tuple[object, ...]:
    return (
        "foreign-wait-drop",
        value + 1,
        os.getpid(),
        _PAYLOAD_BYTE * _PAYLOAD_BYTES,
    )


@ray.remote(num_cpus=0, resources={_OWNER: 1})
def _return_wait_drop_child(value: int) -> object:
    return _wait_drop_child.remote(value)


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("foreign wait/drop work exceeded its deadline")
    return remaining


def _close_reference(reference, deadline):
    if reference is None:
        return
    finalizer, done = reference._finalizer, reference._release_done
    assert finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _physical_probe(address, object_id, node_id, requester_node_id, deadline):
    remaining = _remaining(deadline)
    # Raw Node observation, not another public get/reconstruction or an input
    # to its observer counters. Unqualified Get proves actual absence, rather
    # than merely rejecting a caller's stale expected producer epoch.
    reply = rpc_request(
        address, "get_object", protocol.GetObject(object_id, requester_node_id),
        connect_timeout=min(0.25, remaining), request_timeout=min(1.0, remaining), deadline=deadline,
    )
    assert type(reply) is protocol.GetObjectReply
    reply = replace(reply)
    assert reply.object_id == object_id and reply.node_id == node_id
    return reply


def test_foreign_wait_drop_replay_then_owner_reconstruction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = None
    report = None
    outer = foreign = None
    core = None
    original_transport_rpc = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    close_errors = []
    observation_lock = threading.Lock()
    observation_count = 0
    observation_failed = overflow = False
    wake = threading.Event()
    phase = "bootstrap"
    owner_reads: list[
        tuple[str, protocol.GetOwnedObject, protocol.GetOwnedObjectReply]
    ] = []
    object_fetch_phases: list[str] = []
    drop_exchanges: list[
        tuple[
            object, protocol.RequestDropOwnedObject,
            protocol.RequestDropOwnedObjectReply,
        ]
    ] = []
    reconstruction_exchanges: list[
        tuple[
            object, protocol.RequestOwnedObjectReconstruction,
            protocol.RequestOwnedObjectReconstructionReply,
        ]
    ] = []
    drop_ack_lost = False

    def observe(items, record):
        nonlocal observation_count, observation_failed, overflow
        try:
            with observation_lock:
                if observation_count < _MAX_OBSERVATIONS:
                    items.append(record)
                    observation_count += 1
                else:
                    overflow = True
        except Exception:
            # Bookkeeping must not turn a real response into transport failure.
            observation_failed = True

    try:
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, _OWNER: 1},
                {"CPU": 1, _PRODUCER: 1},
            ),
            num_workers_per_node=1, inline_threshold=1024,
            object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        owner_node, producer_node = context.nodes
        assert len(owner_node.worker_ids) == len(producer_node.worker_ids) == 1
        assert context.trace_address is None
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 5 and len(managed_addresses) == 6
        assert os.getpid() not in managed_pids

        _remaining(deadline)
        outer = _return_wait_drop_child.remote(41)
        foreign = ray.get(outer, timeout=_remaining(deadline))
        assert isinstance(foreign, ray.ObjectRef)
        assert foreign.owner_worker_id == owner_node.worker_id
        assert foreign.owner_address == owner_node.worker_address
        assert foreign.borrower_token is not None
        logical_id = foreign.object_id
        borrower_token = foreign.borrower_token
        attempt_0 = AttemptID(logical_id.task_id, 0)
        attempt_1 = AttemptID(logical_id.task_id, 1)

        original_transport_rpc = core_module.rpc_request

        def inspect_transport_rpc(
            address: object, handler: str, request: object, **kwargs: object
        ) -> object:
            nonlocal drop_ack_lost
            reply = original_transport_rpc(
                address, handler, request, **kwargs
            )
            if handler == "get_owned_object":
                observe(owner_reads, (phase, request, reply))
            elif handler == "get_object":
                observe(object_fetch_phases, phase)
            elif handler == "request_drop_owned_object":
                observe(drop_exchanges, (address, request, reply))
                if (not drop_ack_lost and type(reply) is protocol.RequestDropOwnedObjectReply
                        and reply.disposition is protocol.DropOwnedObjectDisposition.DROPPED):
                    # Delivery and owner mutation committed; only the ACK is
                    # hidden.  Core must replay this exact operation identity.
                    drop_ack_lost = True
                    raise TransportTimeout(
                        "injected loss after owner drop acknowledgement"
                    )
            elif handler == "request_owned_object_reconstruction":
                observe(reconstruction_exchanges, (address, request, reply))
            return reply

        monkeypatch.setattr(
            core_module, "rpc_request", inspect_transport_rpc
        )

        phase = "wait"
        ready, remaining = ray.wait(
            [foreign, outer], num_returns=2, timeout=_remaining(deadline)
        )
        assert ready == [foreign, outer]
        assert remaining == []
        wait_reads = [item for item in owner_reads if item[0] == "wait"]
        assert wait_reads
        assert all(type(request) is protocol.GetOwnedObject and type(reply) is protocol.GetOwnedObjectReply
                   for _, request, reply in wait_reads)
        assert wait_reads[-1][2].state is protocol.OwnedObjectState.READY_STORED
        assert wait_reads[-1][2].current_attempt == attempt_0
        assert wait_reads[-1][2].data is None
        assert "wait" not in object_fetch_phases

        phase = "drop"
        _remaining(deadline)
        assert ray.drop_object(foreign)
        _remaining(deadline)
        assert drop_ack_lost
        assert len(drop_exchanges) == 2
        first_address, first_request, first_reply = drop_exchanges[0]
        replay_address, replay_request, replay_reply = drop_exchanges[1]
        assert all(type(request) is protocol.RequestDropOwnedObject
                   and type(reply) is protocol.RequestDropOwnedObjectReply
                   for _, request, reply in drop_exchanges)
        assert first_address == replay_address == foreign.owner_address
        assert first_request == replay_request
        assert first_reply == replay_reply
        assert first_request.operation_id
        assert first_request.object_id == first_reply.object_id == logical_id
        assert first_request.owner_worker_id == first_reply.owner_worker_id == (
            foreign.owner_worker_id
        )
        assert first_request.requester_worker_id == (
            first_reply.requester_worker_id
        ) == core.worker_id
        assert first_request.borrower_token == first_reply.borrower_token == (
            borrower_token
        )
        assert first_request.source == first_reply.source
        assert first_request.expected_owner_attempt == (
            first_reply.expected_owner_attempt
        ) == attempt_0
        assert first_request.node_id is first_reply.requested_node_id is None
        assert first_reply.disposition is (
            protocol.DropOwnedObjectDisposition.DROPPED
        )
        assert first_reply.dropped_node_id == producer_node.node_id
        assert first_reply.failure is None and first_reply.detail is None

        phase = "lost-probe"
        lost_request = protocol.GetOwnedObject(
            logical_id, foreign.owner_worker_id, core.worker_id, borrower_token
        )
        # A just-completed producer may still own its finish barrier. The
        # current owner contract temporarily projects LOST as PENDING until
        # forward publication finishes. Poll metadata only; never manufacture
        # LOST or start reconstruction on that unfinished execution.
        for _ in range(_MAX_POLLS):
            _remaining(deadline)
            lost_reply = core._borrow_rpc(
                foreign.owner_address, "get_owned_object", lost_request
            )
            _remaining(deadline)
            assert type(lost_reply) is protocol.GetOwnedObjectReply and lost_reply.accepted
            assert lost_reply.current_attempt == attempt_0
            if lost_reply.state is protocol.OwnedObjectState.LOST:
                break
            assert lost_reply.state is protocol.OwnedObjectState.PENDING
            assert lost_reply.data is lost_reply.error is lost_reply.descriptor is None
            wake.wait(min(0.01, _remaining(deadline)))
        else:
            raise TimeoutError("owner's actual LOST state did not converge")
        assert isinstance(lost_reply, protocol.GetOwnedObjectReply)
        assert lost_reply.accepted
        assert lost_reply.state is protocol.OwnedObjectState.LOST
        assert lost_reply.current_attempt == attempt_0
        assert lost_reply.data is lost_reply.error is lost_reply.descriptor is None

        phase = "reconstruct"
        value = ray.get(foreign, timeout=_remaining(deadline))
        assert value[:2] == ("foreign-wait-drop", 42)
        assert value[2] == producer_node.worker_pid
        assert value[3] == _PAYLOAD_BYTE * _PAYLOAD_BYTES
        assert foreign.object_id == logical_id
        assert foreign.borrower_token == borrower_token
        assert len(reconstruction_exchanges) == 1
        address, request, reply = reconstruction_exchanges[0]
        assert type(request) is protocol.RequestOwnedObjectReconstruction
        assert type(reply) is protocol.RequestOwnedObjectReconstructionReply
        assert address == foreign.owner_address
        assert request.object_id == reply.object_id == logical_id
        assert request.owner_worker_id == reply.owner_worker_id == (
            foreign.owner_worker_id
        )
        assert request.requester_worker_id == reply.requester_worker_id == (
            core.worker_id
        )
        assert request.borrower_token == reply.borrower_token == borrower_token
        assert request.source == reply.source
        assert request.expected_owner_attempt == (
            reply.expected_owner_attempt
        ) == attempt_0
        assert reply.disposition is (
            protocol.OwnedObjectReconstructionDisposition.STARTED
        )
        assert reply.reconstruction_attempt == attempt_1
        assert reply.failure is None and reply.detail is None
        assert object_fetch_phases == ["reconstruct"]
        ready_replies = [
            reply for observed_phase, _, reply in owner_reads
            if observed_phase == "reconstruct" and type(reply) is protocol.GetOwnedObjectReply
            and reply.state is protocol.OwnedObjectState.READY_STORED
        ]
        assert ready_replies and ready_replies[-1].current_attempt == attempt_1
        descriptor = ready_replies[-1].descriptor
        assert descriptor is not None and descriptor.object_id == logical_id
        assert descriptor.producer_attempt_id == attempt_1 and descriptor.node_id == producer_node.node_id
        assert descriptor.owner_worker_id == foreign.owner_worker_id
        assert _PAYLOAD_BYTES < descriptor.size_bytes < _PAYLOAD_BYTES + 1024
        with observation_lock:
            assert not observation_failed and not overflow
            assert all(type(request) is protocol.GetOwnedObject and type(reply) is protocol.GetOwnedObjectReply
                       for _, request, reply in owner_reads)

        # Restore the original transport before physical-GC observations. These
        # raw Node reads are not another public fetch and do not change the
        # original wait/drop/reconstruct exchange counts or inject a new fault.
        monkeypatch.setattr(core_module, "rpc_request", original_transport_rpc)
        physical = _physical_probe(
            producer_node.node_address, logical_id, producer_node.node_id, core.node_id, deadline,
        )
        assert physical.found and physical.sealed and physical.producer_attempt_id == attempt_1
        assert physical.owner_worker_id == foreign.owner_worker_id
        assert physical.size_bytes == descriptor.size_bytes and physical.checksum == descriptor.checksum
        release_key = (foreign.owner_worker_id, logical_id, core.worker_id, borrower_token)

        close_deadline = min(deadline, time.monotonic() + _CLEANUP_SECONDS)
        _close_reference(foreign, close_deadline)
        _close_reference(outer, close_deadline)
        assert foreign.closed and outer.closed
        with core._completion:
            for _ in range(_MAX_LOCAL_POLLS):
                if (core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED
                        and release_key not in core._borrowed_release_obligations):
                    break
                core._completion.wait(min(0.1, _remaining(deadline)))
            else:
                raise TimeoutError("outer collection and borrower Release did not converge")
        for _ in range(_MAX_POLLS):
            physical = _physical_probe(
                producer_node.node_address, logical_id, producer_node.node_id, core.node_id, deadline,
            )
            if not physical.found:
                assert not physical.sealed and physical.data is None
                assert physical.error == "object is not present in this node's object store"
                assert physical.producer_attempt_id is physical.owner_worker_id is physical.size_bytes is None
                break
            assert physical.sealed and physical.producer_attempt_id == attempt_1
            assert physical.owner_worker_id == foreign.owner_worker_id
            assert physical.size_bytes == descriptor.size_bytes and physical.checksum == descriptor.checksum
            wake.wait(min(0.01, _remaining(deadline)))
        else:
            raise TimeoutError("reconstructed physical replica was not collected")
        # These observations do not expose the foreign owner's final metadata
        # collection transaction. Only the exact physical/local facts are claimed.
        _remaining(deadline)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if original_transport_rpc is not None:
                monkeypatch.setattr(core_module, "rpc_request", original_transport_rpc)
            for reference in (foreign, outer):
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            try:
                report = ray.shutdown()
            finally:
                if report is not None:
                    managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
                if core is not None and core.owner_address is not None:
                    managed_addresses.add(core.owner_address)
                surviving_pids = tuple(pid for pid in sorted(managed_pids) if _pid_exists(pid))
                surviving_children = tuple(
                    child.pid for child in mp.active_children() if child.pid in managed_pids
                )
                open_addresses = []
                for address in sorted(managed_addresses):
                    try:
                        with socket.create_connection(address, timeout=0.1):
                            open_addresses.append(address)
                    except OSError:
                        pass
                assert not surviving_pids, surviving_pids
                assert not surviving_children, surviving_children
                assert not open_addresses, open_addresses
                assert not close_errors, close_errors
                assert not observation_failed and not overflow
                assert not ray.is_initialized()

    assert context is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.node_pids == context.node_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert tuple(report.worker_pids) == context.worker_pids
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
