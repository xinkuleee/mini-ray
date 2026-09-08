"""Bounded adopted unified-publication owner-death acceptance.

Run only the reviewed exact node ID with scripts/run_bounded_test.py and its
30-second process-group deadline. Bounds: one GCS, two Nodes, one ordinary
Worker per Node (five startup children), one exact owner-Worker SIGKILL and
one replacement; two tasks with max_retries=0, one tiny Driver put, 8 KiB
padding, and a 1 MiB ObjectStore per Node. No Actors, placement groups, tracing,
test-owned listeners, gates, threads, or synthetic death records.

All work shares a ten-second monotonic deadline; reference cleanup gets three
seconds. init/shutdown retain their existing finite runtime deadlines, and the
external runner is the final process-tree bound, including partial startup.

This slice waits for exact successful Complete AND owner adoption before the
kill. GCS owner_cleaned is the causal barrier for owner-wide Node fences, child
hold/graph cleanup, Node retirement, and the still-live executor Worker ACK.
Existing graph queries retain immutable manifests after edge release; they do
not expose active_edges. Accordingly cleanup is checked through owner_cleaned,
source hold tombstones, physical replica absence, and clean GCS/Worker drain,
not by treating a persistent manifest as an active graph or replaying cleanup.
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass
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
from miniray.api import _get_runtime
from miniray.control import GET_CONTAINED_GRAPH_HANDLER, GET_WORKER_STATE_HANDLER
from miniray.core import _worker_death_reference_id
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import GET_OBJECT_HANDLER
from miniray.output_publication import OutputPublicationEnvelope, OutputPublicationID
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.runtime_binding import current_core_worker
from miniray.stored_publication import BorrowedContainedSource
from miniray.transport import request as rpc_request
from tests.integration.test_multi_contained_output_path import _close_local


pytestmark = pytest.mark.multiprocess_smoke

_OWNER_RESOURCE = "output_owner_death_owner"
_PRODUCER_RESOURCE = "output_owner_death_producer"
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_SOURCE_VALUE = ("surviving-output-child-source", 42)
_PADDING = b"O" * (8 * 1024)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("output owner-death acceptance exceeded its deadline")
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
        wake.wait(min(0.01, remaining))


@ray.remote(num_cpus=1, resources={_PRODUCER_RESOURCE: 1}, max_retries=0)
def _stored_borrowed_source(container):
    source = container[0]
    if not isinstance(source, ray.ObjectRef):
        raise TypeError("nested source must remain a borrowed ObjectRef")
    return {"source": source, "padding": _PADDING, "executor_pid": os.getpid()}


@ray.remote(num_cpus=0, resources={_OWNER_RESOURCE: 1}, max_retries=0)
def _return_adopted_worker_owned_ref(container, deadline: float):
    _remaining(deadline)
    child = _stored_borrowed_source.remote(container)
    ready, pending = ray.wait([child], num_returns=1, timeout=_remaining(deadline))
    assert ready == [child] and not pending
    core = current_core_worker()
    assert core is not None and child.owner_worker_id == core.worker_id

    # Waiting for metadata readiness does not fetch or deserialize the stored
    # value on Node A. Only Node B owns physical bytes in this acceptance.
    def adopted_locally():
        with core._completion:
            return child.object_id not in core._task_finish_barriers

    _poll_until(adopted_locally, deadline, "child adoption did not finish")
    snapshot = core.owner_table.snapshot(child.object_id)
    assert snapshot.state is ObjectState.READY_STORED
    membership = snapshot.output_publication
    assert membership is not None
    return child, membership.publication_id, container[0].object_id, os.getpid()


def _assert_metadata_only(value: object) -> None:
    if isinstance(value, (AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID)):
        return
    assert not isinstance(value, (
        bytes, bytearray, memoryview, OutputPublicationEnvelope,
        protocol.ResultDescriptor, protocol.ObjectStoreDescriptor,
    )), "GCS retained output payload or a result descriptor"
    if is_dataclass(value) and not isinstance(value, type):
        for field in fields(value):
            _assert_metadata_only(getattr(value, field.name))
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _assert_metadata_only(item)
    else:
        assert value is None or isinstance(value, (str, int, float, bool, Enum))


def _rpc(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, request, connect_timeout=min(0.5, remaining),
        request_timeout=min(2.0, remaining), deadline=deadline,
    )


def _recovery(context, publication_id, deadline):
    request = wire.GetOutputPublicationRecovery(publication_id)
    reply = _rpc(
        context.gcs_address, wire.GET_OUTPUT_PUBLICATION_RECOVERY_HANDLER,
        request, deadline,
    )
    assert type(reply) is wire.GetOutputPublicationRecoveryReply
    assert reply.request == request and reply.found and reply.snapshot is not None
    _assert_metadata_only(reply)
    return reply.snapshot


def _worker_state(context, worker_id, deadline):
    reply = _rpc(
        context.gcs_address, GET_WORKER_STATE_HANDLER,
        protocol.GetWorkerState(worker_id), deadline,
    )
    assert type(reply) is protocol.GetWorkerStateReply
    assert reply.found and reply.worker_id == worker_id
    return reply


def _graph(context, publication_id, deadline):
    request = protocol.GetContainedGraph(publication_id.graph_transaction_id)
    reply = _rpc(context.gcs_address, GET_CONTAINED_GRAPH_HANDLER, request, deadline)
    assert type(reply) is protocol.GetContainedGraphReply and reply.request == request
    assert reply.disposition is protocol.StoredPublicationQueryDisposition.FOUND
    _assert_metadata_only(reply)
    return reply.manifest


def _replica(node, object_id, deadline):
    # No expected epoch filter: a stale/different replica must not count as
    # absence. This only reads existing Node bytes; it creates no replica/pin.
    reply = _rpc(
        node.node_address, GET_OBJECT_HANDLER,
        protocol.GetObject(object_id, node.node_id), deadline,
    )
    assert type(reply) is protocol.GetObjectReply
    assert reply.object_id == object_id and reply.node_id == node.node_id
    return reply


def _close_reference(reference, deadline):
    if reference is None:
        return
    if reference.borrower_token is None:
        _close_local(reference, deadline)
        return
    # Use the real foreign release finalizer, without close()'s bare wait.
    # Even a partial failure enqueues cleanup before the finite wait expires.
    reference._closed = True
    if reference._finalizer is not None:
        reference._finalizer()
    if reference._release_done is not None:
        assert reference._release_done.wait(max(0.0, deadline - time.monotonic()))
    assert reference.closed


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def test_adopted_output_owner_death_cleans_live_executor_and_source_holds():
    context = report = core = source = outer = foreign = None
    owner_node = producer_node = death = None
    managed_pids, managed_addresses = set(), set()
    close_errors = []
    try:
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=(
                {"CPU": 1, _OWNER_RESOURCE: 1},
                {"CPU": 1, _PRODUCER_RESOURCE: 1},
            ),
            inline_threshold=1024, object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        owner_node, producer_node = context.nodes
        assert len(owner_node.worker_ids) == len(producer_node.worker_ids) == 1
        assert context.trace_address is None
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.add(context.gcs_address)
        for node in context.nodes:
            managed_addresses.update((node.node_address, node.worker_address))
        assert len(managed_pids) == len(managed_addresses) == 5
        assert os.getpid() not in managed_pids
        managed_addresses.add(runtime.owner_service.address)

        source = ray.put(_SOURCE_VALUE)
        outer = _return_adopted_worker_owned_ref.remote([source], deadline)
        foreign, publication_id, source_id, parent_pid = ray.get(
            outer, timeout=_remaining(deadline)
        )
        assert isinstance(foreign, ray.ObjectRef)
        assert type(publication_id) is OutputPublicationID
        assert source_id == source.object_id
        assert parent_pid == owner_node.worker_pid
        assert foreign.owner_worker_id == owner_node.worker_id
        assert foreign.owner_address == owner_node.worker_address
        assert foreign.borrower_token is not None
        assert publication_id.output_ids == (foreign.object_id,)
        assert publication_id.attempt_id == AttemptID(foreign.object_id.task_id, 0)

        def adopted():
            snapshot = _recovery(context, publication_id, deadline)
            return snapshot if snapshot.complete is not None and snapshot.adopted is not None else None

        before = _poll_until(adopted, deadline, "GCS did not confirm adopted Complete")
        assert before.owner_death is None and before.owner_cleaned is None
        assert before.frozen_node_death is None and before.rollback is None
        assert before.adopted.complete == before.complete
        manifest = before.manifest
        assert manifest.header.owner_worker_id == owner_node.worker_id
        assert manifest.header.executor_worker_id == producer_node.worker_id
        assert manifest.header.node_incarnation.node_id == producer_node.node_id
        assert manifest.header.node_incarnation.node_pid == producer_node.node_pid
        assert before.adopted.owner_worker_id == owner_node.worker_id
        assert len(manifest.slots) == 1
        slot = manifest.slots[0]
        assert slot.tier is protocol.ResultStorage.OBJECT_STORE
        assert len(_PADDING) < slot.size_bytes < 32 * 1024
        assert len(slot.transfers) == 1
        transfer = slot.transfers[0]
        assert transfer.contained_object_id == source_id
        assert transfer.contained_owner_worker_id == core.worker_id
        assert transfer.contained_owner_address == runtime.owner_service.address
        assert transfer.final_hold.container_owner_worker_id == owner_node.worker_id
        assert transfer.final_hold.container_object_id == foreign.object_id
        assert isinstance(transfer.source, BorrowedContainedSource)
        assert transfer.source.borrower_worker_id == producer_node.worker_id
        assert isinstance(transfer.source.original_source, protocol.TaskHoldSource)
        source_before = core.owner_table.snapshot(source_id)
        assert source_before.state is ObjectState.READY_INLINE
        assert transfer.final_hold in source_before.contained_holds
        assert ray.get(source, timeout=_remaining(deadline)) == _SOURCE_VALUE
        assert _graph(context, publication_id, deadline) == manifest.to_graph_manifest()

        physical = _replica(producer_node, foreign.object_id, deadline)
        assert physical.found and physical.sealed
        assert physical.producer_attempt_id == publication_id.attempt_id
        assert physical.owner_worker_id == owner_node.worker_id
        assert physical.size_bytes == slot.size_bytes
        assert physical.checksum == slot.checksum
        assert len(physical.data) == slot.size_bytes
        assert hashlib.sha256(physical.data).hexdigest() == slot.checksum
        executor_before = _worker_state(context, producer_node.worker_id, deadline)
        assert executor_before.state is protocol.WorkerMembershipState.ALIVE
        assert executor_before.incarnation.worker_pid == producer_node.worker_pid
        assert executor_before.incarnation.node_id == producer_node.node_id
        assert executor_before.incarnation.node_pid == producer_node.node_pid
        assert (executor_before.incarnation.node_registration_epoch
                == manifest.header.node_incarnation.registration_epoch)
        assert _pid_exists(producer_node.worker_pid)

        # Retire the Driver-owned container while Node A still acknowledges
        # its child edge. The independently acquired foreign borrower remains.
        _close_local(outer, deadline)
        _poll_until(
            lambda: core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.COLLECTED,
            deadline, "outer container did not collect before owner death",
        )
        assert transfer.final_hold in core.owner_table.snapshot(source_id).contained_holds
        obligation_key = (
            foreign.owner_worker_id, foreign.object_id, core.worker_id,
            foreign.borrower_token,
        )
        assert obligation_key in core._borrowed_release_obligations
        owner_before = _worker_state(context, owner_node.worker_id, deadline)
        assert owner_before.state is protocol.WorkerMembershipState.ALIVE
        assert owner_before.incarnation.worker_pid == owner_node.worker_pid
        assert owner_before.incarnation.node_id == owner_node.node_id
        assert owner_before.incarnation.node_pid == owner_node.node_pid
        assert owner_node.worker_pid > 0 and _pid_exists(owner_node.worker_pid)
        _remaining(deadline)
        os.kill(owner_node.worker_pid, signal.SIGKILL)

        def owner_is_dead():
            reply = _worker_state(context, owner_node.worker_id, deadline)
            return reply.death if reply.state is protocol.WorkerMembershipState.DEAD else None

        death = _poll_until(owner_is_dead, deadline, "GCS owner-death proof did not arrive")
        assert death.incarnation == owner_before.incarnation
        assert death.worker_id == owner_node.worker_id
        assert death.worker_pid == owner_node.worker_pid
        assert death.node_id == owner_node.node_id and death.node_pid == owner_node.node_pid
        assert death.reason is protocol.WorkerDeathReason.PROCESS_EXIT
        assert death.exit_code == -signal.SIGKILL

        def owner_cleanup_finished():
            snapshot = _recovery(context, publication_id, deadline)
            return snapshot if snapshot.owner_cleaned is not None else None

        after = _poll_until(owner_cleanup_finished, deadline, "output owner cleanup did not converge")
        assert after.owner_death == after.owner_cleaned == death
        assert after.manifest == manifest
        assert after.complete == before.complete and after.adopted == before.adopted
        assert after.frozen_node_death is None and after.resolution is None
        assert not after.forward_allowed
        assert _graph(context, publication_id, deadline) == manifest.to_graph_manifest()

        # owner_cleaned cannot use executor death to bypass the Worker ACK:
        # the same physical Node-B Worker must be ALIVE on both sides.
        executor_after = _worker_state(context, producer_node.worker_id, deadline)
        assert executor_after.state is protocol.WorkerMembershipState.ALIVE
        assert executor_after.incarnation == executor_before.incarnation
        assert executor_after.death is None and _pid_exists(producer_node.worker_pid)
        missing = _replica(producer_node, foreign.object_id, deadline)
        assert not missing.found and not missing.sealed
        assert missing.data is None and missing.checksum is None
        assert missing.producer_attempt_id is None and missing.owner_worker_id is None
        assert missing.size_bytes is None
        source_after = core.owner_table.snapshot(source_id)
        assert source_after.state is ObjectState.READY_INLINE
        assert source_after.inline_data == source_before.inline_data
        assert source_after.current_attempt == source_before.current_attempt
        assert source_after.local_tokens == source_before.local_tokens
        for hold in (transfer.final_hold, transfer.provisional_hold):
            assert hold not in source_after.contained_holds
            assert core.owner_table.contained_release_was_seen(source_id, hold)
        assert ray.get(source, timeout=_remaining(deadline)) == _SOURCE_VALUE

        installed = _poll_until(
            lambda: core.owner_table.dead_worker_record(owner_node.worker_id),
            deadline, "Driver did not consume the authoritative Worker death",
        )
        assert installed.death_id == _worker_death_reference_id(death)
        with pytest.raises(ray.OwnerDiedError, match="confirmed dead"):
            ray.get(foreign, timeout=_remaining(deadline))
        with pytest.raises(ray.OwnerDiedError, match="confirmed dead"):
            ray.wait([foreign], num_returns=1, timeout=_remaining(deadline))
        _poll_until(
            lambda: obligation_key not in core._borrowed_release_obligations,
            deadline, "dead-owner borrower obligation was not discharged",
        )
        _close_reference(foreign, deadline)
        _close_local(source, deadline)
        _poll_until(
            lambda: core.owner_table.collection_state(source_id) is ObjectCollectionState.COLLECTED,
            deadline, "surviving source did not collect after its final local close",
        )
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            for reference in (foreign, outer, source):
                try:
                    _close_reference(reference, cleanup_deadline)
                except Exception as exc:
                    close_errors.append(exc)
        finally:
            # Also runs if init raises before publishing a RuntimeContext.
            report = ray.shutdown()

    assert not close_errors, "bounded reference cleanup failed: {!r}".format(close_errors)
    assert context is not None and report is not None and death is not None
    assert not ray.is_initialized()
    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized and report.shutdown_ack_clean
    assert not report.forced and not any(report.worker_forced)
    assert all(code == 0 for code in report.node_exitcodes)
    assert all(code == 0 for code in report.worker_exitcodes)
    assert len(report.worker_pids) == 2
    assert report.worker_pids[0] != owner_node.worker_pid
    assert report.worker_pids[1] == producer_node.worker_pid
    managed_pids.update(report.worker_pids)
    assert len(managed_pids) == 6
    _poll_until(
        lambda: all(not _pid_exists(pid) for pid in managed_pids),
        time.monotonic() + 2.0, "managed child survived clean shutdown",
    )
    assert all(child.pid not in managed_pids for child in mp.active_children())
    for address in managed_addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
