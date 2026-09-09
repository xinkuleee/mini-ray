"""F4/F5: a live publisher loses its real outer owner before Complete.

Each exact case starts one GCS, two Nodes, and one Worker per Node. Worker A
owns an output submitted to Worker B, while the nested child belongs to the
live Driver. The existing OWNER_REGISTER or PROMOTIONS gate holds B's real Prepare.
One exact registered Worker-A SIGKILL is the only injected fault; Node A stays
alive and replaces that Worker once. Two logical tasks, no task retries, one
tiny Driver put, 8 KiB padding, two 1 MiB stores, and one listener/connection.
No extra test threads, Actors, placement groups, or tracing.

The Driver observes exact child-release replies through a passive method
wrapper installed before OwnerService binds its callbacks. After real owner
death, read-only GCS membership confirms the surviving Nodes have acknowledged
the existing owner-wide fences. Only then is the publication gate opened: Node
cleanup needs Prepare's adapter ticket and cannot release children while that
gate remains held. The resumed Prepare must be fenced; local Node cleanup must
obtain the same live executor's actual Finalize ACK. There is no GCS publication
record, graph query or test-driven cleanup RPC.

Node-entry observers do no scheduling or cleanup. They check returned spawn
identities, actual status replies, rejection/Finalize results, and after the
real Node entry exits verify every spawned Worker's port, including A's new
ephemeral replacement port. No announcement channel or new production hook.
Work shares 15 seconds, the existing gate has its own 10-second bound, and
finally gate/reference cleanup has three seconds. Run one exact node ID via
the 30-second runner, followed by its existing bounded process-tree grace.
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
from miniray import api as api_module, node as node_module, output_protocol as wire, protocol
from miniray.api import _get_runtime
from miniray.core import CoreWorker, _worker_death_reference_id
from miniray.control import GET_NODES_HANDLER, GET_WORKER_STATE_HANDLER
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_handoff import OutputHandoffPhase
from miniray.output_publication import OutputPublicationEnvelope
from miniray.output_publication_journal import OutputPublicationJournalState, OutputPublicationStage
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_gate import (
    OUTPUT_PUBLICATION_GATE_RELEASE, OutputPublicationGateConfig,
    OutputPublicationGatePhase, recv_output_publication_gate_arrival,
)
from miniray.publication_sources import BorrowedContainedSource
from miniray.recovery import TaskState
from miniray.resources import AllocationState
from miniray.runtime_binding import current_core_worker
from tests.integration.test_task_path import _close_reference as _close_local
from miniray.transport import request as rpc_request


pytestmark = pytest.mark.multiprocess_smoke
_ACTUAL_NODE_PROCESS_MAIN = api_module._node_process_main
_OWNER_RESOURCE = "precomplete_owner_a"
_EXECUTOR_RESOURCE = "precomplete_executor_b"
_INSPECT = "inspect-precomplete-owner-death"
_SOURCE_VALUE = ("live-driver-child-after-owner-death", 42)
_PADDING = b"P" * (8 * 1024)


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("precomplete owner-death deadline expired")
    return remaining


def _poll_until(predicate, deadline, detail):
    wake = threading.Event()
    while True:
        assert deadline > time.monotonic(), detail
        result = predicate()
        if result:
            return result
        wake.wait(min(0.01, _remaining(deadline)))


def _query(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(address, handler, request, connect_timeout=min(0.5, remaining),
                       request_timeout=min(2.0, remaining), deadline=deadline)


def _worker_state(context, worker_id, deadline):
    reply = _query(context.gcs_address, GET_WORKER_STATE_HANDLER,
                   protocol.GetWorkerState(worker_id), deadline)
    assert type(reply) is protocol.GetWorkerStateReply and reply.found and reply.worker_id == worker_id
    return reply


def _handoff(owner_address, publication_id, deadline):
    request = wire.GetOutputHandoff(publication_id)
    reply = _query(owner_address, wire.GET_OUTPUT_HANDOFF_HANDLER, request, deadline)
    assert type(reply) is wire.OutputHandoffReply and reply.request == request and reply.accepted
    assert reply.snapshot is not None and reply.snapshot.publication_id == publication_id
    _assert_metadata_only(reply)
    return reply.snapshot


def _assert_metadata_only(value):
    if isinstance(value, (AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID)):
        return
    assert not isinstance(value, (bytes, bytearray, memoryview, OutputPublicationEnvelope,
                                  protocol.ResultDescriptor, protocol.ObjectStoreDescriptor))
    if is_dataclass(value) and not isinstance(value, type):
        for item in fields(value):
            _assert_metadata_only(getattr(value, item.name))
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _assert_metadata_only(item)
    else:
        assert value is None or isinstance(value, (str, int, float, bool, Enum))


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


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


def _node_process_with_lifecycle_observations(*args):
    """Record real effects; failed observations only fail the child at exit."""
    node_type = node_module.NodeServer
    original_spawn = node_type._spawn_worker_process
    original_status = node_type._handle_shutdown_status
    original_prepare = node_type._handle_prepare_output_publication
    original_rpc = node_type._background_rpc
    owner_node = bool(args[1].get(_OWNER_RESOURCE, 0))
    spawned, seen_nodes, manifests = [], [], []
    spawn_calls = prepare_accepted = complete_reports = 0
    inspected = ()
    prepare_fenced = False
    worker_finalize = None
    errors = set()

    def spawn(node, worker_id):
        nonlocal spawn_calls
        spawn_calls += 1
        if not seen_nodes:
            seen_nodes.append(node)
        process, address = original_spawn(node, worker_id)
        if len(spawned) < 3:
            spawned.append((worker_id, process.pid, address))
        else:
            errors.add("more than three successful Worker spawns")
        return process, address

    def status(node, request):
        nonlocal inspected
        reply = original_status(node, request)
        if request.request_id == _INSPECT:
            inspected = reply.child_pids
        return reply

    def prepare(node, request):
        nonlocal prepare_accepted, prepare_fenced
        if not owner_node:
            if not manifests:
                manifests.append(request.manifest)
            elif manifests[0] != request.manifest:
                errors.add("executor prepared another publication or attempt")
        try:
            reply = original_prepare(node, request)
        except Exception as exc:
            if not owner_node and "death-fenced" in str(exc):
                prepare_fenced = True
            raise
        if not owner_node:
            if reply.accepted:
                prepare_accepted += 1
            elif (request.manifest.header.owner_worker_id in node._owner_death_fences
                  and "death-fenced" in (reply.error or "")):
                prepare_fenced = True
        return reply

    def rpc(node, address, handler, request, **options):
        nonlocal complete_reports, worker_finalize
        if (not owner_node and handler == wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER
                and type(request) is wire.ReportOutputHandoffComplete):
            complete_reports += 1
        reply = original_rpc(node, address, handler, request, **options)
        if not owner_node and handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER:
            if (type(reply) is wire.FinalizeOutputOwnerDeathReply and reply.request == request
                    and reply.cleaned is True):
                if worker_finalize is not None and worker_finalize != (address, request):
                    errors.add("executor Finalize identity was rebound")
                worker_finalize = (address, request)
        return reply

    node_type._spawn_worker_process = spawn
    node_type._handle_shutdown_status = status
    node_type._handle_prepare_output_publication = prepare
    node_type._background_rpc = rpc
    try:
        _ACTUAL_NODE_PROCESS_MAIN(*args)
        expected_count = 2 if owner_node else 1
        assert spawn_calls == len(spawned) == expected_count and not errors, (spawn_calls, spawned, errors)
        assert len({worker_id for worker_id, _pid, _address in spawned}) == expected_count
        assert len({pid for _worker_id, pid, _address in spawned}) == expected_count
        assert inspected == (spawned[-1][1],), "actual status must report the last spawned Worker"
        (node,) = seen_nodes
        assert node.worker_pids == inspected
        assert len(node._leases) == 1, "each Node admitted only its one logical task"
        if not owner_node:
            (manifest,) = manifests
            identity = manifest.publication_id
            assert prepare_fenced and prepare_accepted == complete_reports == 0
            assert worker_finalize == (spawned[0][2], wire.FinalizeOutputOwnerDeath(
                manifest, node._owner_death_fences[manifest.header.owner_worker_id],
            ))
            snapshot = node._output_publication_journal.snapshot(identity)
            assert snapshot.state is OutputPublicationJournalState.RETIRED and not snapshot.retained_result_slots
            assert snapshot.complete is None and snapshot.rollback_tombstone is None
            assert node._output_publication_journal._records[identity].owner_death == node._owner_death_fences[manifest.header.owner_worker_id]
            assert any(effect.stage is OutputPublicationStage.OWNER_REGISTER for effect in snapshot.intents)
            assert node._output_publications.owner_death_finished(identity)
            record = node._leases[identity.lease_id]
            assert record.state is protocol.LeaseExecutionState.ABANDONED and record.completion is None
            assert record.output_complete_inflight is None
            assert node._ledger.record(record.allocation_token).state is AllocationState.RELEASED
        assert node._object_store.used_bytes == 0 and not node._local_replica_write_claims
        assert node._ledger.available == node._ledger.total and node._ledger.cpu_debt == 0
        # Replacement ports are not included in immutable startup context.
        # Verify the actual returned addresses after normal Node teardown.
        for _worker_id, pid, address in spawned:
            assert not _pid_exists(pid)
            with pytest.raises(OSError):
                with socket.create_connection(address, timeout=0.1):
                    pass
    finally:
        node_type._spawn_worker_process = original_spawn
        node_type._handle_shutdown_status = original_status
        node_type._handle_prepare_output_publication = original_prepare
        node_type._background_rpc = original_rpc


@ray.remote(num_cpus=1, resources={_EXECUTOR_RESOURCE: 1}, max_retries=0)
def _publish_borrowed_child(container):
    child, = container
    assert isinstance(child, ray.ObjectRef) and child.borrower_token is not None
    return {"child": child, "padding": _PADDING, "executor_pid": os.getpid()}


@ray.remote(num_cpus=0, resources={_OWNER_RESOURCE: 1}, max_retries=0)
def _own_pending_output(container, deadline):
    core = current_core_worker()
    assert core is not None
    output = None
    try:
        _remaining(deadline)
        output = _publish_borrowed_child.remote(container)
        assert output.owner_worker_id == core.worker_id and output.borrower_token is None
        ready, pending = ray.wait([output], num_returns=1, timeout=_remaining(deadline))
        assert ready == [output] and not pending
        # Only reached if a failed test releases the gate before the kill. No
        # foreign result handle is fabricated or exported for this acceptance.
        return "factory-finished", os.getpid()
    finally:
        _close_local(output, time.monotonic() + 3.0)


def _wait(core, predicate, deadline):
    with core._completion:
        while True:
            value = predicate()
            if value:
                return value
            core._completion.wait(_remaining(deadline))


def _physical(node, object_id, deadline):
    reply = _query(node.node_address, node_module.GET_OBJECT_HANDLER, protocol.GetObject(object_id, node.node_id), deadline)
    assert type(reply) is protocol.GetObjectReply and reply.object_id == object_id and reply.node_id == node.node_id
    return reply


def _run_precomplete_owner_death(phase):
    assert phase in (OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK, OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK)
    promoted = phase is OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK
    listener = connection = context = core = report = death = source = factory = None
    owner_node = executor_node = transfer = manifest = None
    original_entry = api_module._node_process_main
    original_release = CoreWorker.release_contained_reference
    release_observed = threading.Event()
    observation_lock = threading.Lock()
    observed_releases, observation_errors = {}, set()
    pids, addresses, cleanup_errors = set(), set(), []
    final_live_pids = ()

    def observe_release(owner, request):
        reply = original_release(owner, request)
        if (core is not None and owner is core and transfer is not None
                and request.object_id == transfer.contained_object_id
                and request.hold in (transfer.final_hold, transfer.provisional_hold)):
            exact = (type(reply) is protocol.ReleaseContainedReferenceReply and reply.accepted is True
                     and (reply.object_id, reply.owner_worker_id, reply.hold)
                     == (request.object_id, request.owner_worker_id, request.hold))
            with observation_lock:
                if exact:
                    observed_releases.setdefault(request.hold, reply)
                else:
                    observation_errors.add("actual child-release reply changed its exact identity")
            if exact and request.hold == transfer.final_hold:
                release_observed.set()
        return reply

    try:
        # OwnerService captures a bound release method during init. Installing
        # before init observes its real requests without editing server tables.
        CoreWorker.release_contained_reference = observe_release
        api_module._node_process_main = _node_process_with_lifecycle_observations
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        gate_address = listener.getsockname()
        addresses.add(gate_address)
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _OWNER_RESOURCE: 1}, {"CPU": 1, _EXECUTOR_RESOURCE: 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
            _test_output_publication_gate=OutputPublicationGateConfig(1, gate_address, phase, 10.0),
        )
        deadline = time.monotonic() + 15.0
        runtime = _get_runtime()
        core = runtime.core_worker
        owner_node, executor_node = context.nodes
        assert context.trace_address is None
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, runtime.owner_service.address))
        for node in context.nodes:
            assert len(node.worker_ids) == 1
            addresses.update((node.node_address, node.worker_address))
        assert len(pids) == 5 and len(addresses) == 7 and os.getpid() not in pids
        source = ray.put(_SOURCE_VALUE)
        initial_source = core.owner_table.snapshot(source.object_id)
        assert initial_source.state is ObjectState.READY_INLINE and source.owner_worker_id == core.worker_id
        factory = _own_pending_output.remote([source], deadline)
        listener.settimeout(_remaining(deadline))
        connection, _ = listener.accept()
        connection.settimeout(min(10.0, _remaining(deadline)))
        arrival = recv_output_publication_gate_arrival(connection)
        assert arrival.phase is phase
        _assert_metadata_only(arrival)
        assert (arrival.node_id, arrival.node_pid, arrival.registration_epoch) == (
            executor_node.node_id, executor_node.node_pid, runtime.nodes[1].registration_epoch,
        )
        publication = arrival.publication_id
        assert publication.attempt_id == AttemptID(publication.task_id, 0)
        assert len(publication.output_ids) == 1 and publication.output_ids == publication.full_output_ids
        (output_id,) = publication.output_ids
        assert output_id != factory.object_id and output_id != source.object_id
        before = _handoff(owner_node.worker_address, publication, deadline)
        manifest = before.manifest
        assert manifest.manifest_digest == arrival.manifest_digest
        assert manifest.header.owner_worker_id == owner_node.worker_id
        assert manifest.header.executor_worker_id == executor_node.worker_id
        assert before.phase is OutputHandoffPhase.PENDING
        assert before.complete is before.adoption is before.abort_reason is None
        (slot,) = manifest.slots
        assert slot.tier is protocol.ResultStorage.OBJECT_STORE and slot.object_id == output_id
        assert len(_PADDING) < slot.size_bytes < 32 * 1024
        (transfer,) = slot.transfers
        assert transfer.contained_object_id == source.object_id and transfer.contained_owner_worker_id == core.worker_id
        assert transfer.contained_owner_address == runtime.owner_service.address
        assert type(transfer.source) is BorrowedContainedSource and transfer.source.borrower_worker_id == executor_node.worker_id
        assert type(transfer.source.original_source) is protocol.TaskHoldSource
        assert transfer.source.original_source.hold.submitting_worker_id == owner_node.worker_id
        assert transfer.final_hold.container_owner_worker_id == owner_node.worker_id
        assert transfer.provisional_hold.container_owner_worker_id == executor_node.worker_id
        at_gate = core.owner_table.snapshot(source.object_id)
        assert at_gate.state is ObjectState.READY_INLINE and at_gate.inline_data == initial_source.inline_data
        assert at_gate.local_tokens == initial_source.local_tokens and transfer.source.owner_table_token in at_gate.borrowed_tokens
        assert (transfer.final_hold in at_gate.contained_holds) is promoted
        assert transfer.provisional_hold not in at_gate.contained_holds
        assert core.owner_table.contained_release_was_seen(source.object_id, transfer.provisional_hold) is promoted
        assert not core.owner_table.contained_release_was_seen(source.object_id, transfer.final_hold)
        physical = _physical(executor_node, output_id, deadline)
        assert physical.found is promoted and physical.sealed is promoted
        if promoted:
            assert physical.producer_attempt_id == publication.attempt_id and physical.owner_worker_id == owner_node.worker_id
            assert physical.size_bytes == slot.size_bytes and physical.checksum == slot.checksum
            assert hashlib.sha256(physical.data).hexdigest() == slot.checksum
        else:
            assert physical.data is physical.checksum is physical.producer_attempt_id is None
        owner_before = _worker_state(context, owner_node.worker_id, deadline)
        executor_before = _worker_state(context, executor_node.worker_id, deadline)
        assert owner_before.state is executor_before.state is protocol.WorkerMembershipState.ALIVE
        assert owner_before.incarnation.worker_pid == owner_node.worker_pid and owner_before.incarnation.node_pid == owner_node.node_pid
        assert owner_before.incarnation.node_id == owner_node.node_id
        assert owner_before.incarnation.node_registration_epoch == runtime.nodes[0].registration_epoch
        assert executor_before.incarnation.worker_pid == executor_node.worker_pid
        assert executor_before.incarnation.node_id == executor_node.node_id and executor_before.incarnation.node_pid == executor_node.node_pid
        assert executor_before.incarnation.node_registration_epoch == arrival.registration_epoch
        assert not release_observed.is_set()
        assert factory.owner_worker_id == core.worker_id and factory.borrower_token is None
        assert ray.wait([factory], num_returns=1, timeout=0) == ([], [factory])
        assert owner_node.worker_pid > 0 and _pid_exists(owner_node.worker_pid)
        _remaining(deadline)
        os.kill(owner_node.worker_pid, signal.SIGKILL)

        def confirmed_death():
            candidate = _worker_state(context, owner_node.worker_id, deadline)
            return candidate.death if candidate.state is protocol.WorkerMembershipState.DEAD else None

        death = _poll_until(confirmed_death, deadline, "GCS did not confirm the exact owner Worker exit")
        assert death.incarnation == owner_before.incarnation and death.reason is protocol.WorkerDeathReason.PROCESS_EXIT
        assert death.exit_code == -signal.SIGKILL
        assert _pid_exists(owner_node.node_pid) and _pid_exists(executor_node.node_pid)
        # GetNodes is read-only and excludes Nodes with an outstanding
        # owner-wide fence. It does not drive publication/child cleanup.
        def nodes_fenced():
            reply = _query(context.gcs_address, GET_NODES_HANDLER, protocol.GetNodes(), deadline)
            assert type(reply) is protocol.GetNodesReply
            visible = {info.node_id: info for info in reply.nodes}
            if set(visible) != {owner_node.node_id, executor_node.node_id}:
                return None
            for node in (owner_node, executor_node):
                assert visible[node.node_id].node_pid == node.node_pid
                assert visible[node.node_id].state is protocol.NodeMembershipState.ALIVE
            return reply

        _poll_until(nodes_fenced, deadline, "surviving Nodes did not acknowledge owner fences")
        connection.settimeout(_remaining(deadline))
        connection.sendall(OUTPUT_PUBLICATION_GATE_RELEASE)
        connection.close()
        connection = None
        assert release_observed.wait(_remaining(deadline)), "Node did not release the final child hold"

        def child_holds_released():
            with observation_lock:
                return (set(observed_releases) == {transfer.final_hold, transfer.provisional_hold}
                        and not observation_errors)

        _poll_until(child_holds_released, deadline, "Node did not acknowledge both exact child releases")
        executor_after = _worker_state(context, executor_node.worker_id, deadline)
        assert executor_after.state is protocol.WorkerMembershipState.ALIVE
        assert executor_after.incarnation == executor_before.incarnation and executor_after.death is None
        assert _pid_exists(executor_node.worker_pid)
        assert not _physical(executor_node, output_id, deadline).found
        assert not _physical(owner_node, output_id, deadline).found
        assert not core.owner_table.contains(output_id), "Driver must not take over the dead owner's output"
        child_after = core.owner_table.snapshot(source.object_id)
        assert child_after.state is ObjectState.READY_INLINE and child_after.inline_data == initial_source.inline_data
        assert child_after.local_tokens == initial_source.local_tokens
        for hold in (transfer.final_hold, transfer.provisional_hold):
            assert hold not in child_after.contained_holds
            assert core.owner_table.contained_release_was_seen(source.object_id, hold)
        with observation_lock:
            assert set(observed_releases) == {transfer.final_hold, transfer.provisional_hold}
            assert not observation_errors
        # The Driver owns the factory result. Worker failure is a task error,
        # NOT OwnerDiedError on a fabricated foreign handle.
        with pytest.raises(ray.SystemTaskError):
            ray.get(factory, timeout=_remaining(deadline))
        _wait(core, lambda: factory.object_id not in core._task_finish_barriers, deadline)
        factory_state = core.owner_table.snapshot(factory.object_id)
        assert factory_state.state is ObjectState.ERROR and isinstance(factory_state.error, ray.SystemTaskError)
        record = replace(core._recovery.task_record(factory.object_id.task_id))
        assert record.state is TaskState.SYSTEM_FAILED and record.max_retries == record.retries_started == 0
        assert record.current_attempt == AttemptID(factory.object_id.task_id, 0)
        installed = _wait(core, lambda: core.owner_table.dead_worker_record(owner_node.worker_id), deadline)
        assert installed.death_id == _worker_death_reference_id(death)
        assert ray.get(source, timeout=_remaining(deadline)) == _SOURCE_VALUE
        _close_local(factory, min(deadline, time.monotonic() + 3.0))
        _wait(core, lambda: core.owner_table.collection_state(factory.object_id) is ObjectCollectionState.COLLECTED, deadline)
        _wait(core, lambda: not core.owner_table.snapshot(source.object_id).borrowed_tokens, deadline)
        assert ray.get(source, timeout=_remaining(deadline)) == _SOURCE_VALUE
        _close_local(source, min(deadline, time.monotonic() + 3.0))
        _wait(core, lambda: core.owner_table.collection_state(source.object_id) is ObjectCollectionState.COLLECTED, deadline)
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations

        def inspect_clean_nodes():
            statuses = tuple(_query(node.node_address, node_module.SHUTDOWN_STATUS_HANDLER,
                                    protocol.ShutdownStatusRequest(_INSPECT), deadline) for node in context.nodes)
            assert all(type(status) is protocol.ShutdownStatus and not status.shutdown_requested and not status.finalized
                       for status in statuses)
            if (not all(status.resources_clean for status in statuses)
                    or len(statuses[0].child_pids) != 1 or statuses[0].child_pids == (owner_node.worker_pid,)):
                return None
            assert statuses[1].child_pids == (executor_node.worker_pid,)
            return statuses[0].child_pids + statuses[1].child_pids

        final_live_pids = _poll_until(inspect_clean_nodes, deadline, "replacement and both Node ledgers did not settle")
        pids.update(final_live_pids)
        assert len(pids) == 6 and not _pid_exists(owner_node.worker_pid)
        assert not runtime.node_deaths  # Neither live Node was the injected target.
    finally:
        cleanup_deadline = time.monotonic() + 3.0
        api_module._node_process_main = original_entry
        try:
            try:
                _release_connection(connection, OUTPUT_PUBLICATION_GATE_RELEASE, cleanup_deadline)
            finally:
                if listener is not None:
                    listener.close()
        finally:
            try:
                for reference in (factory, source):
                    try:
                        _close_local(reference, cleanup_deadline)
                    except Exception as exc:
                        cleanup_errors.append(exc)
            finally:
                try:
                    report = ray.shutdown()
                finally:
                    CoreWorker.release_contained_reference = original_release
    assert not cleanup_errors and context is not None and death is not None and report is not None
    assert not ray.is_initialized() and report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean and report.resources_clean and report.finalized and report.shutdown_ack_clean
    assert not report.forced and not report.gcs_forced and not any(report.worker_forced)
    assert report.node_exitcodes == report.worker_exitcodes == (0, 0)
    assert report.node_pids == context.node_pids and report.worker_pids == final_live_pids
    # Normal shutdown may record EXPECTED Node exits; those are not a second
    # injected failure and must not be mistaken for publisher/owner Node loss.
    for index, node_death in enumerate(report.node_deaths):
        if node_death is not None:
            assert node_death.reason is protocol.NodeDeathReason.EXPECTED and node_death.exit_code == 0
            assert (node_death.node_id, node_death.node_pid) == (context.nodes[index].node_id, context.nodes[index].node_pid)
    pids.update(report.worker_pids)
    assert len(pids) == 6
    _poll_until(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0, "managed process survived owner-death shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    # Startup/GCS/Driver/gate ports are checked here; Node exit checks above
    # additionally cover the replacement's real newly allocated endpoint.
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass


def test_owner_death_after_intent_fences_unmaterialized_output_and_cleans_live_executor():
    _run_precomplete_owner_death(OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK)


def test_owner_death_after_promotions_drops_sealed_output_and_cleans_live_child_holds():
    _run_precomplete_owner_death(OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK)
