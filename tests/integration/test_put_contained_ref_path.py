"""A stored put retains a foreign ref through get, task import and collection.

One GCS, one Node and two ordinary Workers: four children, five endpoints, a
1 MiB ObjectStore and exactly three user Tasks (parent, child, consumer). The
Driver puts one 2 KiB-padded container; no task, lease or lineage is invented
for that put. The consumer returns another 2 KiB stored value. The first case
executes it once; the second drops that result once and reconstructs the same
consumer from its retained put dependency (four executions, three TaskIDs).
There is no process fault, Actor, trace or test-owned listener.
The single-node case proves local stored dependency materialization, not a
cross-node transfer. Actual remote borrower ACKs and local put GC are checked;
the foreign child's owner metadata GC is not independently observable here.

All gets and <=128 condition observations per gate share fifteen seconds after
init. Public close/borrow-release convergence in finally share three seconds.
The passive observer stores at most 64 exchanges and changes no RPC arguments
or replies. Init, synchronous put and shutdown retain their production bounds
under the exact 30-second POSIX process-tree runner. Every tracked child PID
and endpoint is checked even after failure. No cross-test fixtures are imported.
"""

from __future__ import annotations

import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.ownership import ObjectCollectionState, ObjectState


pytestmark = pytest.mark.multiprocess_smoke
_WORK_SECONDS = 15.0
_CLEANUP_SECONDS = 3.0
_MAX_POLLS = 128
_MAX_OBSERVATIONS = 64
_PADDING_BYTES = 2 * 1024


@ray.remote(num_cpus=1)
def _child_value(value):
    return value + 1


@ray.remote(num_cpus=1)
def _return_child(value):
    return _child_value.remote(value)


@ray.remote(num_cpus=1, max_retries=1)
def _consume_put(container, deadline):
    child = container["ref"]
    try:
        assert isinstance(child, ray.ObjectRef)
        assert container["alias"] is child
        assert container["padding"] == b"P" * _PADDING_BYTES
        value = ray.get(child, timeout=_remaining(deadline))
        return {"value": value, "padding": b"R" * _PADDING_BYTES, "pid": os.getpid()}
    finally:
        _close(child, min(deadline, time.monotonic() + _CLEANUP_SECONDS))


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("contained put work or cleanup exceeded its deadline")
    return remaining


def _close(reference, deadline):
    if reference is None:
        return
    done = reference._release_done
    assert reference._finalizer is not None and done is not None
    reference.close(timeout=max(0.0, deadline - time.monotonic()))
    assert reference.closed and done.is_set()


def _wait_local(core, predicate, deadline):
    with core._completion:
        for index in range(_MAX_POLLS):
            if predicate():
                return
            if index + 1 < _MAX_POLLS:
                core._completion.wait(min(0.1, _remaining(deadline)))
    raise TimeoutError("contained put local state did not converge")


def _borrow_key(reference, core):
    return (reference.owner_worker_id, reference.object_id,
            core.worker_id, reference.borrower_token)


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def test_stored_put_foreign_ref_survives_source_close_and_task_argument_import():
    _run_contained_put(reconstruct=False)


def test_stored_put_dependency_survives_consumer_reconstruction():
    _run_contained_put(reconstruct=True)


def _run_contained_put(*, reconstruct):
    context = runtime = core = report = None
    parent = source = outer = independent = consumer = None
    original_rpc = original_borrow = original_push = None
    managed_pids, managed_addresses = set(), set()
    observations, cleanup_errors = [], []
    observed_lock = threading.Lock()
    observation_failed = overflow = False
    cleanup_deadline = None

    def remember(address, handler, request, reply):
        nonlocal observation_failed, overflow
        try:
            with observed_lock:
                if len(observations) < _MAX_OBSERVATIONS:
                    observations.append((address, handler, request, reply))
                else:
                    overflow = True
        except Exception:
            observation_failed = True

    def inspect_rpc(address, handler, request):
        reply = original_rpc(address, handler, request)
        if handler in ("request_worker_lease", "seal_object", "get_object", "drop_object_replica"):
            remember(address, handler, request, reply)
        return reply

    def inspect_borrow(address, handler, request):
        reply = original_borrow(address, handler, request)
        if handler in ("acquire_borrowed_object", "release_borrowed_object",
                       "prepare_stored_contained_pin", "promote_stored_contained_pin",
                       "release_contained_reference"):
            remember(address, handler, request, reply)
        return reply

    def inspect_push(address, handler, request):
        reply = original_push(address, handler, request)
        if handler == "push_task":
            remember(address, handler, request, reply)
        return reply

    try:
        context = ray.init(
            num_nodes=1, num_cpus=2, num_workers_per_node=2,
            inline_threshold=1024, object_store_bytes=1024 * 1024,
            enable_tracing=False,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        runtime = _get_runtime()
        core = runtime.core_worker
        if runtime.owner_service is not None:
            managed_addresses.add(runtime.owner_service.address)
        assert runtime.owner_service is not None and context.trace_address is None
        assert len(context.nodes) == 1 and len(context.worker_ids) == 2
        assert len(managed_pids) == 4 and len(managed_addresses) == 5
        assert os.getpid() not in managed_pids
        initial_submission_index = core._submission_index
        original_rpc, original_borrow, original_push = core._rpc, core._borrow_rpc, core._push_task_rpc
        core._rpc, core._borrow_rpc, core._push_task_rpc = inspect_rpc, inspect_borrow, inspect_push

        parent = _return_child.remote(41)
        source = ray.get(parent, timeout=_remaining(deadline))
        assert isinstance(source, ray.ObjectRef) and source.borrower_token is not None
        assert source.owner_worker_id in context.worker_ids
        assert source.owner_address in context.worker_addresses
        assert ray.get(source, timeout=_remaining(deadline)) == 42
        _wait_local(core, lambda: core._accepted_task_count == 0 and not core._protocol_unresolved
                    and parent.object_id not in core._task_finish_barriers, deadline)
        assert core._submission_index == initial_submission_index + 1
        _close(parent, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        _wait_local(core, lambda: core.owner_table.collection_state(parent.object_id)
                    is ObjectCollectionState.COLLECTED, deadline)

        _remaining(deadline)
        outer = ray.put({"ref": source, "alias": source, "padding": b"P" * _PADDING_BYTES})
        _remaining(deadline)
        put_snapshot = core.owner_table.snapshot(outer.object_id)
        assert put_snapshot.state is ObjectState.READY_STORED
        assert put_snapshot.producer_task_spec is None and put_snapshot.output_publication is None
        assert core._recovery.lineage_for_object(outer.object_id) is None
        assert core._submission_index == initial_submission_index + 1
        assert core._accepted_task_count == 0 and core._inflight_puts == 0
        assert outer.object_id not in core._task_finish_barriers
        assert outer.object_id not in getattr(core, "_put_handoffs", {})
        edge, = put_snapshot.outgoing_contained_edges
        assert edge.contained_object_id == source.object_id
        assert edge.contained_owner_worker_id == source.owner_worker_id
        descriptor = put_snapshot.canonical_stored_result
        assert descriptor is not None and descriptor.storage is protocol.ResultStorage.OBJECT_STORE
        assert _PADDING_BYTES <= descriptor.size_bytes <= 4 * 1024
        assert descriptor.node_id == context.node_id
        assert put_snapshot.locations == frozenset({context.node_id})
        assert put_snapshot.current_attempt.attempt_number == 0

        source_key = _borrow_key(source, core)
        _close(source, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        _wait_local(core, lambda: source_key not in core._borrowed_release_obligations, deadline)
        restored = ray.get(outer, timeout=_remaining(deadline))
        independent = restored["ref"]
        assert restored["alias"] is independent and restored["padding"] == b"P" * _PADDING_BYTES
        assert independent.object_id == source.object_id and independent is not source
        assert independent.borrower_token is not None and independent.borrower_token != source.borrower_token
        assert independent.owner_worker_id == source.owner_worker_id
        assert ray.get(independent, timeout=_remaining(deadline)) == 42

        consumer = _consume_put.remote(outer, deadline)
        assert core._submission_index == initial_submission_index + 2
        consumer_spec = core.owner_table.snapshot(consumer.object_id).producer_task_spec
        assert consumer_spec is not None and len(consumer_spec.args) == 2
        assert consumer_spec.args[0] == protocol.RefArg(outer.object_id, core.worker_id)
        assert type(consumer_spec.args[1]) is protocol.InlineArg
        _close(outer, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        first_value = ray.get(consumer, timeout=_remaining(deadline))
        assert first_value["value"] == 42 and first_value["padding"] == b"R" * _PADDING_BYTES
        assert first_value["pid"] in context.worker_pids
        _wait_local(core, lambda: core._accepted_task_count == 0 and not core._protocol_unresolved
                    and consumer.object_id not in core._task_finish_barriers, deadline)
        first_consumer = core.owner_table.snapshot(consumer.object_id)
        assert first_consumer.state is ObjectState.READY_STORED
        assert first_consumer.current_attempt.attempt_number == 0
        assert _PADDING_BYTES <= first_consumer.canonical_stored_result.size_bytes <= 4 * 1024
        retained_put = core.owner_table.snapshot(outer.object_id)
        assert outer.closed and not retained_put.local_tokens
        assert retained_put.lineage_tokens and retained_put.state is ObjectState.READY_STORED
        assert retained_put.current_attempt == put_snapshot.current_attempt
        assert retained_put.outgoing_contained_edges == put_snapshot.outgoing_contained_edges
        assert core.owner_table.collection_state(outer.object_id) is ObjectCollectionState.ACTIVE
        if reconstruct:
            _remaining(deadline)
            assert ray.drop_object(consumer)
            lost = core.owner_table.snapshot(consumer.object_id)
            assert lost.state is ObjectState.LOST and lost.current_attempt == first_consumer.current_attempt
            after_drop_put = core.owner_table.snapshot(outer.object_id)
            assert after_drop_put.state is ObjectState.READY_STORED
            assert after_drop_put.lineage_tokens == retained_put.lineage_tokens
            assert after_drop_put.current_attempt == put_snapshot.current_attempt
            assert after_drop_put.canonical_stored_result == descriptor
            assert after_drop_put.outgoing_contained_edges == put_snapshot.outgoing_contained_edges
            rebuilt = ray.get(consumer, timeout=_remaining(deadline))
            assert rebuilt["value"] == 42 and rebuilt["padding"] == b"R" * _PADDING_BYTES
            assert rebuilt["pid"] in context.worker_pids
            _wait_local(core, lambda: core._accepted_task_count == 0 and not core._protocol_unresolved
                        and consumer.object_id not in core._task_finish_barriers, deadline)
            replacement = core.owner_table.snapshot(consumer.object_id)
            assert replacement.state is ObjectState.READY_STORED
            assert replacement.current_attempt == first_consumer.current_attempt.next()
            assert replacement.current_attempt.task_id == consumer.object_id.task_id
            assert core._submission_index == initial_submission_index + 2
            assert core._recovery.task_record(consumer.object_id.task_id).retries_started == 1
            assert core.owner_table.snapshot(outer.object_id).current_attempt == put_snapshot.current_attempt
            assert core.owner_table.snapshot(outer.object_id).outgoing_contained_edges == put_snapshot.outgoing_contained_edges
        else:
            assert core._recovery.task_record(consumer.object_id.task_id).retries_started == 0
        # Collection of the consumer result releases any retained input lineage.
        _close(consumer, min(deadline, time.monotonic() + _CLEANUP_SECONDS))
        _wait_local(core, lambda: core.owner_table.collection_state(outer.object_id)
                    is ObjectCollectionState.COLLECTED, deadline)
        assert outer.object_id not in core._object_gc_obligations
        assert ray.get(independent, timeout=_remaining(deadline)) == 42
        assert core._recovery.lineage_for_object(outer.object_id) is None

        with observed_lock:
            exchanges = tuple(observations)
        assert not observation_failed and not overflow
        seals = [request for _, handler, request, reply in exchanges
                 if handler == "seal_object" and request.object_id == outer.object_id and reply.sealed]
        assert seals and all(request == seals[0] for request in seals)
        fetches = [(request, reply) for _, handler, request, reply in exchanges
                   if handler == "get_object" and request.object_id == outer.object_id]
        assert fetches and all(reply.found and reply.sealed and reply.checksum == descriptor.checksum
                               and reply.size_bytes == descriptor.size_bytes for _, reply in fetches)
        pushes = [request for _, handler, request, _ in exchanges
                  if handler == "push_task" and request.spec.task_id == consumer.object_id.task_id]
        by_attempt = {}
        for request in pushes:
            previous = by_attempt.setdefault(request.spec.attempt_id.attempt_number, request)
            assert request == previous
        assert set(by_attempt) == ({0, 1} if reconstruct else {0})
        expected_dependency = protocol.ObjectStoreDescriptor(
            outer.object_id, core.worker_id, put_snapshot.current_attempt,
            context.node_id, descriptor.size_bytes, descriptor.checksum,
        )
        assert all(request.dependencies == (expected_dependency,) for request in pushes)
        assert all(request.spec.args == consumer_spec.args for request in pushes)
        assert all(request.spec.return_ids() == (consumer.object_id,) for request in pushes)
        if reconstruct:
            assert by_attempt[0].lease_id != by_attempt[1].lease_id
            assert by_attempt[0].spec.task_id == by_attempt[1].spec.task_id
            assert by_attempt[1].spec.attempt_id == by_attempt[0].spec.attempt_id.next()
        assert all(request.spec.task_id != outer.object_id.task_id
                   for _, handler, request, _ in exchanges if handler == "push_task")
        assert all(request.task_id != outer.object_id.task_id
                   for _, handler, request, _ in exchanges if handler == "request_worker_lease")
        drops = [(request, reply) for _, handler, request, reply in exchanges
                 if handler == "drop_object_replica" and request.object_id == outer.object_id]
        assert drops and all(request == protocol.DropObjectReplica(
            outer.object_id, put_snapshot.current_attempt, core.worker_id, context.node_id, descriptor.checksum
        ) and reply.status in (protocol.DropObjectReplicaStatus.DROPPED,
                               protocol.DropObjectReplicaStatus.ALREADY_DROPPED) for request, reply in drops)
        releases = [(request, reply) for _, handler, request, reply in exchanges
                    if handler == "release_contained_reference"
                    and request.hold == edge.incoming_hold(core.worker_id)]
        assert releases and all(reply.accepted and reply.hold == request.hold
                                and reply.object_id == source.object_id for request, reply in releases)
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        _close(independent, cleanup_deadline)
        independent_key = _borrow_key(independent, core)
        _wait_local(core, lambda: independent_key not in core._borrowed_release_obligations, cleanup_deadline)
    finally:
        if cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            keys = []
            for reference in (consumer, independent, outer, source, parent):
                if reference is not None and core is not None and reference.borrower_token is not None:
                    keys.append(_borrow_key(reference, core))
                try:
                    _close(reference, cleanup_deadline)
                except Exception as exc:
                    cleanup_errors.append(("reference close", repr(exc)))
            if core is not None and keys:
                try:
                    _wait_local(core, lambda: all(key not in core._borrowed_release_obligations
                                                 for key in keys), cleanup_deadline)
                except Exception as exc:
                    cleanup_errors.append(("borrower release", repr(exc)))
        finally:
            try:
                report = ray.shutdown()
            except Exception as exc:
                cleanup_errors.append(("shutdown", repr(exc)))
            finally:
                if core is not None and original_rpc is not None:
                    core._rpc, core._borrow_rpc, core._push_task_rpc = original_rpc, original_borrow, original_push
        if report is not None:
            managed_pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
        survivors = tuple(pid for pid in sorted(managed_pids) if _pid_exists(pid))
        children = tuple(child.pid for child in mp.active_children() if child.pid in managed_pids)
        open_addresses = []
        for address in sorted(managed_addresses):
            try:
                with socket.create_connection(address, timeout=0.1):
                    open_addresses.append(address)
            except OSError:
                pass
        assert not survivors, survivors
        assert not children, children
        assert not open_addresses, open_addresses
        assert not observation_failed and not overflow
        assert not cleanup_errors, cleanup_errors
        assert not ray.is_initialized()

    assert context is not None and report is not None
    assert report.core_stopped and report.gcs_pid == context.gcs_pid
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_clean and report.worker_clean and report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert report.node_exitcodes == (0,) and report.worker_exitcodes == (0, 0)
    assert report.worker_cleans == (True, True) and report.worker_forced == (False, False)
