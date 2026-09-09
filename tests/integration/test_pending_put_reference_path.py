"""Explicit stored put imports a pending nested Ref without a readiness edge.

Two Nodes/one Worker each, five managed children, six runtime endpoints, one
test listener/two connections, two Tasks and one 4 KiB padded put, 1 MiB per
store. Source Task stays PENDING behind its actual user-code gate. Consumer
is pinned to the other Node, pulls only the stored put, imports its child Ref,
and reports entry while that child is still pending. Only explicit get waits.
No test thread, Actor, process failure, retry or implicit StoredArg is used.
Work shares15s after init; socket IO and <=64 observations share that deadline.
Finally releases gates and closes handles within3s, then unconditional normal
shutdown checks every PID/endpoint. Exact30s process-tree runner is required.
"""

import multiprocessing as mp
import os
import socket
import struct
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.ownership import ObjectCollectionState, ObjectState
from tests.integration._retained_path_support import close, remaining, wait_local, _pid_exists

pytestmark = pytest.mark.multiprocess_smoke
_SOURCE = 'pending-put-source'
_TARGET = 'pending-put-target'
_FRAME = struct.Struct('!cQ')
_PADDING = b'P' * 4096


def _announce(address, marker, deadline):
    gate = socket.create_connection(address, timeout=min(2.0, remaining(deadline)))
    gate.settimeout(remaining(deadline))
    gate.sendall(_FRAME.pack(marker, os.getpid()))
    return gate


@ray.remote(num_cpus=1, resources={_SOURCE: 1}, max_retries=0)
def _pending_child(address, deadline):
    with _announce(address, b'S', deadline) as gate:
        if gate.recv(1) != b'G':
            raise RuntimeError('pending source gate did not release')
    return 42


@ray.remote(num_cpus=1, resources={_TARGET: 1}, max_retries=0)
def _consume_container(container, address, deadline):
    child = container['ref']
    assert isinstance(child, ray.ObjectRef) and child is container['again']
    assert container['padding'] == _PADDING
    try:
        with _announce(address, b'C', deadline) as gate:
            if gate.recv(1) != b'G':
                raise RuntimeError('consumer gate did not release')
            return ray.get(child, timeout=remaining(deadline)), os.getpid()
    finally:
        close(child, deadline)


def _accept(listener, expected, deadline):
    listener.settimeout(remaining(deadline))
    connection, _ = listener.accept()
    connection.settimeout(remaining(deadline))
    data = bytearray()
    while len(data) < _FRAME.size:
        chunk = connection.recv(_FRAME.size - len(data))
        if not chunk:
            connection.close()
            raise RuntimeError('partial pending-put gate frame')
        data.extend(chunk)
    marker, pid = _FRAME.unpack(data)
    assert marker == expected
    return connection, pid


def test_stored_put_dependency_imports_pending_child_before_explicit_get():
    context = core = report = listener = source = outer = consumer = None
    pids, addresses, connections, errors = set(), set(), [], []
    original_rpc = original_borrow = original_push = None
    observations, lock = [], threading.Lock()
    overflow = False
    def observe(kind, original, address, handler, message):
        nonlocal overflow
        reply = original(address, handler, message)
        if handler in ('request_worker_lease', 'push_task', 'release_contained_reference', 'get_owned_object'):
            with lock:
                if len(observations) < 64:
                    observations.append((kind, address, handler, message, reply))
                else:
                    overflow = True
        return reply
    try:
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(2)
        address = listener.getsockname()
        addresses.add(address)
        context = ray.init(num_nodes=2, num_workers_per_node=1,
            node_resources=({'CPU': 1, _SOURCE: 1}, {'CPU': 1, _TARGET: 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False)
        deadline = time.monotonic() + 15.0
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        addresses.add(runtime.owner_service.address)
        assert len(pids) == 5 and len(addresses) == 7
        original_rpc, original_borrow, original_push = core._rpc, core._borrow_rpc, core._push_task_rpc
        core._rpc = lambda a,h,m: observe('node', original_rpc, a,h,m)
        core._borrow_rpc = lambda a,h,m: observe('owner', original_borrow, a,h,m)
        core._push_task_rpc = lambda a,h,m: observe('worker', original_push, a,h,m)
        source = _pending_child.remote(address, deadline)
        source_gate, pid = _accept(listener, b'S', deadline)
        connections.append(source_gate)
        assert pid == context.nodes[0].worker_pid
        assert core.owner_table.snapshot(source.object_id).state is ObjectState.PENDING
        outer = ray.put({'ref': source, 'again': source, 'padding': _PADDING})
        stored = core.owner_table.snapshot(outer.object_id)
        assert stored.state is ObjectState.READY_STORED and stored.producer_task_spec is None
        assert core._recovery.lineage_for_object(outer.object_id) is None
        edge, = stored.outgoing_contained_edges
        assert edge.contained_object_id == source.object_id
        assert edge.incoming_hold(core.worker_id) in core.owner_table.snapshot(source.object_id).contained_holds
        consumer = _consume_container.remote(outer, address, deadline)
        spec = core.owner_table.snapshot(consumer.object_id).producer_task_spec
        assert spec.args[0] == protocol.RefArg(outer.object_id, core.worker_id)
        close(source, deadline)
        close(outer, deadline)
        consumer_gate, consumer_pid = _accept(listener, b'C', deadline)
        connections.append(consumer_gate)
        assert consumer_pid == context.nodes[1].worker_pid
        pending = core.owner_table.snapshot(source.object_id)
        assert pending.state is ObjectState.PENDING and not pending.local_tokens
        assert edge.incoming_hold(core.worker_id) in pending.contained_holds
        assert len(pending.borrowed_tokens) == 1
        assert not pending.submitted_tokens and not pending.lineage_tokens
        retained_outer = core.owner_table.snapshot(outer.object_id)
        assert retained_outer.locations == frozenset(context.node_ids)
        assert retained_outer.lineage_tokens and not retained_outer.local_tokens
        assert ray.wait([consumer], timeout=0) == ([], [consumer])
        # Both arrivals precede this release, so source READY cannot have made
        # the consumer's dependency scheduling or contained import succeed.
        consumer_gate.sendall(b'G')
        source_gate.sendall(b'G')
        assert ray.get(consumer, timeout=remaining(deadline)) == (42, consumer_pid)
        wait_local(core, lambda: core._accepted_task_count == 0 and not core._task_finish_barriers, deadline)
        with lock:
            exchanges = tuple(observations)
        pushes = [message for _,_,handler,message,_ in exchanges if handler == 'push_task'
                  and message.spec.task_id == consumer.object_id.task_id]
        assert len(pushes) == 1 and len(pushes[0].dependencies) == 1
        assert pushes[0].dependencies[0].object_id == outer.object_id
        assert pushes[0].dependencies[0].node_id == context.nodes[1].node_id
        assert source.object_id not in {item.object_id for item in pushes[0].dependencies}
        close(consumer, deadline)
        wait_local(core, lambda: all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
                                    for ref in (source, outer, consumer)), deadline)
        assert not core._object_gc_obligations and not overflow
    finally:
        deadline = time.monotonic() + 3.0
        for connection in connections:
            try:
                connection.settimeout(min(0.1, max(0.001, deadline - time.monotonic())))
                connection.sendall(b'G')
            except OSError:
                pass
            finally:
                connection.close()
        if listener is not None:
            listener.close()
        for ref in (consumer, outer, source):
            try:
                close(ref, deadline)
            except Exception as exc:
                errors.append(repr(exc))
        try:
            report = ray.shutdown()
        finally:
            if core is not None and original_rpc is not None:
                core._rpc, core._borrow_rpc, core._push_task_rpc = original_rpc, original_borrow, original_push
            if report is not None:
                pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
            survivors = tuple(pid for pid in pids if _pid_exists(pid))
            active = tuple(child.pid for child in mp.active_children() if child.pid in pids)
            open_addresses = []
            for endpoint in addresses:
                try:
                    with socket.create_connection(endpoint, timeout=0.1):
                        open_addresses.append(endpoint)
                except OSError:
                    pass
            assert not survivors and not active and not open_addresses, (survivors, active, open_addresses)
            assert not errors, errors
            assert not ray.is_initialized()
    assert context is not None and report is not None
    assert report.core_stopped and report.gcs_clean and report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized and report.shutdown_ack_clean and not report.forced
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert report.gcs_exitcode == 0 and report.node_exitcodes == report.worker_exitcodes == (0, 0)
