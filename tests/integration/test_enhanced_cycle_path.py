"""Real publication graph evidence through supported put/Task/replay APIs.

Positive case: one Node/Worker, three children/four endpoints, two tiny puts,
one Task and one 1 MiB store. It proves actual put and Task COMMIT/RELEASE.
Sequential cycle: one Node/Worker, three TaskIDs/four executions, one put,
one stored result dropped once. Persistent Worker-local B=put([A]) remains
live when whole reconstruction A proposes A->B against committed B->A.
Concurrent cycle: two Nodes/one Worker each, five children/six endpoints,
six TaskIDs/eight executions, two puts/two 1 MiB stores. X->B and Y->A are
committed before reconstructed A->X and B->Y race for the same graph.

Cycle cases add exactly one test listener/thread. It handles two/four fixed
connections: before actual C1 sends, then after original C1 replies but before
either Node advances. Actual reply tags and GCS queries must show CYCLE and
(concurrent only) a still-PREPARED winner, never a fabricated witness. No
process failure, Actor, synthetic ObjectID/borrower, patched authority or DFS.
Work shares fifteen seconds after init; gate handling shares that deadline
and the production gate's ten seconds. All waits/polls are bounded; finally
closes references and saved Worker puts in one three-second epoch, releases
gate sockets, joins the test thread, then always shuts down and checks every
PID/endpoint. Each selector runs under the exact 30-second tree runner.
"""

from contextlib import contextmanager
from functools import partial
import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import api as api_module, enhanced_publication as ep, protocol
from miniray.api import _get_runtime
from miniray.node import GET_OBJECT_HANDLER
from miniray.output_handoff import OutputHandoffPhase
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_gate import (
    OUTPUT_PUBLICATION_GATE_RELEASE, GraphReservationOutcome,
    OutputPublicationGateConfig, OutputPublicationGatePhase,
    recv_output_publication_gate_arrival,
)
from miniray.transport import request as rpc_request
from tests.integration import _cycle_runtime_state as helpers
from tests.integration._retained_path_support import Case, _pid_exists, close, remaining, wait_local


pytestmark = pytest.mark.multiprocess_smoke
_ACTUAL_NODE_MAIN = api_module._node_process_main
_RESOURCES = ("cycle_worker_a", "cycle_worker_b")


def _query(context, request, deadline):
    reply = rpc_request(context.gcs_address, ep.PUBLICATION_HANDLER, request,
                        connect_timeout=min(0.5, remaining(deadline)),
                        request_timeout=min(2.0, remaining(deadline)), deadline=deadline)
    assert type(reply) is ep.PublicationReply and reply.request == request and reply.accepted
    assert reply.snapshot is not None and reply.snapshot.reference == request.reference
    return reply.snapshot


def _graph(context, reference, deadline):
    return _query(context, ep.GetPublication(reference), deadline)


def _wait_graph_retired(context, reference, deadline):
    pause = threading.Event()
    for index in range(96):
        snapshot = _graph(context, reference, deadline)
        if snapshot.receipt(ep.PublicationStage.RETIRED) is not None:
            assert not snapshot.graph_active and not snapshot.forward_open
            assert snapshot.closed_holds is not None and snapshot.closed_holds.reference == reference
            return snapshot
        if index < 95:
            pause.wait(min(0.05, remaining(deadline)))
    raise TimeoutError("actual graph retirement did not converge")


def _finish(core, references, deadline):
    wait_local(core, lambda: core._accepted_task_count == 0 and not core._protocol_unresolved
               and all(ref.object_id not in core._task_finish_barriers for ref in references), deadline)


def _gated_node_main(*args, gate_address):
    """Configure only the existing private gate on each original Node entry."""
    arguments = list(args)
    node_index = 0 if arguments[1].get(_RESOURCES[0], 0) else 1
    arguments[-1] = OutputPublicationGateConfig(
        node_index=node_index, address=gate_address, graph_reservation_barrier=True,
        attempt_number=1, timeout_seconds=10.0,
    )
    _ACTUAL_NODE_MAIN(*arguments)


class _GraphRound:
    """One finite synchronization round; never creates protocol replies."""

    def __init__(self, parties):
        assert parties in (1, 2)
        self.parties = parties
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(parties)
        self.address = self.listener.getsockname()
        self.connections = []
        self.arrivals = []
        self.snapshots = []
        self.errors = []
        self.thread = None
        self.stop = threading.Event()
        self.lock = threading.Lock()

    def _accept(self, deadline):
        for _ in range(160):
            if self.stop.is_set():
                raise RuntimeError("graph round cleanup interrupted accept")
            self.listener.settimeout(min(0.1, remaining(deadline)))
            try:
                return self.listener.accept()[0]
            except socket.timeout:
                pass
        raise TimeoutError("graph round did not receive its fixed connection")

    def start(self, context, object_ids, deadline):
        assert self.thread is None and len(object_ids) == self.parties

        def run():
            try:
                for phase in (OutputPublicationGatePhase.BEFORE_GRAPH_PREPARE,
                              OutputPublicationGatePhase.AFTER_GRAPH_PREPARE_REPLY):
                    batch = []
                    for _ in range(self.parties):
                        connection = self._accept(deadline)
                        with self.lock:
                            self.connections.append(connection)
                        connection.settimeout(remaining(deadline))
                        arrival = recv_output_publication_gate_arrival(connection)
                        assert arrival.phase is phase
                        assert arrival.publication_id.attempt_id.attempt_number == 1
                        assert arrival.publication_id.object_id in object_ids
                        batch.append((arrival, connection))
                    assert len({arrival.publication_id.object_id for arrival, _ in batch}) == self.parties
                    self.arrivals.extend(arrival for arrival, _ in batch)
                    if phase is OutputPublicationGatePhase.BEFORE_GRAPH_PREPARE:
                        assert all(arrival.graph_outcome is GraphReservationOutcome.UNOBSERVED for arrival, _ in batch)
                    else:
                        expected = ([GraphReservationOutcome.CYCLE] if self.parties == 1 else
                                    [GraphReservationOutcome.ACCEPTED, GraphReservationOutcome.CYCLE])
                        assert sorted(arrival.graph_outcome.value for arrival, _ in batch) == sorted(value.value for value in expected)
                        for arrival, _ in batch:
                            reference = ep.PublicationRef(arrival.publication_id, arrival.manifest_digest)
                            snapshot = _graph(context, reference, deadline)
                            assert snapshot.complete is None and snapshot.adoption is None
                            assert snapshot.receipt(ep.PublicationStage.COMMITTED) is None
                            assert snapshot.receipt(ep.PublicationStage.ARMED) is None
                            if arrival.graph_outcome is GraphReservationOutcome.ACCEPTED:
                                assert snapshot.receipt(ep.PublicationStage.PREPARED) is not None and snapshot.graph_active
                            else:
                                assert snapshot.receipt(ep.PublicationStage.PREPARED) is None and not snapshot.graph_active
                            self.snapshots.append((arrival, snapshot))
                    for _, connection in batch:
                        connection.settimeout(remaining(deadline))
                        connection.sendall(OUTPUT_PUBLICATION_GATE_RELEASE)
                        connection.close()
                        with self.lock:
                            self.connections.remove(connection)
            except BaseException as exc:
                if not self.stop.is_set():
                    self.errors.append(exc)
            finally:
                self._release_connections()

        self.thread = threading.Thread(target=run, name="enhanced-graph-round", daemon=False)
        self.thread.start()

    def _release_connections(self):
        with self.lock:
            connections, self.connections = self.connections, []
        for connection in connections:
            try:
                connection.settimeout(0.1)
                connection.sendall(OUTPUT_PUBLICATION_GATE_RELEASE)
            except OSError:
                pass
            finally:
                connection.close()

    def join(self, deadline):
        assert self.thread is not None
        self.thread.join(remaining(deadline))
        assert not self.thread.is_alive(), "graph round thread did not finish"
        assert not self.errors, self.errors
        assert len(self.arrivals) == 2 * self.parties and len(self.snapshots) == self.parties

    def close(self, deadline):
        self.stop.set()
        self._release_connections()
        self.listener.close()
        if self.thread is not None:
            self.thread.join(max(0.0, deadline - time.monotonic()))
            assert not self.thread.is_alive(), "graph round thread survived cleanup"


@contextmanager
def _cluster(monkeypatch, nodes, gate=None):
    context = case = report = None
    pids, addresses, errors, clearers = set(), set(), [], []
    if gate is not None:
        monkeypatch.setattr(api_module, "_node_process_main", partial(_gated_node_main, gate_address=gate.address))
        addresses.add(gate.address)
    try:
        context = ray.init(num_nodes=nodes, num_workers_per_node=1,
                           node_resources=tuple({"CPU": 1, _RESOURCES[index]: 1} for index in range(nodes)),
                           inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False)
        pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        addresses.add(runtime.owner_service.address)
        case = Case(context, runtime.core_worker, time.monotonic() + 15.0)
        for index in range(nodes):
            clearers.append(ray.remote(num_cpus=0, resources={_RESOURCES[index]: 1}, max_retries=0)(helpers.clear))
        assert len(pids) == 1 + 2 * nodes and len(addresses) == 2 + 2 * nodes + (gate is not None)
        yield case, clearers
    finally:
        deadline = time.monotonic() + 3.0
        if gate is not None:
            try:
                gate.close(deadline)
            except Exception as exc:
                errors.append(("gate cleanup", repr(exc)))
        try:
            if case is not None:
                # Tests remove clearers after successful explicit cleanup. On
                # failure each configured Worker gets at most one cleanup Task.
                for clearer in clearers:
                    try:
                        ref = case.keep(clearer.remote(deadline))
                        ray.get(ref, timeout=remaining(deadline))
                    except Exception as exc:
                        errors.append(("saved put cleanup", repr(exc)))
                keys = []
                for ref in reversed(case.refs):
                    if ref.borrower_token is not None:
                        keys.append((ref.owner_worker_id, ref.object_id, case.core.worker_id, ref.borrower_token))
                    try:
                        close(ref, deadline)
                    except Exception as exc:
                        errors.append(("close", repr(exc)))
                try:
                    wait_local(case.core, lambda: all(key not in case.core._borrowed_release_obligations for key in keys), deadline)
                except Exception as exc:
                    errors.append(("borrower cleanup", repr(exc)))
        finally:
            try:
                report = ray.shutdown()
            except Exception as exc:
                errors.append(("shutdown", repr(exc)))
        if report is not None:
            pids.update((report.gcs_pid, *report.node_pids, *report.worker_pids))
        survivors = tuple(pid for pid in pids if _pid_exists(pid))
        active = tuple(child.pid for child in mp.active_children() if child.pid in pids)
        listening = []
        for address in addresses:
            try:
                with socket.create_connection(address, timeout=0.1):
                    listening.append(address)
            except OSError:
                pass
        assert not survivors and not active and not listening, (survivors, active, listening)
        assert not errors, errors
        assert not ray.is_initialized()
        if context is not None:
            assert report is not None and report.core_stopped
            assert report.gcs_clean and report.node_clean and report.worker_clean and report.resources_clean
            assert report.finalized and report.shutdown_ack_clean and not report.forced
            assert report.gcs_pid == context.gcs_pid and report.worker_pids == context.worker_pids
            assert report.node_pids == context.node_pids and report.gcs_exitcode == 0
            assert report.node_exitcodes == (0,) * nodes and report.worker_exitcodes == (0,) * nodes


def test_public_put_and_task_graphs_commit_then_release(monkeypatch):
    with _cluster(monkeypatch, 1) as (case, clearers):
        clearers.clear()  # This case creates no persistent Worker module state.
        core, context, deadline = case.core, case.context, case.deadline
        child = case.keep(ray.put(42))
        box = case.keep(ray.put([child]))
        box_publication = core._publication_client().current(box.object_id)
        assert type(box_publication) is ep.PutPublication
        committed_box = _graph(context, box_publication.reference, deadline)
        assert committed_box.graph_active and committed_box.receipt(ep.PublicationStage.COMMITTED) is not None
        assert committed_box.complete is None and committed_box.adoption is None
        assert len(committed_box.publication.transfers) == 1
        assert committed_box.publication.transfers[0].contained_object_id == child.object_id
        function = ray.remote(num_cpus=1, max_retries=0)(helpers.echo_container)
        outer = case.keep(function.remote(box))
        value = ray.get(outer, timeout=remaining(deadline))
        restored = case.keep(value["child"])
        assert value["padding"] == helpers.PADDING and ray.get(restored, timeout=remaining(deadline)) == 42
        _finish(core, [outer], deadline)
        publication = core._publication_client().current(outer.object_id)
        assert type(publication) is ep.TaskPublication
        committed_outer = _graph(context, publication.reference, deadline)
        assert committed_outer.graph_active and committed_outer.complete is not None and committed_outer.adoption is not None
        assert committed_outer.receipt(ep.PublicationStage.COMMITTED) is not None
        assert committed_outer.publication.transfers[0].contained_object_id == child.object_id
        descriptor = core.owner_table.snapshot(outer.object_id).canonical_stored_result
        close(restored, deadline)
        close(outer, deadline)
        close(box, deadline)
        close(child, deadline)
        wait_local(core, lambda: all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
                                    for ref in (outer, box, child)), deadline)
        retired_outer = _wait_graph_retired(context, publication.reference, deadline)
        retired_box = _wait_graph_retired(context, box_publication.reference, deadline)
        assert retired_outer.complete == committed_outer.complete and retired_outer.adoption == committed_outer.adoption
        assert retired_box.receipt(ep.PublicationStage.COMMITTED) == committed_box.receipt(ep.PublicationStage.COMMITTED)
        query = protocol.GetObject(outer.object_id, context.node_id, publication.reference.key.attempt_id,
                                   core.worker_id, descriptor.size_bytes, descriptor.checksum)
        absent = rpc_request(context.node_address, GET_OBJECT_HANDLER, query,
                             connect_timeout=min(0.5, remaining(deadline)),
                             request_timeout=min(2.0, remaining(deadline)), deadline=deadline)
        assert not absent.found and absent.data is None


def test_public_reconstruction_rejects_reachable_two_object_cycle(monkeypatch):
    _run_cycle(monkeypatch, nodes=1)


def test_concurrent_real_reservations_reject_one_four_object_cycle(monkeypatch):
    _run_cycle(monkeypatch, nodes=2)


def _run_cycle(monkeypatch, *, nodes):
    gate = _GraphRound(nodes)
    with _cluster(monkeypatch, nodes, gate) as (case, clearers):
        core, context, deadline = case.core, case.context, case.deadline
        producers = [ray.remote(num_cpus=1, resources={_RESOURCES[index]: 1}, max_retries=1)(helpers.produce)
                     for index in range(nodes)]
        retainers = [ray.remote(num_cpus=0, resources={_RESOURCES[index]: 1}, max_retries=0)(helpers.retain)
                    for index in range(nodes)]
        outputs = [case.keep(producer.remote()) for producer in producers]
        initial = ray.get(outputs, timeout=remaining(deadline))
        assert [value["pid"] for value in initial] == list(context.worker_pids)
        assert all(value["calls"] == 1 and value["padding"] == helpers.PADDING for value in initial)
        _finish(core, outputs, deadline)
        first_publications = [core._publication_client().current(ref.object_id) for ref in outputs]
        assert all(publication.reference.key.attempt_id.attempt_number == 0 for publication in first_publications)
        setup = [case.keep(retainer.remote([outputs[(index + 1) % nodes]]))
                 for index, retainer in enumerate(retainers)]
        saved = ray.get(setup, timeout=remaining(deadline))
        _finish(core, setup, deadline)
        assert [value[0] for value in saved] == list(context.worker_pids)
        boxes = []
        for index, (_pid, object_id, owner_id, reference) in enumerate(saved):
            assert owner_id == context.worker_ids[index]
            snapshot = _graph(context, reference, deadline)
            assert type(snapshot.publication) is ep.PutPublication and snapshot.publication.object_id == object_id
            assert snapshot.graph_active and snapshot.receipt(ep.PublicationStage.COMMITTED) is not None
            transfer, = snapshot.publication.transfers
            target = outputs[(index + 1) % nodes]
            assert transfer.contained_object_id == target.object_id
            assert transfer.final_hold in core.owner_table.snapshot(target.object_id).contained_holds
            boxes.append(snapshot)
        for ref in setup:
            close(ref, deadline)
        wait_local(core, lambda: all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
                                    for ref in setup), deadline)
        for ref in outputs:
            assert ray.drop_object(ref)
            assert core.owner_table.snapshot(ref.object_id).state is ObjectState.LOST
        gate.start(context, {ref.object_id for ref in outputs}, deadline)
        ready, pending = ray.wait(outputs, num_returns=nodes, timeout=remaining(deadline))
        assert set(ready) == set(outputs) and not pending
        gate.join(deadline)
        _finish(core, outputs, deadline)
        rejected = accepted = None
        rebuilt_handles = []
        for arrival, at_reply in gate.snapshots:
            object_id = arrival.publication_id.object_id
            index = [ref.object_id for ref in outputs].index(object_id)
            transfer, = at_reply.publication.transfers
            assert transfer.contained_object_id == saved[index][1]
            assert at_reply.publication.manifest.header.executor_worker_id == context.worker_ids[index]
            assert arrival.node_pid == context.node_pids[index]
            ref = outputs[index]
            snapshot = core.owner_table.snapshot(object_id)
            assert snapshot.current_attempt == first_publications[index].reference.key.attempt_id.next()
            assert core._recovery.task_record(object_id.task_id).retries_started == 1
            if arrival.graph_outcome is GraphReservationOutcome.CYCLE:
                rejected = at_reply.reference
                assert snapshot.state is ObjectState.ERROR and not snapshot.outgoing_contained_edges
                with pytest.raises(ray.SystemTaskError):
                    ray.get(ref, timeout=remaining(deadline))
                handoff = core._output_handoff_table().query(arrival.publication_id)
                assert handoff is not None and handoff.phase is OutputHandoffPhase.ABORTED
                assert handoff.complete is None and handoff.adoption is None
                retired = _wait_graph_retired(context, rejected, deadline)
                assert retired.receipt(ep.PublicationStage.PREPARED) is None and retired.complete is None
                assert retired.closed_holds.rollback_scope is not None
                scope = retired.closed_holds.rollback_scope
                assert scope.prepare_intents == scope.promote_intents == () and not scope.materialization_started
            else:
                accepted = at_reply.reference
                value = ray.get(ref, timeout=remaining(deadline))
                restored = case.keep(value["box"])
                rebuilt_handles.append(restored)
                assert value["pid"] == initial[index]["pid"] and value["calls"] == 2
                assert value["padding"] == helpers.PADDING and restored.object_id == saved[index][1]
                assert snapshot.state is ObjectState.READY_STORED and len(snapshot.outgoing_contained_edges) == 1
                committed = _graph(context, accepted, deadline)
                assert committed.graph_active and committed.complete is not None and committed.adoption is not None
        assert rejected is not None and (accepted is not None) is (nodes == 2)
        for publication in first_publications:
            old = _wait_graph_retired(context, publication.reference, deadline)
            assert old.receipt(ep.PublicationStage.COMMITTED) is not None and old.complete is not None
        # Empty setup outputs cannot accidentally keep A/B through Task lineage.
        for ref in setup:
            assert core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
        cleanup_deadline = min(deadline, time.monotonic() + 3.0)
        clear_outputs = [case.keep(clearer.remote(cleanup_deadline)) for clearer in clearers]
        clearers.clear()
        cleared = ray.get(clear_outputs, timeout=remaining(cleanup_deadline))
        assert [value[0] for value in cleared] == list(context.worker_pids)
        assert all(value[1:] == (2, True) for value in cleared)
        for ref in rebuilt_handles + outputs + clear_outputs:
            close(ref, cleanup_deadline)
        for snapshot in boxes:
            _wait_graph_retired(context, snapshot.reference, cleanup_deadline)
        if accepted is not None:
            _wait_graph_retired(context, accepted, cleanup_deadline)
        wait_local(core, lambda: all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
                                    for ref in outputs + clear_outputs), cleanup_deadline)
