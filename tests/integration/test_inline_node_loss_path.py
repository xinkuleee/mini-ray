"""Bounded unified-output publishing-Node-loss acceptance for one INLINE slot.

Both tests use a delivery gate after local Complete/resource release and the
exact owner terminal ACK, before any Complete/outcome envelope can leave that
Node. KEEP releases that gate and pauses the real received envelope at the
owner-adoption call boundary; DROP crashes with the delivery gate unopened.
No Driver wrapper discards a result or fabricates a transport failure.

Static bounds per exact invocation: spawn; one GCS, two Nodes, one Worker and
one CPU per Node (five managed children); one local blocker and one producer
with at most one reconstruction; one Driver put; 1 MiB store per Node; only
tiny INLINE objects; one test-owned loopback listener; no Actor/PG/tracing or
extra test thread.  Work shares a 10-second monotonic deadline, every gate has
a finite timeout, and the external allowlisted runner imposes 30 seconds.

The child is Driver-owned and reaches the producer as a nested handle.  Thus
publisher death does not kill the child's owner.  Its exact borrowed-source
publication and reconstruction hold rewriting are part of this narrow slice.
The retained test names describe the INLINE storage tier, not the retired
single-return protocol.  These tests do not cover unknown Complete, mixed-tier
batches, targeted outputs or outer-owner death.
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass
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
from miniray.ids import (
    AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID,
)
from miniray.node import REQUEST_LEASE_HANDLER
from miniray.output_publication import OutputPublicationEnvelope, OutputPublicationID
from miniray.output_handoff import NodeLostOutputResolution, OutputHandoffPhase
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_gate import (
    OUTPUT_PUBLICATION_GATE_RELEASE, OutputPublicationGateConfig,
    OutputPublicationGatePhase, recv_output_publication_gate_arrival,
)
from miniray.recovery import TaskState
from miniray.publication_sources import BorrowedContainedSource
from miniray.transport import request as rpc_request
from miniray.worker import PUSH_TASK_HANDLER


pytestmark = pytest.mark.multiprocess_smoke

_SURVIVOR_RESOURCE = "inline_node_loss_survivor"
_WORK_SECONDS = 10.0
_GATE_SECONDS = 10.0
_CLEANUP_SECONDS = 5.0
_INLINE_THRESHOLD = 4096
_OBJECT_STORE_BYTES = 1024 * 1024
_BLOCKER_RELEASE = b"B"
_RESULT_MARKER = "inline-node-loss-result"
_CHILD_VALUE = ("surviving-driver-owned-child", 42)


def _remaining(deadline: float) -> float:
    value = deadline - time.monotonic()
    if value <= 0:
        raise TimeoutError("INLINE Node-loss smoke exhausted its work deadline")
    return value


def _enqueue_close(reference: object) -> None:
    """Use ObjectRef's real finalizer without its unbounded close() wait.

    All Driver handles in this test are local owner handles.  The callback
    merely posts their exact normal release token; it performs no I/O.
    """

    if isinstance(reference, ray.ObjectRef):
        reference._closed = True
        if reference._finalizer is not None:
            reference._finalizer()


def _close_with_deadline(reference: ray.ObjectRef, deadline: float) -> None:
    assert reference.borrower_token is None and reference._local_token is not None
    _enqueue_close(reference)
    if reference._release_done is not None:
        assert reference._release_done.wait(_remaining(deadline)), (
            "owner-reference release exceeded the shared work deadline"
        )
    assert reference.closed


@ray.remote(num_cpus=1, resources={_SURVIVOR_RESOURCE: 1}, max_retries=0)
def _occupy_survivor(gate_address: tuple[str, int]) -> int:
    with socket.create_connection(gate_address, timeout=_GATE_SECONDS) as gate:
        gate.settimeout(_GATE_SECONDS)
        gate.sendall(os.getpid().to_bytes(8, "big"))
        if gate.recv(1) != _BLOCKER_RELEASE:
            raise RuntimeError("survivor blocker gate closed without release")
    return os.getpid()


@ray.remote(num_cpus=1, max_retries=1)
def _return_borrowed_inline(container: list[object]) -> tuple[object, str, int]:
    child = container[0]
    if not isinstance(child, ray.ObjectRef):
        raise TypeError("nested argument was materialized instead of imported")
    return child, _RESULT_MARKER, os.getpid()


def _recv_exact(connection: socket.socket, size: int, deadline: float) -> bytes:
    value = bytearray()
    while len(value) < size:
        connection.settimeout(_remaining(deadline))
        chunk = connection.recv(size - len(value))
        if not chunk:
            raise RuntimeError("blocker closed before its fixed-size PID frame")
        value.extend(chunk)
    return bytes(value)


def _assert_metadata_only(value: object) -> None:
    """Walk actual owner/membership wire values without decoding any result payload.

    Opaque identity bytes are permitted.  Object bytes, ResultDescriptors and
    completed envelopes are not; checking the types catches payload custody
    even when a payload happens to differ from the expected tiny result.
    """

    if isinstance(value, (AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID)):
        return
    assert not isinstance(value, (
        bytes, bytearray, memoryview, OutputPublicationEnvelope,
        protocol.ResultDescriptor, protocol.ObjectStoreDescriptor,
    )), "control value carries object data or a result descriptor"
    if is_dataclass(value) and not isinstance(value, type):
        for item in fields(value):
            assert item.name not in (
                "data", "inline_data", "envelope", "descriptor", "payload", "slot_payloads",
            )
            _assert_metadata_only(getattr(value, item.name))
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _assert_metadata_only(item)
    elif isinstance(value, dict):
        for key, item in value.items():
            _assert_metadata_only(key)
            _assert_metadata_only(item)


def _gcs_query(address, handler, request, deadline):
    _assert_metadata_only(request)
    remaining = _remaining(deadline)
    reply = rpc_request(
        address, handler, request,
        connect_timeout=min(0.5, remaining),
        request_timeout=min(2.0, remaining), deadline=deadline,
    )
    _assert_metadata_only(reply)
    return reply


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _assert_shutdown(context, report, death, pids, addresses) -> None:
    assert context is not None and report is not None and death is not None
    assert not ray.is_initialized()
    assert report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert not report.forced and not report.gcs_forced
    assert report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.node_exitcodes == (0, death.exit_code)
    assert report.node_cleans == (True, False)
    assert report.node_forced == (False, False)
    assert report.node_finalized == (True, False)
    assert report.node_resources_clean == (True, False)
    assert report.node_shutdown_ack_clean == (True, False)
    assert report.worker_exitcodes == (0, None)
    assert report.worker_cleans == (True, False)
    assert report.worker_forced == (False, False)
    assert report.node_deaths[1] == death
    survivor_exit = report.node_deaths[0]
    assert survivor_exit is not None
    assert survivor_exit.node_id == context.nodes[0].node_id
    assert survivor_exit.node_pid == context.nodes[0].node_pid
    assert survivor_exit.reason is protocol.NodeDeathReason.EXPECTED
    assert survivor_exit.exit_code == 0
    # A deliberately crashed Node is not misreported as graceful shutdown.
    assert not report.finalized and not report.shutdown_ack_clean
    assert not report.resources_clean

    deadline = time.monotonic() + _CLEANUP_SECONDS
    wake = threading.Event()
    while any(_pid_exists(pid) for pid in pids):
        remaining = deadline - time.monotonic()
        assert remaining > 0, "managed PIDs survived cleanup: {}".format(
            tuple(pid for pid in pids if _pid_exists(pid))
        )
        # This polls OS cleanup evidence, never selects a fault-injection point.
        wake.wait(min(0.01, remaining))
    assert all(child.pid not in pids for child in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            socket.create_connection(address, timeout=0.1)


def test_received_inline_result_survives_publisher_node_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _run_inline_node_loss(monkeypatch, keep_received=True)


def test_unreceived_inline_result_is_lost_until_explicit_get_reconstructs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _run_inline_node_loss(monkeypatch, keep_received=False)


def _run_inline_node_loss(monkeypatch, *, keep_received: bool) -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connections: list[tuple[socket.socket, bytes]] = []
    context = runtime = core = report = death = arrival = None
    blocker = source = outer = first = second = None
    source_id = outer_id = None
    received_envelope = None
    received_envelope_holder = []
    work_deadline = 0.0
    before_commit = threading.Event()
    allow_commit = threading.Event()
    old_finalized = threading.Event()
    rebuilt_finalized = threading.Event()
    all_collected = threading.Event()
    observer_lock = threading.Lock()
    observed_leases: list[tuple[object, object]] = []
    observed_pushes: list[protocol.PushTask] = []
    received_envelopes: list[OutputPublicationEnvelope] = []
    gcs_calls: list[tuple[object, object]] = []
    generic_retries: list[object] = []
    expected_collection_ids: set[str] = set()
    collected_ids: set[str] = set()
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        listener.settimeout(_GATE_SECONDS)
        gate_address = listener.getsockname()
        managed_addresses.add(gate_address)
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=(
                {"CPU": 1, _SURVIVOR_RESOURCE: 1}, {"CPU": 1},
            ),
            inline_threshold=_INLINE_THRESHOLD,
            object_store_bytes=_OBJECT_STORE_BYTES, enable_tracing=False,
            _test_output_publication_gate=OutputPublicationGateConfig(
                node_index=1, address=gate_address,
                phase=OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY,
                timeout_seconds=_GATE_SECONDS,
            ),
        )
        work_deadline = time.monotonic() + _WORK_SECONDS
        survivor, victim = context.nodes
        runtime = _get_runtime()
        core = runtime.core_worker
        assert runtime.owner_service is not None
        managed_pids.update({context.gcs_pid, *context.node_pids, *context.worker_pids})
        managed_addresses.update({
            context.gcs_address, *context.node_addresses,
            *context.worker_addresses, runtime.owner_service.address,
        })
        assert len(managed_pids) == 5 and os.getpid() not in managed_pids
        assert len(managed_addresses) == 7
        assert all(len(node.worker_pids) == 1 for node in context.nodes)
        assert context.trace_address is None

        original_rpc = core._rpc
        original_push = core._push_task_rpc
        original_retry = core._retry_system_failure
        original_adoption = core._drive_output_publication_adoption
        original_finish = core._finish_pending_task
        original_emit = core._emit

        def inspect_rpc(address, handler, message):
            if address == context.gcs_address:
                _assert_metadata_only(message)
            # No result bytes are replaced, dropped, or reconstructed here.
            reply = original_rpc(address, handler, message)
            if address == context.gcs_address:
                _assert_metadata_only(reply)
            with observer_lock:
                if handler == REQUEST_LEASE_HANDLER:
                    observed_leases.append((message, reply))
                if address == context.gcs_address:
                    gcs_calls.append((message, reply))
                envelope = getattr(reply, "output_publication", None)
                if envelope is not None:
                    received_envelopes.append(envelope)
            return reply

        def inspect_push(address, handler, message):
            assert handler == PUSH_TASK_HANDLER
            assert isinstance(message, protocol.PushTask)
            with observer_lock:
                observed_pushes.append(message)
            reply = original_push(address, handler, message)
            envelope = getattr(reply, "output_publication", None)
            if envelope is not None:
                with observer_lock:
                    received_envelopes.append(envelope)
            return reply

        def inspect_adoption(pending, obligation):
            if (keep_received and arrival is not None
                    and obligation.envelope.publication_id == arrival.publication_id
                    and not before_commit.is_set()):
                assert not core._state_lock._is_owned()
                with observer_lock:
                    assert not received_envelope_holder
                    received_envelope_holder.append(obligation.envelope)
                    before_commit.set()
                if not allow_commit.wait(_remaining(work_deadline)):
                    raise TimeoutError("received-envelope adoption boundary was not released")
            return original_adoption(pending, obligation)

        def inspect_retry(pending, error, **options):
            with observer_lock:
                generic_retries.append(pending.task_id)
            return original_retry(pending, error, **options)

        def inspect_finish(pending):
            finished = original_finish(pending)
            if finished and outer_id is not None and pending.task_id == outer_id.task_id:
                if pending.spec.attempt_id.attempt_number == 0:
                    old_finalized.set()
                elif pending.spec.attempt_id.attempt_number == 1:
                    rebuilt_finalized.set()
            return finished

        def inspect_emit(name, **attributes):
            original_emit(name, **attributes)
            if name == "object_collection_completed":
                with observer_lock:
                    collected_ids.add(attributes.get("object_id"))
                    if expected_collection_ids and expected_collection_ids <= collected_ids:
                        all_collected.set()

        monkeypatch.setattr(core, "_rpc", inspect_rpc)
        monkeypatch.setattr(core, "_push_task_rpc", inspect_push)
        monkeypatch.setattr(core, "_retry_system_failure", inspect_retry)
        monkeypatch.setattr(core, "_drive_output_publication_adoption", inspect_adoption)
        monkeypatch.setattr(core, "_finish_pending_task", inspect_finish)
        monkeypatch.setattr(core, "_emit", inspect_emit)

        blocker = _occupy_survivor.remote(gate_address)
        listener.settimeout(_remaining(work_deadline))
        blocker_gate, _peer = listener.accept()
        connections.append((blocker_gate, _BLOCKER_RELEASE))
        assert int.from_bytes(_recv_exact(blocker_gate, 8, work_deadline), "big") == survivor.worker_pid
        source = ray.put(_CHILD_VALUE)
        source_id = source.object_id
        outer = _return_borrowed_inline.remote([source])
        outer_id = outer.object_id
        with observer_lock:
            expected_collection_ids.update(map(str, (blocker.object_id, source_id, outer_id)))
        # remote() installed the nested submitted/lineage holds before returning.
        _close_with_deadline(source, work_deadline)

        listener.settimeout(_remaining(work_deadline))
        publication_gate, _peer = listener.accept()
        connections.append((publication_gate, OUTPUT_PUBLICATION_GATE_RELEASE))
        publication_gate.settimeout(_remaining(work_deadline))
        arrival = recv_output_publication_gate_arrival(publication_gate)
        publication = arrival.publication_id
        assert type(publication) is OutputPublicationID
        assert arrival.phase is OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY
        assert arrival.node_id == victim.node_id
        assert arrival.node_pid == victim.node_pid
        assert arrival.registration_epoch == runtime.nodes[1].registration_epoch
        assert publication.task_id == outer_id.task_id
        assert ((publication.object_id,)) == ((publication.object_id,)) == (outer_id,)
        assert publication.attempt_id.attempt_number == 0

        # The actual Node reported Complete to this owner before opening its
        # delivery gate. Query only that metadata, never reconstruct bytes.
        recovery_query = wire.GetOutputHandoff(publication)
        recovered = _gcs_query(
            runtime.owner_service.address, wire.GET_OUTPUT_HANDOFF_HANDLER,
            recovery_query, work_deadline,
        )
        assert type(recovered) is wire.OutputHandoffReply
        assert recovered.request == recovery_query and recovered.accepted
        prepared = recovered.snapshot
        assert prepared is not None and prepared.phase is OutputHandoffPhase.PENDING and prepared.complete is not None
        assert prepared.publication_id == publication
        assert prepared.manifest.manifest_digest == arrival.manifest_digest
        assert prepared.complete.publication_id == publication
        assert prepared.complete.manifest_digest == arrival.manifest_digest
        assert prepared.complete.status is protocol.TaskReplyStatus.SUCCEEDED
        assert prepared.adoption is None and prepared.abort_reason is None
        assert prepared.manifest.header.owner_worker_id == core.worker_id
        assert prepared.manifest.header.executor_worker_id == victim.worker_id
        node_incarnation = prepared.manifest.header.node_incarnation
        assert (node_incarnation.node_id, node_incarnation.node_pid, node_incarnation.registration_epoch) == (
            arrival.node_id, arrival.node_pid, arrival.registration_epoch,
        )
        assert len(((prepared.manifest.value,))) == 1
        slot = (prepared.manifest.value)
        assert (prepared.manifest.publication_id).object_id == outer_id and slot.tier is protocol.ResultStorage.INLINE
        assert 0 < slot.size_bytes <= _INLINE_THRESHOLD and len(slot.transfers) == 1

        (edge,) = slot.edges
        assert edge.container_object_id == outer_id
        assert edge.contained_object_id == source_id
        assert edge.contained_owner_worker_id == core.worker_id
        assert edge.contained_owner_address == runtime.owner_service.address
        source_snapshot = core.owner_table.snapshot(source_id)
        assert not source_snapshot.local_tokens
        assert source_snapshot.submitted_tokens and source_snapshot.lineage_tokens
        assert source_snapshot.contained_holds == frozenset({edge.incoming_hold(core.worker_id)})

        before = core.owner_table.snapshot(outer_id)
        assert before.state is ObjectState.PENDING
        assert before.inline_data is None and before.output_publication is None
        assert before.canonical_stored_result is None
        assert before.output_retirement_id is None
        assert not before.locations and not before.outgoing_contained_edges
        with observer_lock:
            assert not received_envelopes

        if keep_received:
            publication_gate.settimeout(_remaining(work_deadline))
            publication_gate.sendall(OUTPUT_PUBLICATION_GATE_RELEASE)
            publication_gate.close()
            assert before_commit.wait(_remaining(work_deadline))
            # The real received envelope is the argument to Core adoption.
            # The wrapper pauses its call before any owner CAS or new RPC.
            with core._state_lock:
                (received_envelope,) = received_envelope_holder
                assert type(received_envelope) is OutputPublicationEnvelope
                assert publication not in getattr(core, "_output_loss_choices", {})
                assert publication not in getattr(core, "_output_loss_completed", set())
                assert received_envelope.publication_id == publication
                assert received_envelope.manifest == prepared.manifest
                assert received_envelope.complete == prepared.complete
                assert len(((received_envelope.result,))) == 1
                assert (received_envelope.result).inline_data is not None
                assert core.owner_table.snapshot(outer_id).state is ObjectState.PENDING
            with observer_lock:
                assert received_envelopes
                assert all(value == received_envelope for value in received_envelopes
                           if value.publication_id == publication)
        else:
            # The Node gate has never received its release.  Complete and outcome
            # replies are held there, not dropped by a Driver test adapter.
            assert not before_commit.is_set()
            with core._state_lock:
                assert publication not in getattr(core, "_output_result_custody", {})

        death = _test_crash_node(victim.node_id, timeout=_remaining(work_deadline))
        assert death.node_id == arrival.node_id and death.node_pid == arrival.node_pid
        assert death.registration_epoch == arrival.registration_epoch
        assert death.exit_code == -signal.SIGKILL
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT
        publication_gate.close()
        allow_commit.set()
        assert old_finalized.wait(_remaining(work_deadline))

        with core._state_lock:
            settled = core.owner_table.snapshot(outer_id)
            succeeded = core._recovery.task_record(outer_id.task_id)
            assert succeeded.state is TaskState.SUCCEEDED
            assert succeeded.current_attempt == publication.attempt_id
            assert succeeded.retries_started == 0
            assert settled.current_attempt == publication.attempt_id
            assert outer_id.task_id not in core._protocol_unresolved
            assert outer_id not in core._task_finish_barriers
            assert publication not in core._output_result_custody
            assert publication in core._output_loss_completed
            decision = core._output_loss_choices[publication]
            assert decision is keep_received
        if keep_received:
            assert settled.state is ObjectState.READY_INLINE
            assert received_envelope is not None
            assert settled.inline_data == (received_envelope.result).inline_data
            assert settled.output_publication is not None
            assert settled.output_publication.publication_id == publication
            assert settled.output_publication.slot_index == 0
            assert settled.output_publication.manifest == prepared.manifest
            assert settled.outgoing_contained_edges == frozenset({edge})
        else:
            assert settled.state is ObjectState.LOST
            assert settled.inline_data is None and settled.output_publication is None
            assert not settled.outgoing_contained_edges and not settled.locations
            assert core._recovery.lineage_for_object(outer_id) is not None
        assert settled.canonical_stored_result is None
        assert settled.output_retirement_id is None
        source_snapshot = core.owner_table.snapshot(source_id)
        assert not source_snapshot.submitted_tokens
        assert source_snapshot.lineage_tokens
        assert source_snapshot.contained_holds == (
            frozenset({edge.incoming_hold(core.worker_id)}) if keep_received
            else frozenset()
        )

        with core._state_lock:
            resolution = core.owner_table._output_loss_receipts[publication]
        assert type(resolution) is NodeLostOutputResolution
        resolution.validate_manifest(prepared.manifest)
        assert resolution.publication_id == publication
        assert resolution.node_death == death and resolution.owner_worker_id == core.worker_id
        assert resolution.manifest_digest == arrival.manifest_digest
        assert resolution.complete == prepared.complete and resolution.keep is keep_received
        transfer = slot.transfers[0]
        assert isinstance(transfer.source, BorrowedContainedSource)
        assert transfer.source.borrower_worker_id == victim.worker_id
        assert isinstance(transfer.source.original_source, protocol.TaskHoldSource)
        original_hold = transfer.source.original_source.hold
        assert original_hold.origin_attempt_id == publication.attempt_id
        assert transfer.final_hold == edge.incoming_hold(core.worker_id)
        if keep_received:
            assert resolution.cleanup == ()
        else:
            assert all(type(reply) is protocol.ReleaseContainedReferenceReply and reply.accepted
                       for reply in resolution.cleanup)
            assert {(reply.object_id, reply.owner_worker_id, reply.hold) for reply in resolution.cleanup} == {
                (source_id, core.worker_id, transfer.final_hold),
                (source_id, core.worker_id, transfer.provisional_hold),
            }
        terminal = _gcs_query(runtime.owner_service.address, wire.GET_OUTPUT_HANDOFF_HANDLER,
                              recovery_query, work_deadline)
        assert type(terminal) is wire.OutputHandoffReply and terminal.accepted
        assert terminal.request == recovery_query and terminal.snapshot.manifest == prepared.manifest
        assert terminal.snapshot.complete == prepared.complete
        assert terminal.snapshot.phase is (OutputHandoffPhase.ADOPTED if keep_received else OutputHandoffPhase.ABORTED)
        assert (terminal.snapshot.adoption is not None) is keep_received
        assert _gcs_query(runtime.owner_service.address, wire.GET_OUTPUT_HANDOFF_HANDLER,
                          recovery_query, work_deadline) == terminal

        with observer_lock:
            assert outer_id.task_id not in generic_retries
            leases_before_get = tuple(
                request for request, _reply in observed_leases
                if request.task_id == outer_id.task_id
            )
            assert leases_before_get
            assert {request.attempt_id.attempt_number for request in leases_before_get} == {0}
            if not keep_received:
                assert not received_envelopes
        # Public get on the unrelated blocker only frees survivor capacity;
        # neither it nor the metadata queries requests outer reconstruction.
        blocker_gate.settimeout(_remaining(work_deadline))
        blocker_gate.sendall(_BLOCKER_RELEASE)
        assert ray.get(blocker, timeout=_remaining(work_deadline)) == survivor.worker_pid
        blocker_gate.close()
        assert core._recovery.task_record(outer_id.task_id).retries_started == 0

        first, marker, producing_pid = ray.get(outer, timeout=_remaining(work_deadline))
        expected_pid = victim.worker_pid if keep_received else survivor.worker_pid
        assert marker == _RESULT_MARKER and producing_pid == expected_pid
        assert isinstance(first, ray.ObjectRef) and first.object_id == source_id
        assert first.owner_worker_id == core.worker_id
        assert ray.get(first, timeout=_remaining(work_deadline)) == _CHILD_VALUE
        second, second_marker, second_pid = ray.get(outer, timeout=_remaining(work_deadline))
        assert isinstance(second, ray.ObjectRef) and second is not first
        assert second.object_id == source_id
        assert (second_marker, second_pid) == (marker, producing_pid)
        if not keep_received:
            assert rebuilt_finalized.wait(_remaining(work_deadline))

        after = core.owner_table.snapshot(outer_id)
        final_task = core._recovery.task_record(outer_id.task_id)
        expected_attempt = 0 if keep_received else 1
        assert outer.object_id == outer_id
        assert after.state is ObjectState.READY_INLINE
        assert after.current_attempt.attempt_number == expected_attempt
        assert final_task.state is TaskState.SUCCEEDED
        assert final_task.current_attempt == after.current_attempt
        assert final_task.retries_started == expected_attempt
        assert after.output_publication is not None
        assert after.output_publication.slot_index == 0
        assert after.canonical_stored_result is None and not after.locations
        assert after.output_retirement_id is None
        final_publication = after.output_publication.publication_id
        assert type(final_publication) is OutputPublicationID
        assert ((final_publication.object_id,)) == ((final_publication.object_id,)) == (outer_id,)
        if keep_received:
            assert final_publication == publication
            assert after.inline_data == (received_envelope.result).inline_data
            assert hashlib.sha256(after.inline_data).hexdigest() == slot.checksum
        else:
            assert final_publication != publication
            assert final_publication.attempt_id == publication.attempt_id.next()
            with observer_lock:
                rebuilt_envelopes = tuple(
                    value for value in received_envelopes
                    if value.publication_id == final_publication
                )
                assert not any(value.publication_id == publication for value in received_envelopes)
            assert rebuilt_envelopes
            replacement_envelope = rebuilt_envelopes[0]
            assert replacement_envelope.manifest == after.output_publication.manifest
            assert (replacement_envelope.result).inline_data == after.inline_data
            replacement = (replacement_envelope.manifest.value).transfers[0]
            assert isinstance(replacement.source, BorrowedContainedSource)
            assert replacement.source.borrower_worker_id == survivor.worker_id
            assert replacement.source.original_source.hold != original_hold
            assert replacement.source.original_source.hold.origin_attempt_id == after.current_attempt

        with observer_lock:
            grants = {}
            for request, reply in observed_leases:
                if request.task_id == outer_id.task_id and isinstance(reply, protocol.GrantWorkerLease):
                    pair = request, reply
                    assert grants.setdefault(request.attempt_id.attempt_number, pair) == pair
            assert outer_id.task_id not in generic_retries
            pushes = tuple(value for value in observed_pushes if value.spec.task_id == outer_id.task_id)
        assert set(grants) == ({0} if keep_received else {0, 1})
        assert grants[0][1].node_id == victim.node_id
        assert grants[0][0].lease_id == publication.lease_id
        if not keep_received:
            assert grants[1][1].node_id == survivor.node_id
            assert grants[1][0].lease_id != publication.lease_id
        for request, _reply in grants.values():
            assert request.return_ids == (outer_id,)
        assert {push.spec.attempt_id.attempt_number for push in pushes} == set(grants)
        for push in pushes:
            assert push.spec.max_retries == 1
            assert len(push.spec.args) == 1
            argument = push.spec.args[0]
            assert isinstance(argument, protocol.InlineArg) and len(argument.nested_refs) == 1
            assert argument.nested_refs[0].object_id == source_id
            assert argument.nested_refs[0].hold.origin_attempt_id == push.spec.attempt_id
            assert not push.dependencies  # nested handle is not a readiness dependency

        # Independent local handles must outlive both the original source
        # handle and the container.  Final close drains child holds and lineage.
        _close_with_deadline(outer, work_deadline)
        assert ray.get(first, timeout=_remaining(work_deadline)) == _CHILD_VALUE
        _close_with_deadline(first, work_deadline)
        assert ray.get(second, timeout=_remaining(work_deadline)) == _CHILD_VALUE
        _close_with_deadline(second, work_deadline)
        _close_with_deadline(blocker, work_deadline)
        assert all_collected.wait(_remaining(work_deadline))
        for object_id in (outer_id, source_id, blocker.object_id):
            assert core.owner_table.collection_state(object_id) is ObjectCollectionState.COLLECTED
            assert not core.owner_table.contains(object_id)
            assert object_id not in core._objects
            assert object_id not in core._stored_descriptors
            assert object_id not in core._object_gc_obligations
            assert core._recovery.lineage_for_object(object_id) is None
        assert core.owner_table._output_loss_receipts[publication] == resolution
        final_handoff = core._output_handoff_table().query(final_publication)
        assert final_handoff.adoption is not None and final_handoff.complete is not None
    finally:
        # Release all test gates even if a semantic assertion fails.  Restore
        # observers before real shutdown drains and joins its own threads.
        allow_commit.set()
        for connection, release in connections:
            try:
                connection.settimeout(0.1)
                connection.sendall(release)
            except OSError:
                pass
            connection.close()
        listener.close()
        monkeypatch.undo()
        for reference in (second, first, outer, source, blocker):
            _enqueue_close(reference)
        report = ray.shutdown()

    _assert_shutdown(context, report, death, managed_pids, managed_addresses)
