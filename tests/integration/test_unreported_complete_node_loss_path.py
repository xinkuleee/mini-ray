"""F3: local Complete exists, but its terminal and delivery never escape.

This is one bounded scenario with TWO controlled events: a publication-channel
partition, then one managed publisher Node crash. It is not a generic one-fault
claim. Five startup children, two 1 MiB stores, one live Driver-owned tiny child,
one 8 KiB borrowed-child result with one SYSTEM retry, and one survivor blocker.
One listener/two connections; no extra test thread, Actor, PG, or tracing.

Only the victim's test-module spawn wrapper changes delivery. Terminal reports
raise TransportTimeout BEFORE the real transport call, never return fake ACKs.
The common Complete/outcome checkpoint first verifies the actual journal
Complete, completed lease, and released CPU. Its metadata-only frame is sent
only after a matching real terminal-send attempt was suppressed. The ordinary
configured output gate is deliberately absent: its AFTER_COMPLETE phase forces
an owner terminal ACK and would test the wrong window.

The test-local frame prefix means LOCAL_COMPLETE_UNREPORTED; the existing
arrival encoding supplies exact incarnation/publication/digest only. Neither
the observer's witness nor any envelope is installed into Core custody. The
owner must still have registration only when the Node dies, so recovery remains UNKNOWN,
cleans old live-child holds, and then retries. Test and gate work each share a
ten-second deadline; gate/refs cleanup gets three seconds. Run only this exact
ID through the 30-second process-tree runner, plus its bounded cleanup grace.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from functools import partial
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
from miniray.api import _get_runtime, _test_crash_node
from miniray.core import _worker_death_reference_id
from miniray.control import GET_WORKER_STATE_HANDLER
from miniray.ids import AttemptID
from miniray.output_publication import OutputPublicationCompleteWitness, OutputPublicationEnvelope
from miniray.output_publication_journal import OutputPublicationJournalState
from miniray.output_handoff import NodeLostOutputResolution, OutputHandoffPhase
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.publication_gate import OutputPublicationGateArrival, OutputPublicationGatePhase, recv_output_publication_gate_arrival
from miniray.publication_sources import BorrowedContainedSource
from miniray.recovery import TaskState
from miniray.resources import AllocationState
from miniray.transport import TransportTimeout
from tests.support._legacy_reference_cleanup import _close_local
from tests.integration.test_stored_outer_node_loss_path import (
    _BLOCKER_RELEASE, _SURVIVOR_RESOURCE, _assert_metadata_only, _handoff, _loss_receipt,
    _occupy_survivor, _pid_exists, _poll_until, _query,
    _recv_exact, _release_connection, _remaining,
)


pytestmark = pytest.mark.multiprocess_smoke
_ACTUAL_NODE_PROCESS_MAIN = api_module._node_process_main
_SECONDS = 10.0
_FRAME_PREFIX = b"MRLCU001"  # LOCAL_COMPLETE_UNREPORTED, not an owner terminal proof.
_GATE_RELEASE = b"G"
_SOURCE_VALUE = ("unreported-complete-live-child", 42)
_PADDING = b"C" * (8 * 1024)


@dataclass(frozen=True)
class _RetryCut:
    resolution: object
    current_attempt: AttemptID
    retries_started: int
    task_state: TaskState
    outer: object
    source: object
    dependency_hold: protocol.TaskReferenceHold
    release_seen: tuple[bool, bool]
    executor_death: object


def _wait(core, predicate, deadline):
    with core._completion:
        while True:
            result = predicate()
            if result:
                return result
            core._completion.wait(_remaining(deadline))


def _physical(node, object_id, deadline):
    reply = _query(node.node_address, node_module.GET_OBJECT_HANDLER,
                   protocol.GetObject(object_id, node.node_id), deadline)
    assert type(reply) is protocol.GetObjectReply and reply.object_id == object_id and reply.node_id == node.node_id
    return reply


class _UnreportedCompleteGate:
    """One exact witness, one leader connection, one deadline for followers."""

    def __init__(self, address):
        self.address = address
        self.lock = threading.Lock()
        self.terminal_seen = threading.Event()
        self.done = threading.Event()
        self.witness = self.deadline = self.error = None
        self.started = self.released = False
        self.suppressed = 0

    def _select_locked(self, witness):
        witness = replace(witness)
        if self.witness is None:
            self.witness = witness
            self.deadline = time.monotonic() + _SECONDS
        elif self.witness != witness:
            raise AssertionError("the one publication partition observed another Complete")

    def suppress_terminal(self, witness):
        with self.lock:
            self._select_locked(witness)
            if self.released or time.monotonic() >= self.deadline:
                return False  # A failed test must not leave shutdown partitioned.
            self.suppressed += 1
            self.terminal_seen.set()
            return True

    def checkpoint(self, node, reply):
        envelope = getattr(reply, "output_publication", None)
        witness = getattr(reply, "output_completion", None)
        if envelope is None and witness is None:
            return
        if envelope is not None:
            assert type(envelope) is OutputPublicationEnvelope
            witness = envelope.complete
        assert type(witness) is OutputPublicationCompleteWitness
        witness = replace(witness)
        journal = node._output_publication_journal

        def validate_local_complete():
            with journal.linearize(witness.publication_id), node._state_lock:
                snapshot = journal.snapshot(witness.publication_id)
                manifest = snapshot.manifest
                record = node._output_lease_record_locked(manifest)
                assert getattr(node, "_output_publication_gate", None) is None
                assert snapshot.complete == witness and snapshot.state is OutputPublicationJournalState.COMPLETED
                assert snapshot.rollback is None and snapshot.rollback_tombstone is None
                assert record.state is protocol.LeaseExecutionState.COMPLETED
                assert record.completion is not None and record.completion.status is protocol.TaskReplyStatus.SUCCEEDED
                assert record.output_complete_inflight is None
                allocation = node._ledger.record(record.allocation_token)
                assert allocation is not None and allocation.state is AllocationState.RELEASED
                assert node._ledger.available == node._ledger.total and node._ledger.cpu_debt == 0
                assert node._workers[manifest.header.executor_worker_id].active_lease_id is None
                assert envelope is not None and envelope.manifest == manifest
                assert tuple(journal.materialized_result(witness.publication_id, index)
                             for index in range(len(manifest.slots))) == envelope.results
                return OutputPublicationGateArrival.from_manifest(
                    manifest, OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY,
                )

        arrival = validate_local_complete()
        with self.lock:
            self._select_locked(witness)
            leader = not self.started
            self.started = True
            deadline = self.deadline
        if not leader:
            if not self.done.wait(_remaining(deadline)):
                raise TimeoutError("unreported Complete leader exceeded its shared deadline")
            if self.error is not None:
                raise RuntimeError(self.error)
            return
        try:
            # No Node/journal/resource/observation lock is held during waits.
            assert self.terminal_seen.wait(_remaining(deadline)), "no actual terminal-send attempt was suppressed"
            with self.lock:
                assert self.suppressed >= 1 and self.witness == witness and not self.released
            assert witness in node._output_publications.pending_terminal_reports()
            assert validate_local_complete() == arrival
            with socket.create_connection(self.address, timeout=_remaining(deadline)) as connection:
                connection.settimeout(_remaining(deadline))
                connection.sendall(_FRAME_PREFIX + arrival.to_bytes())
                if _recv_exact(connection, 1, deadline) != _GATE_RELEASE:
                    raise RuntimeError("unreported Complete gate closed without release")
        except BaseException as exc:
            self.error = str(exc) or type(exc).__name__
            raise
        finally:
            with self.lock:
                self.released = True
            self.done.set()


def _node_process_with_unreported_complete(address, *args):
    # The existing resource topology identifies the survivor. Only the other
    # Node is partitioned; its normal startup/setsid/Worker path stays intact.
    if args[1].get(_SURVIVOR_RESOURCE, 0):
        return _ACTUAL_NODE_PROCESS_MAIN(*args)
    node_type = node_module.NodeServer
    original_rpc = node_type._background_rpc
    original_checkpoint = node_type._test_output_result_delivery_checkpoint
    gate = _UnreportedCompleteGate(address)

    def send(node, destination, handler, message, **options):
        if (handler == wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER
                and type(message) is wire.ReportOutputHandoffComplete
                and gate.suppress_terminal(message.witness)):
            # Do not call original_rpc and then lose the ACK: that would make
            # Complete KNOWN at the owner and invalidate this acceptance.
            raise TransportTimeout("test partition withheld the actual terminal report before send")
        return original_rpc(node, destination, handler, message, **options)

    def checkpoint(node, reply):
        original_checkpoint(node, reply)  # Ordinary unconfigured hook is inert.
        gate.checkpoint(node, reply)  # Both Complete and outcome use this seam.

    node_type._background_rpc = send
    node_type._test_output_result_delivery_checkpoint = checkpoint
    try:
        _ACTUAL_NODE_PROCESS_MAIN(*args)
    finally:
        node_type._background_rpc = original_rpc
        node_type._test_output_result_delivery_checkpoint = original_checkpoint


@ray.remote(num_cpus=1, max_retries=1)
def _return_unreported_borrowed_child(container):
    child, = container
    assert isinstance(child, ray.ObjectRef) and child.borrower_token is not None
    return {"child": child, "padding": _PADDING, "executor_pid": os.getpid()}


def test_locally_completed_unreported_output_crash_is_unknown_then_cleans_before_retry():
    listener = blocker_connection = publication_connection = None
    context = core = report = death = source = blocker = outer = restored = None
    transfer = publication = original_rpc = original_retry = None
    original_entry = api_module._node_process_main
    pids, addresses, close_errors = set(), set(), []
    observations = threading.Lock()
    grants, cuts = {}, {}
    observation_errors = set()
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(2)
        address = listener.getsockname()
        addresses.add(address)
        api_module._node_process_main = partial(_node_process_with_unreported_complete, address)
        context = ray.init(
            num_nodes=2, num_workers_per_node=1,
            node_resources=({"CPU": 1, _SURVIVOR_RESOURCE: 1}, {"CPU": 1}),
            inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False,
        )
        deadline = time.monotonic() + _SECONDS
        runtime = _get_runtime()
        core = runtime.core_worker
        survivor, victim = context.nodes
        assert core.node_id == survivor.node_id and context.trace_address is None
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
        original_rpc, original_retry = core._rpc, core._retry_system_failure

        def inspect_rpc(destination, handler, message):
            reply = original_rpc(destination, handler, message)
            with observations:
                if handler == node_module.REQUEST_LEASE_HANDLER and type(reply) is protocol.GrantWorkerLease:
                    key = message.task_id, message.attempt_id
                    if key not in grants and len(grants) >= 4:
                        observation_errors.add("grant inventory exceeded four identities")
                    elif grants.setdefault(key, (message, reply)) != (message, reply):
                        observation_errors.add("one grant identity was rebound")
            return reply

        def inspect_retry(pending, error, **kwargs):
            if publication is not None and pending.task_id == publication.task_id:
                with core._completion:
                    record = core._recovery.task_record(pending.task_id)
                    owner = core.owner_table.snapshot(pending.output_ids[0])
                    child = core.owner_table.snapshot(source_id)
                    released = tuple(core.owner_table.contained_release_was_seen(source_id, hold)
                                     for hold in (transfer.final_hold, transfer.provisional_hold))
                    installed = core.owner_table.dead_worker_record(victim.worker_id)
                    with observations:
                        if pending.spec.attempt_id not in cuts and len(cuts) >= 1:
                            observation_errors.add("more than one old attempt requested retry")
                        else:
                            cuts.setdefault(pending.spec.attempt_id, _RetryCut(
                                _loss_receipt(core, publication), record.current_attempt, record.retries_started,
                                record.state, owner, child, pending.dependency_hold, released, installed,
                            ))
            return original_retry(pending, error, **kwargs)

        core._rpc, core._retry_system_failure = inspect_rpc, inspect_retry
        blocker = _occupy_survivor.remote(address, deadline)
        listener.settimeout(_remaining(deadline))
        blocker_connection, _ = listener.accept()
        assert int.from_bytes(_recv_exact(blocker_connection, 8, deadline), "big") == survivor.worker_pid
        outer = _return_unreported_borrowed_child.remote([source])
        object_id = outer.object_id
        listener.settimeout(_remaining(deadline))
        publication_connection, _ = listener.accept()
        assert _recv_exact(publication_connection, len(_FRAME_PREFIX), deadline) == _FRAME_PREFIX
        publication_connection.settimeout(_remaining(deadline))
        arrival = recv_output_publication_gate_arrival(publication_connection)
        assert arrival.phase is OutputPublicationGatePhase.AFTER_COMPLETE_BEFORE_TASK_REPLY
        publication = arrival.publication_id
        assert (arrival.node_id, arrival.node_pid, arrival.registration_epoch) == (
            victim.node_id, victim.node_pid, runtime.nodes[1].registration_epoch,
        )
        assert publication.output_ids == publication.full_output_ids == (object_id,)
        assert publication.attempt_id == AttemptID(object_id.task_id, 0)
        _assert_metadata_only(arrival)
        # Arrival proves LOCAL Complete and CPU release, not owner knowledge.
        # This independent query is the defining F3 distinction from the
        # configured AFTER_COMPLETE gate and from the pre-Complete ARM gate.
        before = _handoff(runtime.owner_service.address, publication, deadline)
        manifest = before.manifest
        assert manifest.manifest_digest == arrival.manifest_digest
        assert before.phase is OutputHandoffPhase.PENDING
        assert before.complete is before.adoption is before.abort_reason is None
        assert manifest.header.owner_worker_id == core.worker_id and manifest.header.executor_worker_id == victim.worker_id
        (slot,) = manifest.slots
        assert slot.tier is protocol.ResultStorage.OBJECT_STORE and len(_PADDING) < slot.size_bytes < 32 * 1024
        (transfer,) = slot.transfers
        assert type(transfer.source) is BorrowedContainedSource and transfer.source.borrower_worker_id == victim.worker_id
        assert transfer.contained_object_id == source_id and transfer.contained_owner_worker_id == core.worker_id
        assert transfer.contained_owner_address == runtime.owner_service.address
        assert type(transfer.source.original_source) is protocol.TaskHoldSource
        hold = transfer.source.original_source.hold
        assert hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED and hold.origin_attempt_id == publication.attempt_id
        at_complete = core.owner_table.snapshot(source_id)
        assert at_complete.contained_holds == frozenset((transfer.final_hold,))
        assert at_complete.submitted_tokens == frozenset((hold,)) and at_complete.lineage_tokens
        assert at_complete.inline_data == initial_source.inline_data and at_complete.local_tokens == initial_source.local_tokens
        assert core.owner_table.contained_release_was_seen(source_id, transfer.provisional_hold)
        assert not core.owner_table.contained_release_was_seen(source_id, transfer.final_hold)
        # Worker drains its import/source borrow BEFORE requesting Complete.
        # Do not claim this later window still has the ARM gate's active borrow.
        physical = _physical(victim, object_id, deadline)
        assert physical.found and physical.sealed and physical.producer_attempt_id == publication.attempt_id
        assert physical.owner_worker_id == core.worker_id and physical.size_bytes == slot.size_bytes
        assert physical.checksum == slot.checksum == hashlib.sha256(physical.data).hexdigest()
        pending = core.owner_table.snapshot(object_id)
        assert pending.state is ObjectState.PENDING and pending.output_publication is None and not pending.locations
        assert pending.current_attempt == publication.attempt_id
        assert publication not in getattr(core, "_output_result_custody", {})
        assert core._recovery.task_record(object_id.task_id).retries_started == 0
        # Close the delivery connection only AFTER authoritative crash.
        # Releasing it first could disclose Complete through the normal path.
        death = _test_crash_node(victim.node_id, timeout=_remaining(deadline))
        assert (death.node_id, death.node_pid, death.registration_epoch) == (arrival.node_id, arrival.node_pid, arrival.registration_epoch)
        assert death.reason is protocol.NodeDeathReason.PROCESS_EXIT and death.exit_code == -signal.SIGKILL
        publication_connection.close()
        publication_connection = None

        def retry_observed():
            with observations:
                return cuts.get(publication.attempt_id)

        cut = _poll_until(retry_observed, deadline, "UNKNOWN cleanup never reached the system-retry boundary")
        assert cut.current_attempt == publication.attempt_id and cut.retries_started == 0
        assert cut.outer.state is ObjectState.PENDING and cut.outer.output_publication is None
        assert cut.outer.current_attempt == publication.attempt_id and not cut.outer.locations
        assert type(cut.resolution) is NodeLostOutputResolution and cut.resolution.complete is None and not cut.resolution.keep
        assert cut.resolution.node_death == death and cut.resolution.publication_id == publication
        assert cut.resolution.manifest_digest == manifest.manifest_digest and cut.resolution.owner_worker_id == core.worker_id
        assert cut.release_seen == (True, True) and cut.dependency_hold == hold
        assert cut.source.submitted_tokens == at_complete.submitted_tokens and cut.source.lineage_tokens == at_complete.lineage_tokens
        assert not cut.source.contained_holds and cut.source.inline_data == initial_source.inline_data
        cut.resolution.validate_manifest(manifest)
        assert all(type(reply) is protocol.ReleaseContainedReferenceReply and reply.accepted
                   for reply in cut.resolution.cleanup)
        assert {(reply.object_id, reply.owner_worker_id, reply.hold) for reply in cut.resolution.cleanup} == {
            (source_id, core.worker_id, transfer.final_hold),
            (source_id, core.worker_id, transfer.provisional_hold),
        }
        assert _loss_receipt(core, publication) == cut.resolution
        resolved = _handoff(runtime.owner_service.address, publication, deadline)
        assert resolved.manifest == manifest and resolved.complete is resolved.adoption is None
        assert resolved.phase is OutputHandoffPhase.ABORTED and resolved.abort_reason is not None
        _wait(core, lambda: core._recovery.task_record(object_id.task_id).current_attempt == publication.attempt_id.next(), deadline)
        record = replace(core._recovery.task_record(object_id.task_id))
        assert record.state is TaskState.RETRY_PENDING and record.retries_started == 1

        old_borrow = transfer.source.owner_table_token
        _wait(core, lambda: core.owner_table.dead_worker_record(victim.worker_id) is not None
              and old_borrow not in core.owner_table.snapshot(source_id).borrowed_tokens, deadline)
        worker = _query(context.gcs_address, GET_WORKER_STATE_HANDLER, protocol.GetWorkerState(victim.worker_id), deadline)
        assert type(worker) is protocol.GetWorkerStateReply and worker.found and worker.worker_id == victim.worker_id
        assert worker.state is protocol.WorkerMembershipState.DEAD and worker.death.reason is protocol.WorkerDeathReason.NODE_EXIT
        assert (worker.death.node_id, worker.death.node_pid, worker.death.node_registration_epoch, worker.death.worker_pid) == (
            victim.node_id, victim.node_pid, arrival.registration_epoch, victim.worker_pid,
        )
        assert core.owner_table.dead_worker_record(victim.worker_id).death_id == _worker_death_reference_id(worker.death)
        assert ray.get(source, timeout=_remaining(deadline)) == _SOURCE_VALUE
        with observations:
            assert {attempt for task, attempt in grants if task == object_id.task_id} == {publication.attempt_id}
            assert len(cuts) == 1 and not observation_errors
        blocker_connection.settimeout(_remaining(deadline))
        blocker_connection.sendall(_BLOCKER_RELEASE)
        assert ray.get(blocker, timeout=_remaining(deadline)) == survivor.worker_pid
        blocker_connection.close()
        blocker_connection = None
        value = ray.get(outer, timeout=_remaining(deadline))
        restored = value["child"]
        assert value["padding"] == _PADDING and value["executor_pid"] == survivor.worker_pid
        assert isinstance(restored, ray.ObjectRef) and restored.object_id == source_id and restored.owner_worker_id == core.worker_id
        assert restored.borrower_token is None and ray.get(restored, timeout=_remaining(deadline)) == _SOURCE_VALUE
        _wait(core, lambda: object_id not in core._task_finish_barriers, deadline)
        completed = core.owner_table.snapshot(object_id)
        member = completed.output_publication
        assert completed.state is ObjectState.READY_STORED and completed.current_attempt == publication.attempt_id.next()
        assert member is not None and member.publication_id != publication
        assert member.manifest.header.executor_worker_id == survivor.worker_id
        (successor,) = member.slot.transfers
        assert successor.contained_object_id == source_id and successor.contained_owner_worker_id == core.worker_id
        assert successor.final_hold != transfer.final_hold and successor.provisional_hold != transfer.provisional_hold
        assert core.owner_table.snapshot(source_id).contained_holds == frozenset((successor.final_hold,))
        record = replace(core._recovery.task_record(object_id.task_id))
        assert record.state is TaskState.SUCCEEDED and record.retries_started == 1 and record.retries_remaining == 0
        assert core._recovery.active_recovery(object_id.task_id) is None
        assert _handoff(runtime.owner_service.address, publication, deadline) == resolved
        assert _loss_receipt(core, publication) == cut.resolution
        with observations:
            assert {attempt for task, attempt in grants if task == object_id.task_id} == {publication.attempt_id, publication.attempt_id.next()}
            assert len(cuts) == 1 and not observation_errors

        for reference in (restored, outer, blocker):
            _close_local(reference, min(deadline, time.monotonic() + 3.0))
        _wait(core, lambda: all(core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
                               for ref in (outer, blocker)), deadline)
        assert not _physical(survivor, object_id, deadline).found
        assert core.owner_table.contained_release_was_seen(source_id, successor.final_hold)
        assert core.owner_table.contained_release_was_seen(source_id, successor.provisional_hold)
        assert core.owner_table.contained_release_was_seen(source_id, transfer.final_hold)
        assert ray.get(source, timeout=_remaining(deadline)) == _SOURCE_VALUE
        _close_local(source, min(deadline, time.monotonic() + 3.0))
        _wait(core, lambda: core.owner_table.collection_state(source_id) is ObjectCollectionState.COLLECTED, deadline)
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations

        def survivor_clean():
            status = _query(survivor.node_address, node_module.SHUTDOWN_STATUS_HANDLER,
                            protocol.ShutdownStatusRequest("inspect-unreported-complete-cleanup"), deadline)
            assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested
            return status if status.resources_clean else None

        assert _poll_until(survivor_clean, deadline, "survivor cleanup did not settle").child_pids == (survivor.worker_pid,)
    finally:
        cleanup_deadline = time.monotonic() + 3.0
        api_module._node_process_main = original_entry
        try:
            try:
                _release_connection(publication_connection, _GATE_RELEASE, cleanup_deadline)
            finally:
                try:
                    _release_connection(blocker_connection, _BLOCKER_RELEASE, cleanup_deadline)
                finally:
                    if listener is not None:
                        listener.close()
        finally:
            try:
                for reference in (restored, outer, blocker, source):
                    try:
                        _close_local(reference, cleanup_deadline)
                    except Exception as exc:
                        close_errors.append(exc)
            finally:
                if core is not None and original_rpc is not None:
                    core._rpc, core._retry_system_failure = original_rpc, original_retry
                report = ray.shutdown()
    assert not close_errors and context is not None and death is not None and report is not None
    assert not ray.is_initialized() and report.core_stopped and report.gcs_clean and report.gcs_exitcode == 0
    assert not report.gcs_forced and not report.forced
    assert report.node_exitcodes == (0, death.exit_code) and report.node_cleans == (True, False)
    assert report.node_forced == (False, False) and report.node_finalized == (True, False)
    assert report.worker_exitcodes == (0, None) and report.worker_cleans == (True, False)
    assert report.worker_forced == (False, False) and report.node_deaths[1] == death
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert not report.node_clean and not report.worker_clean and not report.resources_clean
    assert not report.finalized and not report.shutdown_ack_clean
    _poll_until(lambda: all(not _pid_exists(pid) for pid in pids), time.monotonic() + 2.0, "managed process survived F3 shutdown")
    assert all(process.pid not in pids for process in mp.active_children())
    for address in addresses:
        with pytest.raises(OSError):
            with socket.create_connection(address, timeout=0.1):
                pass
