"""Pure logical-finish barriers across ordinary task success and replay.

Real Core methods, owner/recovery tables, and FIFO admission are composed
synchronously.  No Core constructor, mailbox, thread, process, socket, or
wall-clock wait is started.  Interleaving hooks model the finalizer races.
Successful results use real OutputDiscovery, a Node publication adapter and
journal, metadata-only recovery, and real Node storage/drop handlers. Per case:
at most three logical tasks, six publications, two selected slots, 128 bytes
per serialized slot and one 4 KiB in-memory store. No output contains refs.
"""

from __future__ import annotations

import queue
import socket
import subprocess
import threading
import time
from dataclasses import replace
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import core as core_module, output_protocol as wire, protocol
from miniray.core import (
    CoreWorker, ObjectRef, RemoteFunctionDefinition, _ForeignDependencyGuard,
    _PendingTask,
)
from miniray.errors import SystemTaskError
from miniray.foreign_lineage_runtime import (
    ForeignLineageRenewalDisposition, ForeignLineageRenewalResult,
)
from miniray.ids import JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import (
    OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation,
)
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.output_recovery import OutputPublicationRecoveryAuthority
from miniray.ownership import ObjectCollectionState, ObjectOwnerTable, ObjectState
from miniray.reconstruction_runtime import ReconstructionDisposition
from miniray.recovery import RecoveryManager, TaskState
from miniray.resources import AllocationToken, ResourceLedger, ResourceVector
from miniray.trace import MemoryEventSink
from miniray.transport import TransportTimeout
from tests.unit.test_output_publication import _assert_metadata


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("finish-barrier test attempted actual runtime work")

    for owner, method in (
        (CoreWorker, "__init__"), (NodeServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (socket, "socket"),
        (socket, "create_connection"), (subprocess, "Popen"), (time, "sleep"),
    ):
        monkeypatch.setattr(owner, method, forbidden)


class _Composition:
    """Reentrant synchronous composition/condition with no blocking wait."""

    def __init__(self) -> None:
        self.depth = 0
        self.notifications = 0
        self.waits = 0
        self.before_enter = None

    def __enter__(self):
        callback = self.before_enter
        self.before_enter = None
        if callback is not None:
            callback()
        self.depth += 1
        return self

    def __exit__(self, *_exc):
        self.depth -= 1
        assert self.depth >= 0

    def notify_all(self) -> None:
        assert self.depth > 0
        self.notifications += 1

    def wait(self, _timeout=None) -> None:
        self.waits += 1
        pytest.fail("the deterministic finish interleave must not wait")


class _OutputBackend:
    """Only bounded Node storage and metadata GCS effects, no transport."""

    def __init__(self, core, no_rpc):
        self.core, self.no_rpc = core, no_rpc
        self.journal = OutputPublicationJournal()
        self.recovery = OutputPublicationRecoveryAuthority()
        self.store = ObjectStore(4096)
        self.ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        self.completed = {}
        self.calls = []
        self.executor = WorkerID(bytes.fromhex("ef" * 16))
        node = object.__new__(NodeServer)
        self.node = node
        node.node_id, node._node_pid, node._registration_epoch = core.node_id, 3001, 1
        node._state_lock = threading.RLock()
        node._object_store = self.store
        node._object_manager = ObjectManager(node.node_id, self.store)
        node._sealed_metadata = {}
        node._dropped_metadata = {}
        node._local_replica_write_claims = {}
        node._object_localization_locks = {}
        node._owner_death_fences = {}
        node._output_publication_journal = self.journal
        self.adapter = OutputPublicationNodeAdapter(
            self.journal, report_intent=self.recovery.report_intent,
            arm_complete=self.recovery.arm_complete,
            report_terminal=self.recovery.report_terminal,
            report_rollback=self.recovery.report_rollback,
            prepare_child=no_rpc, promote_child=no_rpc, release_child=no_rpc,
            prepare_graph=no_rpc, abort_graph=no_rpc,
            seal_replica=node._seal_output_publication_replica,
            drop_replica=node._drop_output_publication_replica,
        )

    def address(self, node_id):
        if node_id != self.core.node_id:
            self.no_rpc("unexpected Node route", node_id)
        return self.core.node_address

    def rpc(self, address, handler, request):
        self.calls.append((handler, request))
        assert len(self.calls) <= 32
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == self.core.gcs_address
            if type(request) is wire.ReportOutputPublicationTerminal:
                ack = self.recovery.report_terminal(request.witness)
            elif type(request) is wire.ReportOutputPublicationAdopted:
                assert self.completed[request.proof.complete.publication_id] == request.proof.complete
                ack = self.recovery.report_adopted(request.proof)
            elif type(request) is wire.ReportOutputPublicationSlotCollected:
                identity = request.proof.complete.publication_id
                snapshot = self.recovery.snapshot(identity)
                slot = snapshot.manifest.slots[request.proof.slot_index]
                assert slot.object_id == request.proof.object_id and not slot.edges
                if slot.tier is protocol.ResultStorage.OBJECT_STORE:
                    assert not self.store.contains(slot.object_id, sealed_only=False)
                    assert self.node._dropped_metadata[slot.object_id] == (
                        identity.attempt_id, self.core.worker_id, slot.checksum,
                    )
                ack = self.recovery.report_slot_collected(request.proof)
            else:
                return self.no_rpc(handler, request)
            assert ack.snapshot.manifest.to_graph_manifest() is None
            _assert_metadata(ack.snapshot)
            return wire.OutputRecoveryReply(request, ack)
        if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
            assert address == self.core.node_address
            assert type(request) is wire.AckOutputPublicationAdopted
            identity = request.proof.complete.publication_id
            assert self.completed[identity] == request.proof.complete
            assert self.recovery.snapshot(identity).adopted == request.proof
            self.journal.retire_completed(request.proof)
            return wire.AckOutputPublicationAdoptedReply(request, True)
        if handler == "drop_object_replica":
            assert address == self.core.node_address
            return self.node._handle_drop_object_replica(request)
        return self.no_rpc(handler, request)

    def succeed(self, pending, *, stored, value):
        assert len(self.completed) < 6 and len(pending.output_ids) <= 2
        identity = OutputPublicationID(
            LeaseID((len(self.completed) + 1).to_bytes(16, "big")), pending.execution,
        )
        header = OutputPublicationHeader(
            identity, self.core.job_id, self.executor, self.core.worker_id,
            OutputPublicationNodeIncarnation(self.core.node_id, 3001, 1),
        )
        discovery = OutputDiscoverySession(header, inline_threshold=0 if stored else 128)
        outputs = discovery.discover((value,) * len(pending.output_ids))
        assert all(len(payload) <= 128 for payload in outputs.slot_payloads)
        assert outputs.manifest.to_graph_manifest() is None
        _assert_metadata(outputs.manifest)
        assert discovery.source_references == ()
        token = AllocationToken("finish-lease-{}".format(len(self.completed)))
        self.ledger.allocate(ResourceVector({"CPU": 1}), token)
        self.adapter.prepare(outputs.manifest, outputs.slot_payloads)
        discovery.release_sources_after_promotions()

        def complete_lease(witness):
            assert witness.publication_id == identity
            assert witness.manifest_digest == outputs.manifest.manifest_digest
            assert self.journal.snapshot(identity).complete == witness
            assert identity not in self.completed
            assert self.ledger.release(token)
            self.completed[identity] = witness

        envelope = self.adapter.complete(identity, commit_lease=complete_lease)
        assert self.ledger.available == ResourceVector({"CPU": 1})
        assert self.recovery.snapshot(identity).complete is None
        assert self.adapter.report_terminal(identity)
        _assert_metadata(self.recovery.snapshot(identity))
        reply = protocol.TaskReply(
            pending.task_id, pending.spec.attempt_id, self.executor,
            protocol.TaskReplyStatus.SUCCEEDED, envelope.results,
            target_execution=pending.target_execution, output_publication=envelope,
        )
        assert self.core._publish_reply(
            pending, reply, expected_node_id=self.core.node_id,
            expected_lease_id=identity.lease_id,
        )
        assert not self.journal.snapshot(identity).retained_result_slots
        assert self.recovery.snapshot(identity).adopted.complete == envelope.complete
        assert self.core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
        return envelope.results

    def lose(self, pending):
        for object_id in pending.output_ids:
            owner = self.core.owner_table.snapshot(object_id)
            descriptor = owner.canonical_stored_result
            assert owner.state is ObjectState.READY_STORED and descriptor is not None
            assert owner.output_publication.publication_id.execution == pending.execution
            drop = protocol.DropObjectReplica(
                object_id, pending.spec.attempt_id, self.core.worker_id,
                self.core.node_id, descriptor.checksum,
            )
            reply = self.node._handle_drop_object_replica(drop)
            assert reply.status is protocol.DropObjectReplicaStatus.DROPPED
            assert not self.store.contains(object_id, sealed_only=False)
            assert self.core.owner_table.mark_lost(object_id, pending.spec.attempt_id)
            self.core._stored_descriptors.pop(object_id, None)
        assert self.core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED


class _Fixture:
    def __init__(self) -> None:
        core = object.__new__(CoreWorker)
        self.core = core
        core.job_id = JobID.random()
        core.driver_task_id = TaskID.for_driver(core.job_id)
        core.worker_id = WorkerID.random()
        core.node_id = NodeID.random()
        core.node_address = ("127.0.0.1", 39931)
        core.owner_address = ("127.0.0.1", 39932)
        core.gcs_address = ("127.0.0.1", 39930)
        core.event_sink = MemoryEventSink()
        core._state_lock = _Composition()
        core._completion = core._state_lock
        core._owner_table = ObjectOwnerTable()
        core._recovery = RecoveryManager()
        core._objects = {}
        core._stored_descriptors = {}
        core._submissions = queue.Queue()
        core._ready_tasks = queue.Queue()
        core._protocol_unresolved = {}
        core._object_gc_obligations = {}
        core._dead_nodes = {}
        core._task_finish_barriers = {}
        core._accepting = True
        core._accepted_task_count = 0
        core._submission_index = 0
        core._reference_index = 0
        core._finished_tasks = set()
        core._finishing_tasks = set()
        core._active_task_finishes = set()
        self.gc_checks = []
        core._enqueue_inline_gc_check = self.gc_checks.append
        core._loads_owned_value = cloudpickle.loads

        def no_rpc(*_args, **_kwargs):
            pytest.fail("finish-barrier fixture must never contact a runtime")

        self.backend = _OutputBackend(core, no_rpc)
        core._rpc = self.backend.rpc
        core._borrow_rpc = no_rpc
        core._resolve_node_address = self.backend.address

        def prepare_refs(object_ids):
            # Keep public handles inert: real reference finalizers would create
            # a mailbox thread.  Owner token installation stays in the actual
            # _register_submission admission transaction.
            prepared = []
            for object_id in object_ids:
                token = ("test-local", core._reference_index)
                core._reference_index += 1
                prepared.append((
                    ObjectRef(object_id, core.worker_id, core.owner_address), token
                ))
            return tuple(prepared)

        core._prepare_local_object_refs = prepare_refs
        core._bind_prepared_local_object_refs = (
            lambda prepared: tuple(ref for ref, _token in prepared)
        )
        key = protocol.FunctionKey(core.job_id, __name__, "producer", "v1")
        self.definition = RemoteFunctionDefinition(
            key, protocol.FunctionDefinition.from_payload(key, b"unused-function")
        )

    def submit(self, *refs: ObjectRef, num_returns: int = 1):
        pending, output = self.core._register_submission(
            self.definition, tuple(refs), {}, ResourceVector({"CPU": 1}),
            max_retries=3, num_returns=num_returns, _enqueue=True,
        )
        assert self.core._submissions.get_nowait() is pending
        self.core._submissions.task_done()
        assert all(
            self.core._task_finish_barriers[object_id] is pending
            for object_id in pending.output_ids
        )
        return pending, output

    def succeed(
        self, pending: _PendingTask, *, stored: bool = False, value: object = 7,
    ) -> tuple[protocol.ResultDescriptor, ...]:
        return self.backend.succeed(pending, stored=stored, value=value)

    def lose(self, pending: _PendingTask) -> None:
        self.backend.lose(pending)

    def close_refs(self, pending: _PendingTask) -> None:
        for object_id in pending.output_ids:
            for token in self.core.owner_table.snapshot(object_id).local_tokens:
                assert self.core.owner_table.release_local_reference(object_id, token)

    def queued(self) -> tuple[_PendingTask, ...]:
        values = []
        size = self.core._submissions.qsize()
        assert size <= 16
        for _ in range(size):
            item = self.core._submissions.get_nowait()
            self.core._submissions.task_done()
            if isinstance(item, _PendingTask):
                values.append(item)
        return tuple(values)

    def ready_dependency(self):
        pending, ref = self.submit()
        self.succeed(pending)
        assert self.core._finish_pending_task(pending)
        assert not self.queued()
        return pending, ref


def test_ordinary_stored_loss_waits_for_old_finalizer_before_get_reconstruction():
    fixture = _Fixture()
    core = fixture.core
    dependency, dependency_ref = fixture.ready_dependency()
    pending, ref = fixture.submit(dependency_ref)
    old_hold = pending.dependency_hold
    assert core.owner_table.snapshot(dependency.object_id).submitted_tokens == frozenset({old_hold})
    fixture.succeed(pending, stored=True)
    fixture.lose(pending)
    before = core.owner_table.snapshot(pending.object_id)
    recovery_before = replace(core._recovery.task_record(pending.task_id))

    with pytest.raises(TimeoutError, match="retirement did not finish"):
        core.get(ref, timeout=0)
    assert core._start_or_join_reconstruction(
        pending.object_id, core._objects[pending.object_id]
    ) is None
    assert core.owner_table.snapshot(pending.object_id) == before
    assert core._recovery.task_record(pending.task_id) == recovery_before
    assert core._accepted_task_count == 1
    assert not fixture.queued()

    assert core._finish_pending_task(pending)
    assert core._accepted_task_count == 0
    assert pending.object_id not in core._task_finish_barriers
    assert core.owner_table.snapshot(dependency.object_id).submitted_tokens == frozenset()
    assert pending.object_id in fixture.gc_checks
    assert not fixture.queued()

    # A real get now admits one reconstruction, then timeout=0 exits at its
    # new PENDING state without blocking or executing the queued task.
    with pytest.raises(TimeoutError, match="was not ready before timeout"):
        core.get(ref, timeout=0)
    (reconstructed,) = fixture.queued()
    assert reconstructed.spec.attempt_id == pending.spec.attempt_id.next()
    assert reconstructed.dependency_hold != old_hold
    assert core._task_finish_barriers[pending.object_id] is reconstructed
    assert core._accepted_task_count == 1
    assert core.owner_table.snapshot(dependency.object_id).submitted_tokens == frozenset({
        reconstructed.dependency_hold
    })
    assert not core._finish_pending_task(pending)
    assert core._accepted_task_count == 1
    assert core._task_finish_barriers[pending.object_id] is reconstructed

    fixture.succeed(reconstructed)
    assert core.get(ref, timeout=0) == 7
    assert core._finish_pending_task(reconstructed)
    assert core._finish_pending_task(reconstructed)
    assert core._accepted_task_count == 0
    assert not core.owner_table.snapshot(dependency.object_id).submitted_tokens
    assert not core._task_finish_barriers


def test_last_reference_gc_cannot_erase_output_before_its_finalizer():
    fixture = _Fixture()
    core = fixture.core
    dependency, dependency_ref = fixture.ready_dependency()
    pending, _ref = fixture.submit(dependency_ref)
    fixture.succeed(pending, stored=True)
    fixture.lose(pending)
    fixture.close_refs(pending)
    assert not core.owner_table.snapshot(pending.object_id).is_live
    before = core.owner_table.snapshot(pending.object_id)

    core._reference_released(pending.object_id)

    assert core.owner_table.snapshot(pending.object_id) == before
    assert not core._object_gc_obligations
    assert core._recovery.lineage_for_object(pending.object_id) is not None
    assert core._finish_pending_task(pending)
    core._reference_released(pending.object_id)
    assert core.owner_table.collection_state(pending.object_id) is ObjectCollectionState.COLLECTED
    assert pending.object_id not in core._objects
    assert core._recovery.lineage_for_object(pending.object_id) is None
    assert not core.owner_table.snapshot(dependency.object_id).submitted_tokens
    assert not core.owner_table.snapshot(dependency.object_id).lineage_tokens
    assert core._accepted_task_count == 0
    assert not core._object_gc_obligations


def test_recursive_loss_preflights_every_finish_gate_before_any_reconstruction():
    fixture = _Fixture()
    core = fixture.core
    first, first_ref = fixture.submit()
    fixture.succeed(first, stored=True)
    assert core._finish_pending_task(first)
    assert not fixture.queued()
    second, second_ref = fixture.submit()
    fixture.succeed(second, stored=True)
    root, root_ref = fixture.submit(first_ref, second_ref)
    fixture.succeed(root, stored=True)
    assert core._finish_pending_task(root)
    assert not fixture.queued()
    for item in (first, second, root):
        fixture.lose(item)
    before = {item.object_id: core.owner_table.snapshot(item.object_id) for item in (first, second, root)}
    records = {item.task_id: replace(core._recovery.task_record(item.task_id)) for item in (first, second, root)}

    # first precedes the blocked second in dependency order.  A per-node gate
    # inside the commit loop would incorrectly start first before noticing it.
    assert core._start_or_join_reconstruction(
        root.object_id, core._objects[root.object_id]
    ) is None
    assert not fixture.queued()
    assert core._accepted_task_count == 1
    for item in (first, second, root):
        assert core.owner_table.snapshot(item.object_id) == before[item.object_id]
        assert core._recovery.task_record(item.task_id) == records[item.task_id]
        assert core._recovery.active_recovery(item.task_id) is None
    assert not core._reconstruction._sessions

    assert core._finish_pending_task(second)
    assert not fixture.queued()
    outcome = core._start_or_join_reconstruction(
        root.object_id, core._objects[root.object_id], return_requested_outcome=True
    )
    assert outcome.disposition is ReconstructionDisposition.START
    reconstructed = fixture.queued()
    assert tuple(item.task_id for item in reconstructed) == (first.task_id, second.task_id, root.task_id)
    assert core._accepted_task_count == 3
    for item in reconstructed:
        assert core._task_finish_barriers[item.object_id] is item
        assert core._recovery.task_record(item.task_id).retries_started == 1
    for item in reconstructed:
        fixture.succeed(item)
        assert core._finish_pending_task(item)
    assert core.get(root_ref, timeout=0) == 7
    assert core._accepted_task_count == 0
    assert not core._task_finish_barriers


def test_pending_join_with_its_own_finish_barrier_does_not_block_parent():
    fixture = _Fixture()
    core = fixture.core
    child, child_ref = fixture.submit()
    fixture.succeed(child, stored=True)
    assert core._finish_pending_task(child)
    assert not fixture.queued()
    root, _root_ref = fixture.submit(child_ref)
    fixture.succeed(root, stored=True)
    assert core._finish_pending_task(root)
    assert not fixture.queued()
    fixture.lose(child)
    fixture.lose(root)

    started = core._start_or_join_reconstruction(
        child.object_id, core._objects[child.object_id], return_requested_outcome=True
    )
    assert started.disposition is ReconstructionDisposition.START
    (child_retry,) = fixture.queued()
    assert core._task_finish_barriers[child.object_id] is child_retry
    joined = core._start_or_join_reconstruction(
        child.object_id, core._objects[child.object_id], return_requested_outcome=True
    )
    assert joined.disposition is ReconstructionDisposition.JOIN
    assert core._accepted_task_count == 1 and not fixture.queued()

    graph = core._reconstruction.preflight_graph(root.object_id)
    assert graph.node_for(child.object_id).action.value == "PENDING_JOIN"
    root_start = core._start_or_join_reconstruction(
        root.object_id, core._objects[root.object_id], return_requested_outcome=True
    )
    assert root_start.disposition is ReconstructionDisposition.START
    (root_retry,) = fixture.queued()
    assert root_retry.task_id == root.task_id
    assert core._accepted_task_count == 2
    assert core._task_finish_barriers[child.object_id] is child_retry
    assert core._task_finish_barriers[root.object_id] is root_retry
    assert core.owner_table.snapshot(child.object_id).submitted_tokens == frozenset({
        root_retry.dependency_hold
    })
    for item in (child_retry, root_retry):
        fixture.succeed(item)
        assert core._finish_pending_task(item)
    assert core._accepted_task_count == 0 and not core._task_finish_barriers


@pytest.mark.parametrize("deadline_already_elapsed", (False, True))
def test_recursive_finish_deferral_respects_get_timeout_without_busy_loop(
    monkeypatch: pytest.MonkeyPatch,
    deadline_already_elapsed: bool,
):
    fixture = _Fixture()
    core = fixture.core
    child, child_ref = fixture.submit()
    fixture.succeed(child, stored=True)
    root, root_ref = fixture.submit(child_ref)
    fixture.succeed(root, stored=True)
    assert core._finish_pending_task(root)
    assert not fixture.queued()
    fixture.lose(child)
    fixture.lose(root)
    original = core._start_or_join_reconstruction
    attempts = []

    def bounded_admission(*args, **kwargs):
        attempts.append(args[0])
        if len(attempts) > 6:
            pytest.fail("get retried deferred lineage without checking its timeout")
        return original(*args, **kwargs)

    core._start_or_join_reconstruction = bounded_admission
    now = 100.0
    waits = []

    def monotonic():
        nonlocal now
        if deadline_already_elapsed:
            now += 1.0
        return now

    def wait(seconds):
        nonlocal now
        assert core._completion.depth > 0
        assert 0 < seconds <= 0.01
        waits.append(seconds)
        if len(waits) > 5:
            pytest.fail("get reset the deadline during finish deferral")
        now += seconds

    core._completion.wait = wait
    monkeypatch.setattr(core_module, "time", SimpleNamespace(monotonic=monotonic))
    with pytest.raises(TimeoutError, match="reconstruction admission did not finish"):
        core.get(root_ref, timeout=0.025)
    if deadline_already_elapsed:
        assert len(attempts) == 1 and not waits
    else:
        assert waits == pytest.approx([0.01, 0.01, 0.005])
        assert now == pytest.approx(100.025)
        assert len(attempts) == 4
    assert not fixture.queued()
    assert core._accepted_task_count == 1
    for pending in (child, root):
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.LOST
        assert core._recovery.task_record(pending.task_id).retries_started == 0


def test_finish_gate_arriving_during_foreign_renewal_prevents_local_commit():
    fixture = _Fixture()
    core = fixture.core
    pending, _ref = fixture.submit()
    fixture.succeed(pending, stored=True)
    assert core._finish_pending_task(pending)
    assert not fixture.queued()
    fixture.lose(pending)
    before = core.owner_table.snapshot(pending.object_id)
    record = replace(core._recovery.task_record(pending.task_id))
    calls = []

    def renewal(task_id, attempt_id):
        # The foreign hold exchange runs outside the Core composition lock.
        # Model a delayed admission exposing an old unfinished output before
        # it returns READY; commit must re-read the finish predicate.
        assert core._state_lock.depth == 0
        core._install_task_finish_barrier_locked(pending)
        calls.append((task_id, attempt_id))
        return ForeignLineageRenewalResult(
            task_id, attempt_id, ForeignLineageRenewalDisposition.READY, (), 0
        )

    def never_commit(*_args):
        pytest.fail("foreign renewal cannot commit across a new finish gate")

    core._foreign_lineage_registry = SimpleNamespace(
        snapshot=lambda task_id: object() if task_id == pending.task_id else None
    )
    core._foreign_lineage_runtime = SimpleNamespace(
        drive_renewal=renewal, validate_renewal_ready=never_commit,
        complete_renewal=never_commit,
    )
    assert core._start_or_join_reconstruction(
        pending.object_id, core._objects[pending.object_id]
    ) is None
    assert calls == [(pending.task_id, pending.spec.attempt_id.next())]
    assert core.owner_table.snapshot(pending.object_id) == before
    assert core._recovery.task_record(pending.task_id) == record
    assert core._recovery.active_recovery(pending.task_id) is None
    assert core._accepted_task_count == 0 and not fixture.queued()


def test_retirement_window_revalidates_all_foreign_renewals_before_any_start():
    fixture = _Fixture()
    core = fixture.core
    leaf, leaf_ref = fixture.submit()
    fixture.succeed(leaf, stored=True)
    assert core._finish_pending_task(leaf) and not fixture.queued()
    root, _root_ref = fixture.submit(leaf_ref)
    fixture.succeed(root, stored=True)
    assert core._finish_pending_task(root) and not fixture.queued()
    fixture.lose(leaf)
    fixture.lose(root)
    denied = []
    retired = []
    checks = []
    retire = core._retire_lost_output_memberships
    def retire_then_revoke(output):
        assert core._state_lock.depth == 0
        result = retire(output)
        retired.append(output)
        if output == root.object_id:
            denied.append(root.task_id)
        return result
    def validate(task_id, attempt_id):
        checks.append(task_id)
        if task_id in denied:
            raise SystemTaskError("foreign owner died during output retirement")
    core._retire_lost_output_memberships = retire_then_revoke
    core._foreign_lineage_registry = SimpleNamespace(snapshot=lambda _task: object())
    core._foreign_lineage_runtime = SimpleNamespace(
        drive_renewal=lambda task, attempt: ForeignLineageRenewalResult(
            task, attempt, ForeignLineageRenewalDisposition.READY, (), 0),
        validate_renewal_ready=validate,
        complete_renewal=lambda *_: pytest.fail("no producer may START before all renewed holds validate"),
    )
    with pytest.raises(SystemTaskError, match="foreign owner died"):
        core._start_or_join_reconstruction(root.object_id, core._objects[root.object_id])
    assert retired == [leaf.object_id, root.object_id]
    assert checks == [leaf.task_id, root.task_id, leaf.task_id, root.task_id]
    assert core._accepted_task_count == 0 and not fixture.queued()
    for pending in (leaf, root):
        record = core._recovery.task_record(pending.task_id)
        assert record.current_attempt == pending.spec.attempt_id and record.retries_started == 0
        assert record.state is TaskState.SUCCEEDED
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.LOST
        assert core.owner_table.snapshot(pending.object_id).current_attempt == pending.spec.attempt_id


def test_foreign_release_ack_loss_keeps_finish_gate_and_local_holds_until_retry():
    fixture = _Fixture()
    core = fixture.core
    dependency, dependency_ref = fixture.ready_dependency()
    original, ref = fixture.submit(dependency_ref)
    foreign_owner = WorkerID.random()
    guard = _ForeignDependencyGuard(
        ObjectID.for_task(TaskID.random()), foreign_owner, ("127.0.0.1", 39933),
        core.worker_id, "foreign-source", protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, core.worker_id,
            original.task_id, original.spec.attempt_id,
        ),
    )
    accepted_guard = replace(
        guard, object_id=ObjectID.for_task(TaskID.random()),
        borrower_token="earlier-acknowledged-source",
    )
    pending = replace(
        original, foreign_dependency_guards=(accepted_guard, guard),
    )
    core._install_task_finish_barrier_locked(pending)
    fixture.succeed(pending, stored=True)
    fixture.lose(pending)
    releases = []

    def release(_address, handler, request):
        assert handler == "release_owned_object_for_task"
        releases.append(request)
        if len(releases) == 2:
            raise TransportTimeout("Release applied; acknowledgement lost")
        return protocol.ReleaseOwnedObjectForTaskReply(
            request.object_id, request.owner_worker_id, request.borrower_worker_id,
            request.hold, True, False,
        )

    core._borrow_rpc = release
    assert not core._finish_pending_task(pending)
    assert core._task_finish_barriers[pending.object_id] is pending
    assert core._accepted_task_count == 1
    assert pending.task_key in core._finishing_tasks
    assert pending.task_key not in core._active_task_finishes
    assert pending.task_key not in core._finished_tasks
    assert core.owner_table.snapshot(dependency.object_id).submitted_tokens == frozenset({
        pending.dependency_hold
    })
    assert pending.task_key in core._foreign_guard_release_retries
    retry = core._foreign_guard_release_retries[pending.task_key]
    assert retry.pending is pending
    assert retry.released_keys == (core._foreign_guard_key(accepted_guard),)
    with pytest.raises(TimeoutError, match="retirement did not finish"):
        core.get(ref, timeout=0)
    assert core._start_or_join_reconstruction(
        pending.object_id, core._objects[pending.object_id]
    ) is None
    assert not fixture.queued()

    assert core._finish_pending_task(pending)
    assert releases[0].object_id == accepted_guard.object_id
    assert releases[1] == releases[2]
    assert core._accepted_task_count == 0
    assert pending.object_id not in core._task_finish_barriers
    assert not core._foreign_guard_release_retries
    assert not core.owner_table.snapshot(dependency.object_id).submitted_tokens
    assert core._finish_pending_task(pending)
    assert len(releases) == 3 and core._accepted_task_count == 0


def test_get_rechecks_finish_predicate_after_finalizer_notifies_before_condition(
    monkeypatch: pytest.MonkeyPatch,
):
    fixture = _Fixture()
    core = fixture.core
    pending, ref = fixture.submit()
    fixture.succeed(pending, stored=True)
    fixture.lose(pending)
    completion = core._completion
    calls = []

    def finish_before_condition():
        calls.append("finish")
        assert core._finish_pending_task(pending)
        assert not core._task_finish_barriers

    def arm_interleave():
        # get computes the retirement wait deadline only after its old
        # snapshot saw the barrier, immediately before entering the condition.
        completion.before_enter = finish_before_condition
        return 100.0

    ticks = iter((lambda: 100.0, arm_interleave, lambda: 102.0))
    monkeypatch.setattr(
        core_module, "time", SimpleNamespace(monotonic=lambda: next(ticks)())
    )
    notifications = completion.notifications
    with pytest.raises(TimeoutError, match="was not ready before timeout"):
        core.get(ref, timeout=1.0)
    assert calls == ["finish"]
    assert completion.waits == 0
    assert completion.notifications > notifications
    (reconstructed,) = fixture.queued()
    assert reconstructed.spec.attempt_id == pending.spec.attempt_id.next()
    assert core._accepted_task_count == 1
    assert core._task_finish_barriers[pending.object_id] is reconstructed


def test_system_retry_moves_every_barrier_but_preserves_logical_hold():
    fixture = _Fixture()
    core = fixture.core
    dependency, dependency_ref = fixture.ready_dependency()
    pending, _refs = fixture.submit(dependency_ref, num_returns=2)
    assert not core._retry_system_failure(pending, SystemTaskError("worker exited"))
    (retried,) = fixture.queued()
    assert retried.spec.attempt_id == pending.spec.attempt_id.next()
    assert retried.dependency_hold == pending.dependency_hold
    assert core._accepted_task_count == 1
    assert all(core._task_finish_barriers[object_id] is retried for object_id in pending.output_ids)
    assert not core._finish_pending_task(pending)
    assert core.owner_table.snapshot(dependency.object_id).submitted_tokens == frozenset({
        pending.dependency_hold
    })
    fixture.succeed(retried)
    assert core._finish_pending_task(retried)
    assert core._accepted_task_count == 0
    assert not core._task_finish_barriers
    assert not core.owner_table.snapshot(dependency.object_id).submitted_tokens
