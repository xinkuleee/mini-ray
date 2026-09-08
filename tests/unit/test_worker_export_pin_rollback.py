"""Current Worker selected outputs and separate generic export compatibility.

Two obsolete multi-return refusal cases are replaced with unified-publication
success/projection contracts. Four generic export-release cases remain pure
compatibility checks, not the current Worker hot path. The original drain
case is separately marked L1: three real Core threads and actual Worker/Core
drain, with one saved retry event instead of a Timer and no network/process.
"""

from __future__ import annotations

import threading
import time

import cloudpickle
import pytest

from miniray import protocol
from miniray.core import CoreWorker, ObjectRef, _ObjectWaiter
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable
from miniray.resources import ResourceVector
from miniray.output_publication import OutputPublicationNodeIncarnation
from miniray.task_outputs import TargetExecutionKey
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER,
    SEAL_OBJECT_HANDLER,
    START_WORKER_LEASE_HANDLER,
    WorkerServer,
)


def _core(worker_id: WorkerID) -> CoreWorker:
    core = object.__new__(CoreWorker)
    core.job_id = JobID.random()
    core.worker_id = worker_id
    core.node_id = NodeID.random()
    core.node_address = ("127.0.0.1", 28101)
    core.driver_task_id = TaskID.for_driver(core.job_id)
    core._owner_table = ObjectOwnerTable()
    core._objects = {}
    core._stored_descriptors = {}
    core._state_lock = threading.RLock()
    core._completion = threading.Condition(core._state_lock)
    core._accepting = False
    core._export_pin_release_obligations = {}
    core._object_gc_obligations = {}
    core._inline_gc_obligations = core._object_gc_obligations
    core._attempt_borrow_releases = {}
    core._borrowed_release_obligations = {}
    core._owner_protocol_open = True
    core._initialize_reference_events()
    return core


def _child(core: CoreWorker) -> ObjectRef:
    task_id = TaskID.derive(
        core.job_id, core.driver_task_id, 7 + len(core._objects)
    )
    object_id = ObjectID.for_task(task_id)
    attempt = AttemptID(task_id, 0)
    core.owner_table.register(object_id, current_attempt=attempt)
    core.owner_table.publish_inline(object_id, attempt, b"child")
    core._objects[object_id] = _ObjectWaiter(threading.Event())
    core.owner_table.add_local_reference(
        object_id, ("test-local", object_id)
    )
    return ObjectRef(object_id, core.worker_id)


def _worker(core: CoreWorker) -> WorkerServer:
    worker = object.__new__(WorkerServer)
    worker.worker_id = core.worker_id
    worker.node_id = core.node_id
    worker.node_address = core.node_address
    worker.gcs_address = None
    worker.inline_threshold = 0
    worker._execution_lock = threading.Lock()
    worker._embedded_core_lock = threading.Lock()
    worker._embedded_core = core
    worker._embedded_core_job_id = core.job_id
    worker._embedded_core_stopped = False
    worker._server = type(
        "Server", (), {"address": ("127.0.0.1", 28102)}
    )()
    worker._replies = {}
    worker._cached_pushes = {}
    worker._completion_acked = set()
    worker._lease_bindings = {}
    worker._attempt_leases = {}
    worker._functions = {}
    return worker


def _push(
    worker: WorkerServer, function: object, *, num_returns: int
) -> protocol.PushTask:
    task_id = TaskID.derive(
        worker._embedded_core_job_id,
        TaskID.for_driver(worker._embedded_core_job_id),
        9,
    )
    attempt = AttemptID(task_id, 0)
    payload = cloudpickle.dumps(function)
    definition = protocol.FunctionDefinition.from_payload(
        protocol.FunctionKey(
            worker._embedded_core_job_id, __name__, "producer", "v1"
        ),
        payload,
    )
    spec = protocol.TaskSpec(
        job_id=worker._embedded_core_job_id,
        task_id=task_id,
        attempt_id=attempt,
        function=definition.key,
        args=(),
        num_returns=num_returns,
        resources=ResourceVector(),
        owner_worker_id=WorkerID.random(),
        function_definition=definition,
    )
    return protocol.PushTask(LeaseID.random(), worker.worker_id, spec)


def _accepted_start(message: protocol.StartWorkerLease, node_id: NodeID):
    return protocol.StartWorkerLeaseReply(
        message.lease_id, protocol.LeaseExecutionState.RUNNING, accepted=True,
        node_incarnation=OutputPublicationNodeIncarnation(node_id, 21001, 3),
    )


def _accepted_completion(message: protocol.CompleteWorkerLease):
    return protocol.CompleteWorkerLeaseReply(
        message.lease_id, message.task_id, message.attempt_id,
        message.worker_id, message.status,
        protocol.LeaseExecutionState.COMPLETED, accepted=True, released=True,
    )


class _SelectedContainedWorker:
    """One Worker call, two original outputs, and real selected publication.

    Start/grant is the existing typed Worker-fixture boundary, not an OS
    scheduling test. Discovery, Node journal/storage/pins/graph/Complete, owner
    CAS and per-selected-slot GC are actual in-memory authorities. At most two
    child puts and two sub-1-KiB stored results use the existing 128-KiB store.
    Targeted mode preserves the original unselected PENDING owner entry: it
    does not invent successful history, reconstruction or whole-owner cleanup.
    """

    def __init__(self, monkeypatch, *, targeted):
        from dataclasses import replace
        from miniray.ref_transfer import ReferenceExportSession
        from tests.unit._pure_core import make_pure_core
        from tests.unit.test_worker_unified_output import _ActualNodePublication, _Fixture, _install_no_runtime

        _install_no_runtime(monkeypatch)

        def forbidden(*_args, **_kwargs):
            pytest.fail("selected Worker publication reached an obsolete export or runtime effect")

        def ready_receipt(event, timeout=None):
            assert event.is_set(), "pure source close attempted a blocking wait"
            return True

        monkeypatch.setattr(threading.Event, "wait", ready_receipt)
        monkeypatch.setattr(CoreWorker, "request_export_pin_release", forbidden)
        monkeypatch.setattr(ReferenceExportSession, "__enter__", forbidden)
        self.targeted = targeted
        values = []
        self.worker_fixture = fixture = _Fixture(monkeypatch, lambda: tuple(values), count=2, threshold=0)
        self.source = source = make_pure_core()
        source.worker_id, source.job_id, source.node_id = fixture.worker.worker_id, fixture.push.spec.job_id, fixture.worker.node_id
        source.driver_task_id = TaskID.for_driver(source.job_id)
        source.owner_address = fixture.worker.address
        self.children = (source.put("first-child"),) if targeted else (source.put("first-child"), source.put("second-child"))
        # Preserve both original Python return values, including the nested
        # second ref in full mode and ordinary integer sibling in target mode.
        values.extend((self.children[0], 2) if targeted else (self.children[0], {"nested": self.children[1]}))
        if targeted:
            target = TargetExecutionKey.from_task_spec(fixture.push.spec, (fixture.push.spec.return_ids()[0],))
            fixture.push = replace(fixture.push, target_execution=target)
        self.spec = fixture.push.spec
        self.outputs = self.spec.return_ids()
        self.before = tuple(source.owner_table.snapshot(child.object_id) for child in self.children)
        self.backend = backend = _ActualNodePublication(fixture, child_tables={source.worker_id: source.owner_table})
        self.owner = ObjectOwnerTable()
        self.owner.register_task_outputs(self.spec, local_tokens=("output-0", "output-1"))
        self.unselected_before = self.owner.snapshot(self.outputs[1]) if targeted else None
        self.prepares = []

        def prepare(message):
            assert not self.prepares
            self.prepares.append(message)
            assert message.manifest == fixture.pending.outputs.manifest
            assert message.slot_payloads == fixture.pending.outputs.slot_payloads
            # Full discovery has serialized all selected slots, but it cannot
            # have installed any child pin, graph reservation or physical byte.
            assert tuple(source.owner_table.snapshot(child.object_id) for child in self.children) == self.before
            assert not backend.journal.publication_ids() and backend.store.used_bytes == 0
            assert not backend.graph.has_active_obligations()
            assert len(message.manifest.slots) == (1 if targeted else 2)
            assert all(slot.size_bytes <= 1024 and len(slot.transfers) == 1 for slot in message.manifest.slots)
            return backend.prepare(message)

        fixture.on_prepare = prepare

    def run(self):
        from miniray import output_protocol as wire
        from miniray.task_outputs import TaskExecutionKey
        from miniray.ownership import ObjectState

        fixture, backend = self.worker_fixture, self.backend
        reply = fixture.worker._handle_push_task(fixture.push)
        assert reply.status is protocol.TaskReplyStatus.SUCCEEDED and reply.error is None
        self.envelope = envelope = reply.output_publication
        assert envelope is not None and envelope == fixture.complete_envelope
        manifest = envelope.manifest
        assert manifest.publication_id.full_output_ids == self.outputs
        assert fixture.push.spec.num_returns == 2 and fixture.push.spec == self.spec
        expected_ids = self.outputs[:1] if self.targeted else self.outputs
        assert manifest.publication_id.output_ids == expected_ids
        assert tuple(result.object_id for result in reply.results) == expected_ids
        assert all(result.storage is protocol.ResultStorage.OBJECT_STORE for result in reply.results)
        assert manifest.execution == (fixture.push.target_execution or TaskExecutionKey.from_task_spec(self.spec))
        assert reply.target_execution == fixture.push.target_execution
        assert fixture.handlers == [START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER, COMPLETE_WORKER_LEASE_HANDLER]
        assert len(self.prepares) == 1 and backend.completions == [envelope.complete]
        assert backend.ledger.available == backend.ledger.total
        assert not fixture.worker._prepared_output_replies and fixture.key in fixture.worker._completion_acked
        assert fixture.worker._handle_push_task(fixture.push) is reply
        assert fixture.executions == [True] and len(fixture.calls) == 3 and len(backend.completions) == 1
        self.transfers = tuple(slot.transfers[0] for slot in manifest.slots)
        for index, (child, transfer) in enumerate(zip(self.children, self.transfers)):
            assert transfer.contained_object_id == child.object_id
            assert transfer.final_hold.container_object_id == expected_ids[index]
            assert transfer.final_hold.container_owner_worker_id == self.spec.owner_worker_id
            snapshot = self.source.owner_table.snapshot(child.object_id)
            assert snapshot.contained_holds == frozenset({transfer.final_hold})
            assert snapshot.local_tokens == self.before[index].local_tokens
            assert self.source.owner_table.contained_release_was_seen(child.object_id, transfer.provisional_hold)
        assert backend.store.used_bytes == sum(result.size_bytes for result in reply.results) <= 2048
        assert all(backend.store.get(result.object_id) == payload for result, payload in zip(reply.results, self.prepares[0].slot_payloads))
        if self.targeted:
            assert self.owner.snapshot(self.outputs[1]) == self.unselected_before
            assert self.unselected_before.state is ObjectState.PENDING
            assert not backend.store.contains(self.outputs[1], sealed_only=False)
        return reply

    def adopt(self):
        from miniray.output_publication_journal import OutputPublicationAdoptionProof
        from miniray.ownership import OutputOwnerPublicationPlan

        backend, envelope = self.backend, self.envelope
        self.graph_manifest = envelope.manifest.to_graph_manifest()
        assert self.graph_manifest is not None
        assert backend.graph.commit_manifest(self.graph_manifest).manifest == self.graph_manifest
        plan = OutputOwnerPublicationPlan(envelope.manifest.execution, envelope)
        assert self.owner.commit_output_publication(plan).committed
        proof = OutputPublicationAdoptionProof(envelope.complete, self.spec.owner_worker_id, "selected-owner-adoption")
        backend.recovery.report_adopted(proof)
        backend.journal.retire_completed(proof)
        assert backend.adapter.report_terminal(envelope.publication_id)
        assert not backend.journal.snapshot(envelope.publication_id).retained_result_slots
        if self.targeted:
            assert self.owner.snapshot(self.outputs[1]) == self.unselected_before

    def collect_slot(self, slot_index):
        from miniray.output_publication_journal import OutputPublicationSlotCleanupProof
        from miniray.ownership import ObjectCollectionState

        backend, envelope = self.backend, self.envelope
        slot = envelope.manifest.slots[slot_index]
        assert self.owner.release_local_reference(slot.object_id, "output-{}".format(slot.object_id.return_index))
        plan = self.owner.begin_output_publication_collection(slot.object_id, collection_id="selected-gc-{}".format(slot_index))
        assert plan is not None and plan.membership.manifest == envelope.manifest
        for transfer in slot.transfers:
            request = protocol.ReleaseContainedReference(transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.final_hold)
            reply = self.source.release_contained_reference(request)
            assert reply.accepted and reply.released
        graph_receipt = backend.graph.release_manifest_container(self.graph_manifest, slot.object_id)
        assert graph_receipt.released_edges == slot.edges
        request = protocol.DropObjectReplica(
            slot.object_id, envelope.publication_id.attempt_id, self.spec.owner_worker_id, backend.node.node_id, slot.checksum,
        )
        dropped = backend.node._handle_drop_object_replica(request)
        assert dropped.accepted and dropped.status is protocol.DropObjectReplicaStatus.DROPPED
        assert (dropped.object_id, dropped.producer_attempt_id, dropped.owner_worker_id, dropped.node_id, dropped.checksum) == (
            request.object_id, request.producer_attempt_id, request.owner_worker_id, request.node_id, request.checksum,
        )
        proof = OutputPublicationSlotCleanupProof(envelope.complete, self.spec.owner_worker_id, slot_index, slot.object_id, plan.collection_id)
        backend.recovery.report_slot_collected(proof)
        assert self.owner.complete_output_publication_collection(plan, graph_receipt).collection.collected
        assert self.owner.collection_state(slot.object_id) is ObjectCollectionState.COLLECTED
        assert not backend.store.contains(slot.object_id, sealed_only=False)
        if self.targeted:
            assert self.owner.snapshot(self.outputs[1]) == self.unselected_before

    def close_sources(self):
        from miniray.core import _WAKE_COORDINATOR
        from miniray.ownership import ObjectCollectionState

        assert self.backend.store.used_bytes == 0 and not self.backend.node._sealed_metadata
        assert not self.backend.graph.has_active_obligations()
        assert len(self.backend.recovery.snapshot(self.envelope.publication_id).slot_collections) == len(self.transfers)
        for child in self.children:
            assert not self.source.owner_table.snapshot(child.object_id).contained_holds
            child.close(timeout=0)
        assert self.source._reference_mailbox.pending.qsize() <= 4
        self.source._reference_mailbox.drain()
        assert all(self.source.owner_table.collection_state(child.object_id) is ObjectCollectionState.COLLECTED for child in self.children)
        assert not self.source._objects and not self.source._object_gc_obligations
        count = self.source._submissions.qsize()
        assert count <= 4
        for _ in range(count):
            item = self.source._submissions.get_nowait()
            try:
                assert item is _WAKE_COORDINATOR
            finally:
                self.source._submissions.task_done()
        assert self.source._submissions.empty() and self.source._submissions.unfinished_tasks == 0

    def close(self):
        from tests.unit._pure_core import close_pure_core

        for child in self.children:
            child.close(timeout=0)
        # An unselected PENDING sibling is not collectable. Release only its
        # real test token; never fabricate an error/success to erase its entry.
        for object_id in self.outputs:
            if self.owner.contains(object_id):
                for token in self.owner.snapshot(object_id).local_tokens:
                    self.owner.release_local_reference(object_id, token)
        close_pure_core(self.source)


@pytest.mark.unit
def test_multi_return_contained_refs_use_one_unified_publication_and_slot_scoped_gc(monkeypatch):
    """Replaces the obsolete two-contained-return rejection requirement."""
    f = _SelectedContainedWorker(monkeypatch, targeted=False)
    try:
        f.run()
        assert len(f.transfers) == 2 and f.transfers[0].final_hold != f.transfers[1].final_hold
        f.adopt()
        sibling = f.owner.snapshot(f.outputs[1])
        second_bytes = f.backend.store.get(f.outputs[1])
        f.collect_slot(0)
        assert f.owner.snapshot(f.outputs[1]) == sibling
        assert f.backend.store.get(f.outputs[1]) == second_bytes
        assert f.source.owner_table.snapshot(f.children[1].object_id).contained_holds == frozenset({f.transfers[1].final_hold})
        f.collect_slot(1)
        assert all(not f.owner.contains(output) for output in f.outputs)
        f.close_sources()
    finally:
        f.close()


@pytest.mark.unit
def test_target_single_contained_slot_keeps_full_identity_without_publishing_other_slot(monkeypatch):
    """Selected success keeps both original IDs, not a one-return TaskSpec."""
    from dataclasses import replace
    from miniray.ownership import ObjectState

    f = _SelectedContainedWorker(monkeypatch, targeted=True)
    try:
        reply = f.run()
        assert len(f.transfers) == 1 and reply.target_execution.target_output_ids == f.outputs[:1]
        with pytest.raises(protocol.ProtocolError, match="complete output manifest"):
            replace(f.worker_fixture.push, spec=replace(f.spec, num_returns=1))
        f.adopt()
        f.collect_slot(0)
        assert f.owner.snapshot(f.outputs[1]) == f.unselected_before
        assert not f.backend.store.contains(f.outputs[1], sealed_only=False)
        assert f.owner.release_local_reference(f.outputs[1], "output-1")
        assert f.owner.snapshot(f.outputs[1]).state is ObjectState.PENDING
        assert not f.owner.snapshot(f.outputs[1]).local_tokens
        assert f.owner.begin_collection(f.outputs[1]) is None
        f.close_sources()
    finally:
        f.close()


@pytest.fixture
def _no_generic_export_runtime(monkeypatch):
    """Guard the explicitly driven generic export-release retries below."""
    import multiprocessing.process
    import socket
    import subprocess

    from miniray import core as core_module, node as node_module, transport
    from miniray import worker as worker_module

    def forbidden(*_args, **_kwargs):
        pytest.fail("pure generic export retry attempted runtime infrastructure")

    def already_set(event, timeout=None):
        assert event.is_set(), "pure local close attempted a blocking wait"
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (WorkerServer, "__init__"),
        (node_module.NodeServer, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "__init__"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (core_module, node_module, worker_module):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_generic_export_runtime")
def test_failed_unpin_is_visible_and_reference_mailbox_retries_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generic export compatibility, not Node/Worker publication rollback.

    A real put and generic serializer create the child and its export pin.
    Only the scheduler-to-mailbox delivery is manual: the original retry
    event, round claim, release driver, tombstone and final GC remain real.
    No Worker, Node, executable Task, timer or actual shutdown is involved.
    """
    from miniray.core import _RetryExportPinRelease, _WAKE_COORDINATOR
    from miniray.ownership import ObjectCollectionState, ObjectState
    from miniray.ref_transfer import ReferenceExportSession
    from tests.unit._pure_core import close_pure_core, make_pure_core

    core = make_pure_core()
    child = core.put("child")
    real_release = core.owner_table.release_contained_reference
    calls, scheduled = [], []
    token = None

    def fail_once(object_id, transfer_token):
        assert object_id == child.object_id and transfer_token == token
        calls.append((object_id, transfer_token))
        assert len(calls) <= 2
        if len(calls) == 1:
            raise RuntimeError("transient owner-table failure")
        return real_release(object_id, transfer_token)

    def deliver(mailbox, event, delay):
        assert mailbox is core._reference_mailbox and mailbox.pending.empty()
        assert type(event) is _RetryExportPinRelease and not scheduled
        assert event.key == (child.object_id, token) and event.scheduled_round == 1
        obligation = core._export_pin_release_obligations[event.key]
        assert obligation.retry_scheduled and obligation.retry_round == event.scheduled_round
        assert delay == 0.01
        scheduled.append(event)
        # Preserve the real event/FIFO rather than inventing a completed ACK.
        mailbox.events.put_nowait(event)

    monkeypatch.setattr(
        core.owner_table, "release_contained_reference", fail_once
    )
    monkeypatch.setattr(core, "_schedule_reference_event", deliver)
    try:
        with ReferenceExportSession(
            core.worker_id, core.owner_address,
            pin=core.owner_table.add_contained_reference,
            unpin=core.request_export_pin_release,
        ) as session:
            payload = cloudpickle.dumps({"child": child})
            assert len(payload) <= 1024 and session.exported_count == 1
            token = session._exports[0][3]
            # Generic commit without an outer ObjectID retains one legacy
            # transfer pin; it is not a selected-output publication backend.
            assert session.commit() == ()
        before = core.owner_table.snapshot(child.object_id)
        assert before.state is ObjectState.READY_INLINE and len(before.local_tokens) == 1
        assert before.contained_tokens == frozenset({token})
        assert core._recovery.reconstruction_snapshot(child.object_id).is_put
        with core._state_lock:
            core._accepting = False
        assert not core.request_export_pin_release(child.object_id, token)
        key = (child.object_id, token)
        assert key in core._export_pin_release_obligations
        obligation = core._export_pin_release_obligations[key]
        assert obligation.retry_scheduled
        assert obligation.retry_round == 1 and obligation.key == key
        assert core.owner_table.snapshot(child.object_id) == before
        assert not core.owner_table.contained_release_was_seen(child.object_id, token)
        assert core._reference_mailbox.events.qsize() == core._reference_mailbox.events.unfinished_tasks == 1
        assert not core.can_finalize_shutdown(
            require_distributed_clean=False
        )

        event = core._reference_mailbox.events.get_nowait()
        try:
            assert event is scheduled[0]
            assert core._claim_export_pin_release_retry(key, event.scheduled_round)
            assert core._drive_export_pin_release(key)
        finally:
            core._reference_mailbox.events.task_done()
        assert calls == [key, key]
        assert not core._export_pin_release_obligations
        assert core.owner_table.contained_release_was_seen(
            child.object_id, token
        )
        assert not core.owner_table.snapshot(child.object_id).contained_tokens
        assert core.owner_table.snapshot(child.object_id).local_tokens == before.local_tokens
        assert core.can_finalize_shutdown(require_distributed_clean=False)
        assert not core._claim_export_pin_release_retry(key, event.scheduled_round)
        assert core._reference_mailbox.events.empty() and core._reference_mailbox.events.unfinished_tasks == 0
        child.close(timeout=0)
        assert core._reference_mailbox.pending.qsize() == 1
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(child.object_id) is ObjectCollectionState.COLLECTED
        assert not core._recovery.reconstruction_snapshot(child.object_id).is_put
        assert core._recovery.lineage_for_object(child.object_id) is None
        assert not core._objects and not core._object_gc_obligations
        assert len(core._reference_mailbox.releases) == 1
        assert core._reference_mailbox.events.empty() and core._reference_mailbox.events.unfinished_tasks == 0
        count = core._submissions.qsize()
        assert count <= 4
        for _ in range(count):
            item = core._submissions.get_nowait()
            try:
                assert item is _WAKE_COORDINATOR
            finally:
                core._submissions.task_done()
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        assert core._accepted_task_count == 0 and not core._task_finish_barriers and not core._protocol_unresolved
    finally:
        child.close(timeout=0)
        close_pure_core(core)


@pytest.mark.unit
def test_worker_session_hands_failed_rollback_to_core_before_returning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Historical name: generic ReferenceExportSession, not Worker publication.

    One child/rollback, synchronous Core mailbox, no runtime or waiting.
    Normal Worker publication uses zero-effect OutputDiscoverySession instead.
    """
    from miniray.ref_transfer import ReferenceExportSession
    from tests.unit._pure_core import make_pure_core, close_pure_core
    from tests.unit.test_worker_unified_output import _install_no_runtime

    _install_no_runtime(monkeypatch)
    core = make_pure_core()
    child = _child(core)
    real_release = core.owner_table.release_contained_reference
    calls = 0
    scheduled = []

    def fail_once(object_id, transfer_token):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient rollback failure")
        return real_release(object_id, transfer_token)

    monkeypatch.setattr(
        core.owner_table, "release_contained_reference", fail_once
    )
    monkeypatch.setattr(
        core, "_schedule_reference_event",
        lambda _mailbox, event, _delay: scheduled.append(event),
    )
    try:
        with pytest.raises(RuntimeError, match="abort publication"):
            with ReferenceExportSession(
                core.worker_id, core.owner_address,
                pin=core.owner_table.add_contained_reference,
                unpin=core.request_export_pin_release,
            ) as session:
                cloudpickle.dumps({"child": child})
                raise RuntimeError("abort publication")

        # The short-lived session may forget the pin only because the owning
        # Core already retained its exact identity and scheduled a retry.
        assert session.exported_count == 0
        assert len(core._export_pin_release_obligations) == 1
        key, obligation = next(
            iter(core._export_pin_release_obligations.items())
        )
        assert key == (child.object_id, obligation.transfer_token)
        assert obligation.transfer_token in (
            core.owner_table.snapshot(child.object_id).contained_tokens
        )
        assert len(scheduled) == 1
        assert scheduled[0].key == key
        assert core._claim_export_pin_release_retry(
            key, scheduled[0].scheduled_round
        )
        assert core._drive_export_pin_release(key)
        assert calls == 2
        assert not core._export_pin_release_obligations
        assert not core.owner_table.snapshot(child.object_id).contained_tokens
    finally:
        for token in tuple(core.owner_table.snapshot(child.object_id).local_tokens):
            assert core.owner_table.release_local_reference(child.object_id, token)
        close_pure_core(core)


class _GenericExportRetry:
    """One put/export pin and at most two original retry events, no runtime.

    This is the generic Core compatibility primitive, not the Worker's normal
    output publication path. Owner mutation, retry-round selection and GC are
    real; only delivery into the existing mailbox FIFO is synchronous.
    """

    def __init__(self, monkeypatch):
        from miniray.core import _RetryExportPinRelease
        from miniray.ref_transfer import ReferenceExportSession
        from tests.unit._pure_core import make_pure_core

        self.core = core = make_pure_core()
        self.child = core.put("generic-export-child")
        with ReferenceExportSession(
            core.worker_id, core.owner_address,
            pin=core.owner_table.add_contained_reference, unpin=core.request_export_pin_release,
        ) as session:
            payload = cloudpickle.dumps({"child": self.child})
            assert len(payload) <= 1024 and session.exported_count == 1
            self.token = session._exports[0][3]
            assert session.commit() == ()
        self.key = self.child.object_id, self.token
        self.before = core.owner_table.snapshot(self.child.object_id)
        assert self.before.contained_tokens == frozenset({self.token})
        assert len(self.before.local_tokens) == 1
        self.available = False
        self.calls, self.events = [], []
        actual_release = core.owner_table.release_contained_reference

        def release(object_id, transfer_token):
            assert (object_id, transfer_token) == self.key and len(self.calls) < 3
            self.calls.append((object_id, transfer_token))
            if not self.available:
                assert core.owner_table.snapshot(object_id) == self.before
                raise RuntimeError("owner table temporarily unavailable")
            return actual_release(object_id, transfer_token)

        def schedule(mailbox, event, delay):
            assert mailbox is core._reference_mailbox and type(event) is _RetryExportPinRelease
            assert event.key == self.key and len(self.events) < 2
            obligation = core._export_pin_release_obligations[self.key]
            assert obligation.retry_scheduled and obligation.retry_round == event.scheduled_round
            assert event.scheduled_round == len(self.events) + 1
            assert delay == 0.01 * 2 ** (event.scheduled_round - 1)
            assert mailbox.events.qsize() <= 1
            self.events.append(event)
            mailbox.events.put_nowait(event)

        monkeypatch.setattr(core.owner_table, "release_contained_reference", release)
        monkeypatch.setattr(core, "_schedule_reference_event", schedule)
        with core._state_lock:
            core._accepting = False  # inert admission fence for the drain precheck

    def take(self, index, *, claimed, released=None):
        core, mailbox = self.core, self.core._reference_mailbox
        event = mailbox.events.get_nowait()
        try:
            assert event is self.events[index]
            accepted = core._claim_export_pin_release_retry(event.key, event.scheduled_round)
            assert accepted is claimed
            if accepted:
                assert core._drive_export_pin_release(event.key) is released
        finally:
            mailbox.events.task_done()
        return event

    def collect(self):
        from miniray.core import _WAKE_COORDINATOR
        from miniray.ownership import ObjectCollectionState

        core, child = self.core, self.child
        assert not core._export_pin_release_obligations
        assert core.owner_table.contained_release_was_seen(child.object_id, self.token)
        assert not core.owner_table.snapshot(child.object_id).contained_tokens
        assert core.owner_table.snapshot(child.object_id).local_tokens == self.before.local_tokens
        assert core.can_finalize_shutdown(require_distributed_clean=False)
        assert core._reference_mailbox.events.empty() and core._reference_mailbox.events.unfinished_tasks == 0
        child.close(timeout=0)
        assert core._reference_mailbox.events.qsize() == 1
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(child.object_id) is ObjectCollectionState.COLLECTED
        assert not core._recovery.reconstruction_snapshot(child.object_id).is_put
        assert core._recovery.lineage_for_object(child.object_id) is None
        assert not core._objects and not core._object_gc_obligations
        assert not core._task_finish_barriers and not core._protocol_unresolved and core._accepted_task_count == 0
        assert len(core._reference_mailbox.releases) == 1
        assert core._reference_mailbox.events.empty() and core._reference_mailbox.events.unfinished_tasks == 0
        count = core._submissions.qsize()
        assert count <= 4
        for _ in range(count):
            item = core._submissions.get_nowait()
            try:
                assert item is _WAKE_COORDINATOR
            finally:
                core._submissions.task_done()
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0

    def close(self):
        from tests.unit._pure_core import close_pure_core

        # Failure cleanup never deletes a durable export obligation to make
        # an incomplete retry look clean. Only release the actual Python ref.
        self.child.close(timeout=0)
        close_pure_core(self.core)


@pytest.mark.unit
@pytest.mark.usefixtures("_no_generic_export_runtime")
def test_shutdown_sync_retry_keeps_export_obligation_until_tombstone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generic export's real shutdown GC precheck, not public shutdown."""
    f = _GenericExportRetry(monkeypatch)
    core, child, token, key = f.core, f.child, f.token, f.key
    try:
        assert not core.request_export_pin_release(child.object_id, token)
        assert key in core._export_pin_release_obligations
        obligation = core._export_pin_release_obligations[key]
        assert obligation.retry_scheduled and obligation.retry_round == 1
        assert len(f.events) == 1 and f.calls == [key]
        assert not core.owner_table.contained_release_was_seen(child.object_id, token)
        assert not core._retry_gc_obligations_for_shutdown()
        assert core._export_pin_release_obligations[key] is obligation
        assert not obligation.retry_scheduled and obligation.retry_round == 1
        assert core.owner_table.snapshot(child.object_id) == f.before
        assert len(f.events) == 1 and f.calls == [key, key]
        assert not core.can_finalize_shutdown(require_distributed_clean=False)

        f.available = True
        assert core._retry_gc_obligations_for_shutdown()
        assert key not in core._export_pin_release_obligations
        assert core.owner_table.contained_release_was_seen(
            child.object_id, token
        )
        assert f.calls == [key, key, key]
        # The synchronous precheck won. The original scheduled event still
        # exists, but its round claim can no longer repeat the owner release.
        f.take(0, claimed=False)
        assert f.calls == [key, key, key] and len(f.events) == 1
        f.collect()
    finally:
        f.close()


@pytest.mark.unit
@pytest.mark.usefixtures("_no_generic_export_runtime")
def test_stale_retry_event_cannot_duplicate_a_newer_export_release_round(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Generic export retry-round fencing with actual final convergence."""
    f = _GenericExportRetry(monkeypatch)
    core, child, token, key = f.core, f.child, f.token, f.key
    try:
        assert not core.request_export_pin_release(child.object_id, token)
        obligation = core._export_pin_release_obligations[key]
        first_round = obligation.retry_round
        assert first_round == 1 and obligation.retry_scheduled
        first_event = f.take(0, claimed=True, released=False)
        assert first_event.scheduled_round == first_round
        second_round = obligation.retry_round
        assert second_round == 2 and second_round > first_round
        assert core._export_pin_release_obligations[key] is obligation
        assert core.owner_table.snapshot(child.object_id) == f.before
        assert f.calls == [key, key] and len(f.events) == 2
        assert obligation.retry_scheduled
        assert not core._claim_export_pin_release_retry(key, first_round)
        assert obligation.retry_round == second_round and obligation.retry_scheduled
        assert f.calls == [key, key]
        assert not core.can_finalize_shutdown(require_distributed_clean=False)
        f.available = True
        second_event = f.take(1, claimed=True, released=True)
        assert second_event.scheduled_round == second_round
        assert f.calls == [key, key, key] and not core._export_pin_release_obligations
        assert not core._claim_export_pin_release_retry(key, second_round)
        f.collect()
    finally:
        f.close()


class _LiveExportDrain:
    """One generic export pin; real Core construction/drain/finalization.

    Exactly three Core threads, one tiny put, no submitted Task, Worker server,
    Node, socket or Timer. The Worker is only the existing drain composition.
    A saved real retry event is delivered to the actual reference consumer
    after synchronous drain wins; this is not a Timer-race or publication test.
    Normal waits/joins are <=1s, failure cleanup shares 2s, and one exact L1
    invocation needs the external 30s runner for internal locks/teardown.
    """

    def __init__(self, monkeypatch):
        import math
        import multiprocessing.process
        import socket
        import subprocess
        from miniray import core as core_module, node as node_module, transport
        from miniray import worker as worker_module
        from miniray.trace import EventSink

        self.core = object.__new__(CoreWorker)
        self.child = self.worker = self.key = None
        self.constructing = self.constructed = self.cleaning = False
        self.main_thread = threading.current_thread()
        self.baseline = self.core_threads()
        self.lock = threading.Lock()
        self.created, self.started, self.joins = [], [], []
        self.violations, self.thread_errors, self.shutdown_errors = [], [], []
        self.releases, self.scheduled, self.claims, self.shutdowns = [], [], [], []
        self.available, self.claim_seen = threading.Event(), threading.Event()
        self.sink = EventSink()
        self.sink_closes = []
        monkeypatch.setattr(self.sink, "close", lambda: self.sink_closes.append(True))
        real_thread, real_constructor = threading.Thread, CoreWorker.__init__
        real_shutdown = self.core.shutdown
        probe = self

        class ObservedThread(real_thread):
            def __init__(thread, *args, **kwargs):
                super().__init__(*args, **kwargs)
                with probe.lock:
                    probe.created.append(thread)
                    valid = (probe.constructing and len(probe.created) <= 3
                             and thread.name in {
                                 "miniray-core-reference-events",
                                 "miniray-core-worker-coordinator",
                                 "miniray-core-worker-dispatch-0",
                             }
                             and sum(item.name == thread.name for item in probe.created) == 1)
                if not valid:
                    probe.forbidden("unplanned thread construction")

            def start(thread):
                if thread not in probe.created or thread in probe.started:
                    probe.forbidden("unowned/repeated thread start")
                probe.started.append(thread)  # track before a fallible start
                super().start()

            def join(thread, timeout=None):
                if (thread not in probe.created or type(timeout) not in (int, float)
                        or not math.isfinite(timeout)
                        or not 0 <= timeout <= (2.0 if probe.cleaning else 1.0)):
                    probe.forbidden("unowned/unbounded thread join")
                probe.joins.append((thread, timeout))
                if len(probe.joins) > 20:
                    probe.forbidden("too many thread joins")
                return super().join(timeout)

            def run(thread):
                try:
                    return super().run()
                except BaseException as exc:
                    with probe.lock:
                        probe.thread_errors.append((thread, exc))

        def constructor(core, *args, **kwargs):
            if core is not probe.core or not probe.constructing:
                probe.forbidden("unplanned Core construction")
            return real_constructor(core, *args, **kwargs)

        def shutdown(timeout=6.0, *, preserve_owner_protocol=False):
            # Worker deliberately catches Core errors and reports unclean.
            # Record those errors separately so missing fixture fields cannot
            # masquerade as the tested export obligation blocking drain.
            try:
                result = real_shutdown(timeout, preserve_owner_protocol=preserve_owner_protocol)
            except BaseException as exc:
                probe.shutdown_errors.append(exc)
                raise
            probe.shutdowns.append((preserve_owner_protocol, result))
            return result

        monkeypatch.setattr(threading, "Thread", ObservedThread)
        monkeypatch.setattr(CoreWorker, "__init__", constructor)
        monkeypatch.setattr(self.core, "shutdown", shutdown)
        for kind in (node_module.NodeServer, WorkerServer, transport.TCPServer):
            monkeypatch.setattr(kind, "__init__", self.forbidden)
        for name in ("socket", "socketpair", "create_connection"):
            monkeypatch.setattr(socket, name, self.forbidden)
        monkeypatch.setattr(subprocess, "Popen", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", self.forbidden)
        monkeypatch.setattr(multiprocessing.process.BaseProcess, "join", self.forbidden)
        monkeypatch.setattr(threading.Timer, "__init__", self.forbidden)
        monkeypatch.setattr(time, "sleep", self.forbidden)
        for module in (core_module, node_module, worker_module):
            monkeypatch.setattr(module, "rpc_request", self.forbidden)
        monkeypatch.setattr(transport, "request", self.forbidden)
        for name in ("_rpc", "_borrow_rpc", "_push_task_rpc", "_actor_call_rpc",
                     "_register_submission", "_execute", "create_actor", "create_placement_group"):
            monkeypatch.setattr(self.core, name, self.forbidden)
        monkeypatch.setattr(self.core, "_schedule_reference_event", self.save_retry)
        self.monkeypatch = monkeypatch

    @staticmethod
    def core_threads():
        return {thread for thread in threading.enumerate() if thread.name.startswith("miniray-core-")}

    def forbidden(self, *args, **kwargs):
        with self.lock:
            self.violations.append((threading.current_thread(), args, kwargs))
        raise AssertionError("live generic export drain attempted unplanned work")

    def construct(self):
        from miniray.ref_transfer import ReferenceExportSession

        self.constructing = True
        try:
            CoreWorker.__init__(
                self.core, ("export-drain-node.invalid", 1), NodeID.random(),
                owner_address=("export-drain-owner.invalid", 1), event_sink=self.sink,
                inline_threshold=1024, dispatch_lanes=1, gcs_address=None, poll_node_deaths=False,
            )
            self.constructed = True
        finally:
            self.constructing = False
        core = self.core
        assert set(self.created) == {core._reference_thread, core._coordinator, core._dispatcher}
        assert self.started == [core._reference_thread, core._dispatcher, core._coordinator]
        assert all(thread.is_alive() for thread in self.started)
        self.child = child = core.put("generic-export-drain-child")
        with ReferenceExportSession(
            core.worker_id, core.owner_address, pin=core.owner_table.add_contained_reference,
            unpin=core.request_export_pin_release,
        ) as session:
            payload = cloudpickle.dumps({"child": child})
            assert len(payload) <= 1024 and session.exported_count == 1
            self.token = session._exports[0][3]
            assert session.commit() == ()
        self.key = child.object_id, self.token
        self.before = core.owner_table.snapshot(child.object_id)
        assert self.before.contained_tokens == frozenset({self.token})
        assert len(self.before.local_tokens) == 1
        real_release = core.owner_table.release_contained_reference
        real_claim = core._claim_export_pin_release_retry

        def release(object_id, token):
            if (object_id, token) != self.key or len(self.releases) >= (5 if self.cleaning else 3):
                self.forbidden("extra or rebound export release")
            caller = threading.current_thread()
            if caller is not self.main_thread:
                self.forbidden("unexpected asynchronous export release")
            if not self.available.is_set():
                snapshot = core.owner_table.snapshot(object_id)
                assert snapshot.contained_tokens == frozenset({self.token})
                assert not core.owner_table.contained_release_was_seen(object_id, token)
                self.releases.append((caller, None))
                raise RuntimeError("export owner table temporarily unavailable")
            result = real_release(object_id, token)
            self.releases.append((caller, result))
            return result

        def claim(key, round_number):
            result = real_claim(key, round_number)
            if (threading.current_thread() is not core._reference_thread
                    or key != self.key or round_number != 1 or self.claims):
                self.forbidden("unexpected retry claim delivery")
            self.claims.append((key, round_number, result))
            self.claim_seen.set()  # FIFO task_done follows in the actual loop
            return result

        self.monkeypatch.setattr(core.owner_table, "release_contained_reference", release)
        self.monkeypatch.setattr(core, "_claim_export_pin_release_retry", claim)
        self.monkeypatch.setattr(core, "put", self.forbidden)
        self.worker = worker = _worker(core)
        worker._embedded_core_drain_lock = threading.Lock()
        worker._lifecycle = threading.Condition(threading.RLock())
        worker._active_tasks, worker._push_obligations = 0, set()
        worker._prepared_output_replies, worker._accepted_pushes = {}, {}
        worker._accepting_tasks = worker._owner_retain_admission_open = True
        worker._drain_request_id, worker._request_timeout = None, 0.5

    def save_retry(self, mailbox, event, delay):
        from miniray.core import _RetryExportPinRelease

        assert mailbox is self.core._reference_mailbox and type(event) is _RetryExportPinRelease
        assert event.key == self.key and event.scheduled_round == 1 and delay == 0.01
        assert not self.scheduled
        self.scheduled.append(event)

    def assert_stopped(self):
        core = self.core
        assert len(self.created) == len(self.started) == 3
        assert all(thread.ident is not None and not thread.is_alive() for thread in self.created)
        assert self.core_threads() == self.baseline
        assert not self.violations and not self.thread_errors and not self.shutdown_errors
        assert self.sink_closes == [True] and core._sink_closed
        assert not core._accepting and not core._owner_protocol_open
        assert core._reference_mailbox.stop_enqueued and core._reference_mailbox.stopped.is_set()
        assert not core._reference_runtime_finalizer.alive
        assert core._reference_mailbox.events.empty() and core._reference_mailbox.events.unfinished_tasks == 0
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        assert core._ready_tasks.empty() and core._ready_tasks.unfinished_tasks == 0
        assert not core._gc_retry_timers_open and not core._gc_retry_timers
        assert not core._export_pin_release_obligations and not core._object_gc_obligations
        assert not core._objects and not core._protocol_unresolved and not core._task_finish_barriers
        assert core._accepted_task_count == 0

    def cleanup(self):
        from miniray.core import _STOP

        self.cleaning = True
        self.available.set()
        deadline = time.monotonic() + 2.0
        if self.child is not None:
            try:
                self.child.close(timeout=min(0.5, max(0.0, deadline - time.monotonic())))
            except TimeoutError:
                pass
        if self.constructed and any(thread.is_alive() for thread in self.created):
            try:
                self.core.shutdown(timeout=min(1.0, max(0.001, deadline - time.monotonic())))
            except Exception:
                pass
        if any(thread.is_alive() for thread in self.created):
            # Failure-only signals/join neither clear owner obligations nor
            # turn a failed drain into successful contract evidence.
            self.core._accepting = False
            gate = getattr(self.core, "_startup_threads_gate", None)
            if gate is not None:
                gate.set()
            for name in ("_submissions", "_ready_tasks"):
                mailbox = getattr(self.core, name, None)
                if mailbox is not None:
                    mailbox.put_nowait(_STOP)
            mailbox = getattr(self.core, "_reference_mailbox", None)
            if mailbox is not None:
                mailbox.stop()
            for thread in self.created:
                if thread.ident is not None:
                    thread.join(max(0.0, deadline - time.monotonic()))
        assert all(not thread.is_alive() for thread in self.created)
        finalizer = getattr(self.core, "_reference_runtime_finalizer", None)
        if finalizer is not None:
            finalizer.detach()
        if not self.sink_closes:
            self.sink.close()
        assert not self.violations and not self.thread_errors and not self.shutdown_errors
        assert self.core_threads() == self.baseline


@pytest.mark.loopback_smoke
def test_worker_drain_stays_unclean_while_export_release_cannot_converge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Actual Worker/Core drain, not a swallowed incomplete-fixture error."""
    from miniray.ownership import ObjectCollectionState

    f = _LiveExportDrain(monkeypatch)
    try:
        f.construct()
        core, child, worker = f.core, f.child, f.worker
        assert not core.request_export_pin_release(child.object_id, f.token)
        obligation = core._export_pin_release_obligations[f.key]
        assert obligation.retry_scheduled and obligation.retry_round == 1 and len(f.scheduled) == 1
        started = worker._handle_begin_drain(protocol.BeginDrain("drain-export"))
        assert started.drain_started and not started.clean
        status = worker._drain_status("drain-export", timeout=0.5)
        assert status.drain_started and not status.clean
        assert f.shutdowns == [(True, False)] and not f.shutdown_errors
        assert [result for _, result in f.releases] == [None, None]
        assert core._export_pin_release_obligations[f.key] is obligation
        assert core.owner_table.snapshot(child.object_id) == f.before
        assert not core.owner_table.contained_release_was_seen(child.object_id, f.token)
        assert not worker._embedded_core_stopped and core._owner_protocol_open
        assert core._reference_thread.is_alive() and core._reference_mailbox.accepting
        assert not core.can_finalize_shutdown(require_distributed_clean=True)

        # A clean preserve-owner drain closes external finalizer admission.
        # Close this real local source first, while its export pin still keeps
        # the object alive; local receipt is not the future export Release.
        child.close(timeout=0.5)
        waiting = core.owner_table.snapshot(child.object_id)
        assert not waiting.local_tokens and waiting.contained_tokens == frozenset({f.token})
        assert not waiting.collection_pending and not core.owner_table.contained_release_was_seen(child.object_id, f.token)
        f.available.set()
        clean = worker._drain_status("drain-export", timeout=1.0)
        assert clean.drain_started and clean.clean
        assert f.shutdowns == [(True, False), (True, True)] and not f.shutdown_errors
        assert [result for _, result in f.releases] == [None, None, True]
        assert not core._export_pin_release_obligations and not core._object_gc_obligations
        assert core.owner_table.collection_state(child.object_id) is ObjectCollectionState.COLLECTED
        assert core.owner_table.contained_release_was_seen(child.object_id, f.token)
        assert not core._recovery.reconstruction_snapshot(child.object_id).is_put
        assert not worker._embedded_core_stopped and worker._drain_clean
        assert core._owner_protocol_open and core._reference_thread.is_alive()
        assert not core._reference_mailbox.accepting and not core._sink_closed
        assert not core._coordinator.is_alive() and not core._dispatcher.is_alive()
        assert core.can_finalize_shutdown(require_distributed_clean=True)

        event, = f.scheduled
        assert core._reference_mailbox.enqueue_internal(event)
        assert f.claim_seen.wait(1.0)
        assert f.claims == [(f.key, 1, False)]
        assert [result for _, result in f.releases] == [None, None, True]
        # The real consumer's task_done follows the claim signal. Only this
        # actual FIFO stop/join certifies that no event remains unfinished.
        assert core.finalize_shutdown(require_distributed_clean=True, timeout=1.0)
        f.assert_stopped()
    finally:
        f.cleanup()
