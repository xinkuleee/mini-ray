"""Current owner-led single-output recovery facts and serial histories.

Seven pure cases compose actual Core/Node/handoff/journal/child reducers.
Node Complete is never inferred from owner history or a death fence. The two
serial orderings do not claim the retired two-thread race coverage; enhanced
INTENT/ARM and GCS frozen worksets are outside the base implementation.
"""

from dataclasses import fields, is_dataclass, replace
from enum import Enum
import threading

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import _HomeRoute
from miniray.core import _NodeDeathObserved, _ObjectWaiter, _OutputNodeLossObligation, _PendingTask, _WAKE_COORDINATOR
from miniray.ids import JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.output_handoff import OutputHandoffPhase
from miniray.output_publication_journal import OutputPublicationJournalState, OutputPublicationStage
from miniray.ownership import ObjectState, OutputOwnerPublicationConflictError
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_output_publication_node_server import _node


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _runtime_tripwires(monkeypatch):
    import multiprocessing.process
    import socket
    import subprocess
    import time
    from miniray import control, core as core_module, transport
    from miniray.core import CoreWorker
    from miniray.node import NodeServer
    from miniray.worker import WorkerServer

    def forbidden(*args, **kwargs):
        pytest.fail("pure recovery fact test attempted runtime or unmodelled work")

    for kind, method in ((CoreWorker, "__init__"), (NodeServer, "__init__"),
                         (WorkerServer, "__init__"), (control.GCSLite, "__init__"),
                         (threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Event, "wait"), (threading.Condition, "wait"),
                         (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
                         (multiprocessing.process.BaseProcess, "start"),
                         (multiprocessing.process.BaseProcess, "join")):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(core_module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)


def _metadata(value):
    if type(value) in (JobID, TaskID, LeaseID, NodeID, WorkerID):
        assert type(value.value) is bytes and len(value.value) == 16
        return
    assert not isinstance(value, (bytes, bytearray, memoryview, protocol.ResultDescriptor, protocol.TaskSpec))
    if value is None or isinstance(value, (str, int, bool, Enum)):
        return
    if is_dataclass(value) and not isinstance(value, type):
        for member in fields(value):
            _metadata(getattr(value, member.name))
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _metadata(item)
    else:
        raise AssertionError("unexpected metadata value: " + type(value).__name__)


class _Fixture:
    def __init__(self, *, refs=True):
        self.f, self.node, self.record, self.complete_request = _node(refs=refs, stored=False)
        self.values, self.manifest, self.identity = self.f.values, self.f.manifest, self.f.id
        self.core = core = make_pure_core()
        values = self.values
        core.job_id, core.worker_id, core.node_id = values.job, values.owner, values.node
        core.node_address, core.owner_address = ("node.invalid", 1), ("owner.invalid", 1)
        core._home_route = _HomeRoute(core.node_id, core.node_address, core._membership_epoch)
        spec = protocol.TaskSpec(values.job, values.task, values.attempt,
                                 protocol.FunctionKey(values.job, __name__, "producer", "1"),
                                 (), 1, ResourceVector({"CPU": 1}), values.owner, max_retries=1)
        core.owner_table.register_task_outputs(spec, local_tokens=("outer-live",))
        core._recovery.register_task(spec, max_retries=1)
        core._objects = {spec.return_ids()[0]: _ObjectWaiter(threading.Event())}
        self.pending = _PendingTask(spec.return_ids()[0], spec)
        with core._state_lock:
            core._install_task_finish_barrier_locked(self.pending)
            core._accepted_task_count += 1
            core._enqueue_reconstruction_task(self.pending)
        assert core._submissions.get_nowait() is self.pending
        core._submissions.task_done()
        self.record.request = replace(self.record.request, requester_owner_address=core.owner_address)
        self.handoffs = self.f.handoffs = core._output_handoff_table()
        self.owner_calls, self.finalize_calls = [], []
        self.node._background_rpc = self.background_rpc
        self.node._output_publications = self.f.adapter = self.node._make_output_publication_adapter()
        core._borrow_rpc = self.borrow_rpc
        incarnation = self.manifest.header.node_incarnation
        self.publisher_death = protocol.NodeDeathRecord(
            "inline-publisher-exit", incarnation.node_id, incarnation.node_pid,
            incarnation.registration_epoch, 9, 1, protocol.NodeDeathReason.PROCESS_EXIT,
            "confirmed publisher death supplied at membership boundary",
        )
        self.owner_death = protocol.WorkerDeathRecord(
            "inline-owner-exit", protocol.WorkerIncarnation(
                NodeID(bytes((17,)) * 16), 1702, 3, values.owner, 1901,
            ), 10, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
        )
        self.fence = protocol.InstallOwnerDeathFence("inline-owner-fence", self.owner_death, values.node)
        self.finalize = wire.FinalizeOutputOwnerDeath(self.manifest, self.owner_death)
        self.prepare_request = wire.PrepareOutputPublication(self.manifest, (values.payload))

    def background_rpc(self, address, handler, request):
        assert not self.node._state_lock._is_owned() and not self.f.journal._lock._is_owned()
        owner_handlers = {
            wire.REGISTER_OUTPUT_HANDOFF_HANDLER: self.core.register_output_handoff,
            wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER: self.core.report_output_handoff_complete,
            wire.REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER: self.core.report_output_handoff_rollback,
        }
        if handler in owner_handlers:
            assert address == self.core.owner_address
            self.owner_calls.append(request)
            return owner_handlers[handler](request)
        if handler == "prepare_stored_contained_pin":
            return self.f.prepare_child(address, request)
        if handler == "promote_stored_contained_pin":
            return self.f.promote_child(address, request)
        if handler == "release_contained_reference":
            return self.f.release_child(address, request)
        assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER
        assert address == self.record.grant.worker_address and request == self.finalize
        self.finalize_calls.append(request)
        # Only Worker source-custody ACK is doubled; Node/child cleanup is real.
        return wire.FinalizeOutputOwnerDeathReply(request, True)

    def borrow_rpc(self, address, handler, request):
        assert not self.core._state_lock._is_owned()
        assert handler == "release_contained_reference"
        return self.f.release_child(address, request)

    def prepare(self):
        assert self.node._handle_prepare_output_publication(self.prepare_request).accepted

    def complete(self):
        reply = self.node._handle_complete_worker_lease_inner(self.complete_request)
        assert reply.accepted and reply.released
        assert reply.output_publication == self.values.envelope
        return reply.output_publication

    def children(self):
        return tuple(self.f.child_owners[t.contained_owner_worker_id].snapshot(t.contained_object_id)
                     for t in (self.manifest.value).transfers)

    def install_publisher_death(self):
        snapshot = protocol.InstallClusterSnapshot(self.publisher_death.death_epoch, "inline-no-survivor", ())
        removal = self.core.handle_node_death(self.publisher_death, snapshot)
        assert removal.lost == removal.surviving == removal.collecting == ()
        observed = self.core._submissions.get_nowait()
        self.core._submissions.task_done()
        assert observed == _NodeDeathObserved(self.publisher_death, snapshot.membership_epoch)
        self.core._classify_node_death(observed)
        assert self.core._submissions.get_nowait() is _WAKE_COORDINATOR
        self.core._submissions.task_done()
        assert self.core._submissions.empty()
        assert self.core._dead_nodes == {self.values.node: self.publisher_death}

    def close(self):
        assert self.core.owner_table.release_local_reference(self.pending.object_id, "outer-live")
        close_pure_core(self.core)


def test_owner_handoff_is_complete_pre_effect_metadata_and_exact_replay(monkeypatch):
    f = _Fixture()
    try:
        child_before = f.children()
        owner_before = f.core.owner_table.snapshot(f.pending.object_id)
        receipts = []
        real_rpc = f.node._background_rpc

        def lose_registration_ack(address, handler, request):
            reply = real_rpc(address, handler, request)
            if handler == wire.REGISTER_OUTPUT_HANDOFF_HANDLER:
                receipts.append(reply)
                assert reply.accepted and reply.snapshot.manifest == f.manifest
                assert reply.snapshot.complete is None and reply.snapshot.phase is OutputHandoffPhase.PENDING
                assert f.children() == child_before and f.f.store.used_bytes == 0
                assert f.core.owner_table.snapshot(f.pending.object_id) == owner_before
                journal = f.f.journal.snapshot(f.identity)
                assert len(journal.intents) == 1 and journal.intents[0].stage is OutputPublicationStage.OWNER_REGISTER
                assert journal.acknowledgements == () and journal.retained_result_slots == ()
                _metadata((reply, journal))
                if len(receipts) == 1:
                    raise TimeoutError("owner registration applied before ACK loss")
            return reply

        monkeypatch.setattr(f.node, "_background_rpc", lose_registration_ack)
        with pytest.raises(TimeoutError, match="registration applied"):
            f.prepare()
        historical = f.handoffs.query(f.identity)
        assert historical == receipts[0].snapshot
        f.prepare()
        assert receipts[1] == receipts[0] and len(receipts) == 2
        assert f.f.journal.materialized_result(f.identity).inline_data == (f.values.payload)
        for transfer, child in zip((f.manifest.value).transfers, f.children()):
            assert transfer.final_hold in child.contained_holds
        envelope = f.complete()
        assert historical.complete is None and f.handoffs.query(f.identity).complete is None
        assert f.node._drive_output_publications()
        current = f.handoffs.query(f.identity)
        assert current.complete == envelope.complete and historical.complete is None
        assert f.core.report_output_handoff_complete(wire.ReportOutputHandoffComplete(envelope.complete)).snapshot == current
        assert f.record.state is protocol.LeaseExecutionState.COMPLETED
        assert f.f.ledger.available == ResourceVector({"CPU": 1})
        _metadata((historical, current, f.f.journal.snapshot(f.identity)))
    finally:
        f.close()


@pytest.mark.parametrize("owner_first", (False, True))
def test_owner_and_publisher_fences_preserve_independent_facts(owner_first):
    f = _Fixture()
    try:
        f.prepare()
        envelope = f.complete()  # Local success exists; its owner report is still pending.
        historical = f.handoffs.query(f.identity)
        journal_before = f.f.journal.snapshot(f.identity)
        child_before = f.children()
        assert historical.complete is None and journal_before.complete == envelope.complete
        assert f.owner_death.incarnation.node_id != f.publisher_death.node_id
        if owner_first:
            fence_reply = f.node._handle_install_owner_death_fence(f.fence)
            assert fence_reply.accepted and not f.core._dead_nodes
            f.install_publisher_death()
        else:
            f.install_publisher_death()
            assert not f.node._owner_death_fences
            fence_reply = f.node._handle_install_owner_death_fence(f.fence)
        assert fence_reply.accepted
        assert f.node._owner_death_fences[f.values.owner] == f.owner_death
        assert f.f.journal.snapshot(f.identity) == journal_before and f.children() == child_before
        assert f.handoffs.query(f.identity) == historical
        assert f.node._handle_install_owner_death_fence(f.fence) == fence_reply
        conflict = f.node._handle_install_owner_death_fence(replace(
            f.fence, owner_death=replace(f.owner_death, detection_id="another-death"),
        ))
        assert not conflict.accepted and f.node._owner_death_fences[f.values.owner] == f.owner_death
        assert not f.node._handle_prepare_output_publication(f.prepare_request).accepted
        assert not f.core.register_output_handoff(wire.RegisterOutputHandoff(f.manifest)).accepted
        # These are independent local histories, not permission to send cleanup
        # to a dead Node. The live-Node owner cleanup is exercised below.
        assert f.f.journal.snapshot(f.identity) == journal_before
        assert f.f.journal.materialized_result(f.identity).inline_data == (f.values.payload)
        assert f.children() == child_before and f.finalize_calls == []
        assert f.record.state is protocol.LeaseExecutionState.COMPLETED
        assert f.handoffs.query(f.identity) == historical and historical.complete is None
        _metadata((historical, journal_before, f.fence, fence_reply))
    finally:
        f.close()


@pytest.mark.parametrize("owner_first", (False, True))
def test_owner_fence_and_prepare_have_two_serial_admission_histories(owner_first):
    f = _Fixture()
    try:
        before = f.children()
        if not owner_first:
            f.prepare()
            assert f.f.journal.materialized_result(f.identity).inline_data == (f.values.payload)
            assert any(child.contained_holds for child in f.children())
        assert f.node._handle_install_owner_death_fence(f.fence).accepted
        assert not f.node._handle_prepare_output_publication(f.prepare_request).accepted
        if owner_first:
            assert f.f.journal.publication_ids() == () and f.handoffs.snapshots() == ()
            assert f.children() == before and f.f.events == []
            assert f.record.output_publication_id is None
        else:
            historical = f.handoffs.query(f.identity)
            assert historical.manifest == f.manifest and historical.complete is None
            assert f.node._handle_finalize_output_owner_death(f.finalize).cleaned
            retired = f.f.journal.snapshot(f.identity)
            assert retired.complete is None and retired.state is OutputPublicationJournalState.RETIRED
            assert retired.rollback_tombstone is None and retired.retained_result_slots == ()
            assert f.record.state is protocol.LeaseExecutionState.ABANDONED
            assert f.f.ledger.available == ResourceVector({"CPU": 1})
            assert f.handoffs.query(f.identity) == historical
            assert f.node._handle_finalize_output_owner_death(f.finalize).cleaned
            assert f.finalize_calls == [f.finalize]
            f.f.assert_no_pins_or_bytes()
            _metadata((historical, retired))
        assert not f.node._handle_prepare_output_publication(f.prepare_request).accepted
        assert f.f.store.used_bytes == 0
        assert f.core.owner_table.snapshot(f.pending.object_id).state is ObjectState.PENDING
        assert f.core._recovery.task_record(f.pending.task_id).retries_started == 0
    finally:
        f.close()


@pytest.mark.parametrize("keep", (False, True))
def test_owner_keep_drop_serial_choice_is_immutable_and_metadata_only(keep):
    f = _Fixture(refs=False)
    try:
        f.prepare()
        envelope = f.complete()
        historical = f.handoffs.query(f.identity)
        journal_before = f.f.journal.snapshot(f.identity)
        assert historical.complete is None and journal_before.complete == envelope.complete
        f.install_publisher_death()
        obligation = _OutputNodeLossObligation(f.identity, f.publisher_death, envelope if keep else None)
        assert f.core._execute(f.pending, f.pending.spec, output_node_loss=obligation) is keep
        receipt = f.core.owner_table._output_loss_receipts[f.identity]
        assert receipt.keep is keep and receipt.complete == (envelope.complete if keep else None)
        owner = f.core.owner_table.snapshot(f.pending.object_id)
        record = replace(f.core._recovery.task_record(f.pending.task_id))
        assert owner.local_tokens == frozenset({"outer-live"})
        assert owner.state is (ObjectState.READY_INLINE if keep else ObjectState.PENDING)
        assert owner.inline_data == ((f.values.payload) if keep else None)
        assert record.state is (TaskState.SUCCEEDED if keep else TaskState.RETRY_PENDING)
        assert record.retries_started == (0 if keep else 1)
        assert historical.complete is None and f.f.journal.snapshot(f.identity) == journal_before
        current = f.handoffs.query(f.identity)
        assert current.phase is (OutputHandoffPhase.ADOPTED if keep else OutputHandoffPhase.ABORTED)
        assert current.complete == (envelope.complete if keep else None)
        assert not f.core.owner_table.resolve_output_node_loss(
            f.manifest, receipt, envelope if keep else None,
        )
        opposite = replace(receipt, keep=not keep, complete=envelope.complete)
        with pytest.raises(OutputOwnerPublicationConflictError, match="rebound"):
            f.core.owner_table.resolve_output_node_loss(f.manifest, opposite, envelope if not keep else None)
        assert f.core.owner_table.snapshot(f.pending.object_id) == owner
        assert f.core._recovery.task_record(f.pending.task_id) == record
        assert f.core.owner_table._output_loss_receipts[f.identity] == receipt
        assert f.handoffs.query(f.identity) == current
        _metadata((historical, current, journal_before, receipt, opposite))
        assert f.f.journal.materialized_result(f.identity).inline_data == (f.values.payload)
    finally:
        f.close()
