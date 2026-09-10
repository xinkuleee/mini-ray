"""Bounded synchronous Node/owner/child-owner publication handlers.

No Node constructor, sockets, worker process, background thread or waits.
One real resource ledger, one tiny result and at most two child transfers.
"""

from dataclasses import replace
from types import SimpleNamespace
import socket
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol, enhanced_publication as ep, control
from miniray.enhanced_publication_control import EnhancedPublicationControl
from miniray.owner_death_fence_registry import OwnerDeathFenceRegistry
from miniray.ids import NodeID
from miniray.node import NodeServer, _LeaseRecord, _WorkerSlot
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_handoff import OutputHandoffTable, OutputHandoffPhase
from miniray.output_publication import OutputPublicationCompleteWitness, OutputPublicationEnvelope, OutputPublicationManifest
from miniray.output_publication_journal import OutputPublicationJournal, OutputPublicationJournalStateError
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.ownership import ObjectOwnerTable, OutputOwnerPublicationPlan
from miniray.publication_sources import BorrowedContainedSource
from miniray.resources import AllocationToken, ResourceLedger
from miniray.output_publication_journal import OutputPublicationAdoptionProof, OutputPublicationJournalState
from miniray.publication_gate import OutputPublicationGatePhase, OutputPublicationGateConfig, GraphReservationOutcome
from miniray.resources import NodeSnapshot, ResourceVector



from tests.unit.test_output_publication import _Fixture as _Values


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure Node handler attempted runtime work")
    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Event, "wait", forbidden)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)



class _Fixture:
    """Node journal/store, actual GCS authority, and real owner/child reducers."""

    def __init__(self, *, refs=True, stored=True, reverse_children=False):
        self.values = _Values(refs=refs, stored=stored)
        if reverse_children:
            assert refs
            slot = (self.values.value)
            (self.values.value) = (replace(slot, transfers=tuple(reversed(slot.transfers))))
            self.values.manifest = OutputPublicationManifest.create(self.values.header, (self.values.value))
            self.values.witness = OutputPublicationCompleteWitness.for_manifest(self.values.manifest)
            self.values.envelope = OutputPublicationEnvelope(
                self.values.manifest, self.values.witness, (self.values.result))
        self.manifest, self.id = self.values.manifest, self.values.publication_id
        self.journal, self.handoffs = OutputPublicationJournal(), OutputHandoffTable()
        self.authority = ep.PublicationAuthority()
        self.gcs = None
        self.owner_address = ("owner.invalid", 1)
        self.publication = ep.TaskPublication(self.manifest, self.owner_address)
        self.gcs_calls = []
        self.store, self.child_owners = ObjectStore(1024), {}
        self.events, self.fault = [], None
        self.ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        self.token = AllocationToken("test-output-lease")
        self.ledger.allocate(ResourceVector({"CPU": 1}), self.token)
        for transfer in (self.manifest.value).transfers:
            table = self.child_owners.setdefault(transfer.contained_owner_worker_id, ObjectOwnerTable())
            table.register(transfer.contained_object_id, local_token="source-live")
            if isinstance(transfer.source, BorrowedContainedSource):
                source = transfer.source.original_source
                root_borrower = (self.values.executor, "upstream-root")
                table.add_borrowed_reference(transfer.contained_object_id, root_borrower)
                table.retain_borrowed_reference_for_task(transfer.contained_object_id, root_borrower, source.hold)
                table.acquire_exported_reference(transfer.contained_object_id, source, transfer.source.owner_table_token)
        self.adapter = OutputPublicationNodeAdapter(self.journal,
            register_owner=self.register_owner, report_complete=self.report_complete,
            report_rollback=self.report_rollback, prepare_child=self.prepare_child,
            publication_value=lambda manifest: ep.TaskPublication(manifest, self.owner_address),
            publication_rpc=self.publication_rpc, abort_owner=self.abort_owner,
            promote_child=self.promote_child, release_child=self.release_child,
            seal_replica=self.unbound_storage, drop_replica=self.unbound_storage)

    def publication_rpc(self, request):
        reply = self.authority.apply(request) if self.gcs is None else self.gcs.enhanced_publication(request)
        assert type(reply) is (ep.PublicationReply if type(request) is ep.GetPublication or not reply.accepted else ep.PublicationStageAck) and reply.request == request
        self.gcs_calls.append((request, reply))
        return reply

    def observe_gcs(self, sink):
        """Bind actual GCS stage emission to the already-used authority."""
        gcs = object.__new__(control.GCSLite)
        gcs.nodes = control.NodeRegistry()
        incarnation = self.manifest.header.node_incarnation
        # The immutable fixture incarnation is epoch two. Populate it using
        # genuine registrations rather than assigning a fabricated epoch.
        assert incarnation.registration_epoch == 2
        assert gcs.nodes.register_message(protocol.RegisterNode(
            NodeID(b'z' * 16), 1700, ("unused-node.invalid", 1), ResourceVector({"CPU": 1}))).accepted
        registered = gcs.nodes.register_message(protocol.RegisterNode(
            incarnation.node_id, incarnation.node_pid, ("node.invalid", 1), ResourceVector({"CPU": 1})))
        assert registered.accepted and registered.registration_epoch == incarnation.registration_epoch
        gcs.workers = control.WorkerRegistry(gcs.nodes)
        assert gcs.workers.register(protocol.RegisterWorkerIncarnation(protocol.WorkerIncarnation(
            incarnation.node_id, incarnation.node_pid, incarnation.registration_epoch,
            self.values.executor, 1801))).accepted
        gcs.owner_death_fences = OwnerDeathFenceRegistry()
        gcs._owner_death_control_lock = threading.RLock()
        gcs.event_sink = sink
        gcs.publication_control = EnhancedPublicationControl(
            nodes=gcs.nodes, workers=gcs.workers, owner_fences=gcs.owner_death_fences,
            lock=gcs._owner_death_control_lock, rpc=self.unbound_storage, authority=self.authority)
        self.gcs = gcs

    def abort_owner(self, publication, scope):
        assert publication == self.publication and scope == self.journal.rollback_scope(self.id)
        current = self.handoffs.query(self.id)
        assert current.complete is current.adoption is None
        aborted = self.handoffs.abort_manifest(self.manifest, "node rollback:" + scope.rollback_id)
        assert aborted.phase is OutputHandoffPhase.ABORTED
        return ep.OwnerAbortReceipt(publication.reference, self.values.owner, scope.rollback_id)

    def adopt_from_owner_table(self, proof):
        # This Node-only boundary supplies an actual owner CAS receipt. It
        # does not claim a scheduled Core/Worker path or fabricate GCS C7.
        values = self.values
        spec = protocol.TaskSpec(values.job, values.task, values.attempt,
            protocol.FunctionKey(values.job, __name__, "adoption", "v1"),
            (), 1, ResourceVector({"CPU": 1}), values.owner)
        owner = ObjectOwnerTable()
        owner.register_task_outputs(spec, local_tokens=("owner-live",))
        assert owner.commit_output_publication(OutputOwnerPublicationPlan(values.execution, values.envelope)).committed
        self.handoffs.record_complete(proof.complete)
        self.handoffs.adopt(proof)
        for request in (ep.RecordTerminal(proof.complete), ep.CommitGraph(self.publication.reference), ep.RecordAdoption(proof)):
            reply = self.publication_rpc(request)
            assert reply.accepted, reply.error
        return reply.receipt

    def unbound_storage(self, *_args):
        pytest.fail("Node storage must be explicitly bound before effects")

    def hit(self, stage):
        self.events.append(stage)
        if self.fault == stage:
            self.fault = None
            raise TimeoutError("injected effect-then-lost-ACK: " + stage)

    def register_owner(self, manifest):
        request = wire.RegisterOutputHandoff(manifest)
        snapshot = self.handoffs.register(manifest, self.id.attempt_id)
        reply = wire.OutputHandoffReply(request, True, snapshot)
        assert reply.request == request and reply.snapshot.manifest == manifest
        self.hit("owner-register")

    def report_complete(self, witness):
        request = wire.ReportOutputHandoffComplete(witness)
        snapshot = self.handoffs.record_complete(request.witness)
        reply = wire.OutputHandoffCompleteAck(snapshot.complete, True)
        assert reply.accepted and reply.witness == request.witness
        self.hit("complete-report")

    def report_rollback(self, tombstone, *, manifest):
        request = wire.ReportOutputHandoffRollback(manifest, tombstone)
        assert tombstone == self.journal.snapshot(self.id).rollback_tombstone
        current = self.handoffs.query(self.id)
        assert current.phase is OutputHandoffPhase.ABORTED
        snapshot = self.handoffs.abort_manifest(manifest, current.abort_reason)
        reply = wire.OutputHandoffReply(request, True, snapshot)
        assert reply.request == request and snapshot.phase is OutputHandoffPhase.ABORTED
        self.hit("rollback-report")

    def prepare_child(self, address, request):
        assert address == request.transfer.contained_owner_address
        result = self.child_owners[request.authority_worker_id].prepare_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id)
        self.hit("prepare")
        return protocol.StoredContainedPinReply(request, result)

    def promote_child(self, address, request):
        assert address == request.transfer.contained_owner_address
        result = self.child_owners[request.authority_worker_id].promote_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id)
        self.hit("promote")
        return protocol.StoredContainedPinReply(request, result)

    def release_child(self, address, request):
        assert address in (("127.0.0.1", 30101), ("127.0.0.1", 30102))
        released = self.child_owners[request.owner_worker_id].release_contained_reference(request.object_id, request.hold)
        self.hit("release")
        return protocol.ReleaseContainedReferenceReply(request.object_id, request.owner_worker_id, request.hold, True, released)

    def prepare(self):
        self.adapter.prepare(self.manifest, (self.values.payload))

    def assert_no_pins_or_bytes(self):
        assert self.store.used_bytes == 0
        assert self.journal.snapshot(self.id).result_retained is False
        for transfer in (self.manifest.value).transfers:
            snapshot = self.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id)
            assert transfer.provisional_hold not in snapshot.contained_holds
            assert transfer.final_hold not in snapshot.contained_holds


def _bind_real_node_storage(fixture):
    node = object.__new__(NodeServer)
    incarnation = fixture.manifest.header.node_incarnation
    node.node_id, node._node_pid, node._registration_epoch = incarnation.node_id, incarnation.node_pid, incarnation.registration_epoch
    node._state_lock = threading.RLock()
    node._object_store = fixture.store
    node._object_manager = ObjectManager(node.node_id, fixture.store)
    node._sealed_metadata, node._dropped_metadata = {}, {}
    node._local_replica_write_claims, node._object_localization_locks = {}, {}
    node._owner_death_fences = {}
    node._output_publication_journal = fixture.journal
    fixture.adapter._seal_replica = node._seal_output_publication_replica
    fixture.adapter._drop_replica = node._drop_output_publication_replica
    return node

def _node(*, refs=True, stored=True, reverse_children=False):
    fixture = _Fixture(refs=refs, stored=stored, reverse_children=reverse_children)
    node = _bind_real_node_storage(fixture)
    values = fixture.values
    node._ledger = fixture.ledger
    node._cluster_nodes = (NodeSnapshot(values.node, ResourceVector({"CPU": 1}), ResourceVector()),)
    node._cluster_addresses = {}
    node._gcs_address = ("gcs.invalid", 1)
    node._registered_with_gcs = False
    node._resource_report_version = 0
    node._resource_reported_version = 0
    node._dependency_pin_cleanups = {}
    node._pinned_transfers = {}
    node._actor_workers = {}
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node.event_sink = None
    request = protocol.RequestWorkerLease(
        values.lease, values.task, values.attempt, ResourceVector({"CPU": 1}),
        values.node, values.owner, return_ids=((values.publication_id.object_id,)),
    )
    grant = protocol.GrantWorkerLease(
        values.lease, values.task, values.attempt, values.node, values.executor,
        ("worker.invalid", 1), fixture.token,
    )
    record = _LeaseRecord(request, fixture.token, grant, state=protocol.LeaseExecutionState.RUNNING)
    node._leases = {values.lease: record}
    node._workers = {values.executor: _WorkerSlot(
        values.executor, process=SimpleNamespace(is_alive=lambda: True),
        address=grant.worker_address, pid=1801, active_lease_id=values.lease,
    )}
    node._worker_order = (values.executor,)
    node._output_publications = fixture.adapter
    complete = protocol.CompleteWorkerLease(
        values.lease, values.task, values.attempt, values.executor,
        protocol.TaskReplyStatus.SUCCEEDED,
    )
    return fixture, node, record, complete


@pytest.mark.parametrize("refs,stored", ((True, True), (False, True), (True, False)))
def test_handlers_complete_single_output_locally_without_owner_complete_rpc(refs, stored):
    fixture, node, record, complete = _node(refs=refs, stored=stored)
    prepared = node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload)))
    assert prepared.accepted
    assert record.output_publication_id == fixture.id
    reply = node._handle_complete_worker_lease_inner(complete)
    assert reply.accepted and reply.released
    assert reply.output_publication == fixture.values.envelope
    assert (reply.output_publication.result == fixture.values.result)
    assert (reply.output_publication.result.object_id) == (fixture.id.object_id)
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    assert fixture.handoffs.query(fixture.id).complete is None
    replay = node._handle_complete_worker_lease_inner(complete)
    assert replay.accepted and not replay.released
    assert replay.output_publication == reply.output_publication
    query = protocol.GetWorkerLeaseOutcome(
        fixture.values.lease, fixture.values.task, fixture.values.attempt, fixture.values.executor,
        fixture.values.owner, ((fixture.id.object_id,)),
    )
    outcome = node._handle_get_worker_lease_outcome(query)
    assert outcome.output_publication == reply.output_publication
    assert len(outcome.descriptors) == int(stored)
    assert node._drive_output_publications()
    assert fixture.handoffs.query(fixture.id).complete == fixture.values.witness


def test_failure_complete_releases_cpu_but_withholds_ack_until_all_compensation():
    fixture, node, record, complete = _node()
    assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))).accepted
    failed = replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    first = node._handle_complete_worker_lease_inner(failed)
    assert not first.accepted
    assert record.state is protocol.LeaseExecutionState.COMPLETED
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    query = protocol.GetWorkerLeaseOutcome(
        fixture.values.lease, fixture.values.task, fixture.values.attempt, fixture.values.executor,
        fixture.values.owner, ((fixture.id.object_id,)),
    )
    unavailable = node._handle_get_worker_lease_outcome(query)
    assert unavailable.found and unavailable.state is protocol.LeaseExecutionState.COMPLETED
    assert unavailable.completion_status is protocol.TaskReplyStatus.SYSTEM_ERROR and unavailable.cleanup_pending
    for _ in range(12):
        reply = node._handle_complete_worker_lease_inner(failed)
        if reply.accepted:
            break
    else:
        pytest.fail("bounded rollback did not consume its effects")
    fixture.assert_no_pins_or_bytes()
    assert fixture.handoffs.query(fixture.id).phase is OutputHandoffPhase.ABORTED
    outcome = node._handle_get_worker_lease_outcome(query)
    assert outcome.found and not outcome.cleanup_pending and outcome.state is protocol.LeaseExecutionState.COMPLETED
    assert outcome.completion_status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert node._handle_complete_worker_lease_inner(failed).accepted
    success = node._handle_complete_worker_lease_inner(complete)
    assert not success.accepted


def test_lost_rollback_report_ack_withholds_failed_outcome_after_local_cleanup():
    fixture, node, record, complete = _node(refs=False)
    assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))).accepted
    failed = replace(complete, status=protocol.TaskReplyStatus.SYSTEM_ERROR)
    fixture.fault = "rollback-report"
    query = protocol.GetWorkerLeaseOutcome(
        fixture.values.lease, fixture.values.task, fixture.values.attempt, fixture.values.executor,
        fixture.values.owner, ((fixture.id.object_id,)),
    )
    observed = False
    for _ in range(4):
        try:
            node._handle_complete_worker_lease_inner(failed)
        except TimeoutError as exc:
            assert "rollback-report" in str(exc)
            observed = True
            break
    assert observed
    assert fixture.store.used_bytes == 0
    assert fixture.journal.snapshot(fixture.id).rollback_tombstone is not None
    assert fixture.handoffs.query(fixture.id).phase is OutputHandoffPhase.ABORTED
    assert not fixture.adapter.rollback_reported(fixture.id)
    assert node._handle_get_worker_lease_outcome(query).cleanup_pending
    assert node._handle_complete_worker_lease_inner(failed).accepted
    assert fixture.adapter.rollback_reported(fixture.id)
    outcome = node._handle_get_worker_lease_outcome(query)
    assert outcome.completion_status is protocol.TaskReplyStatus.SYSTEM_ERROR and not outcome.cleanup_pending


def test_wrong_lease_manifest_has_no_journal_record_or_child_effect():
    fixture, node, record, _complete = _node()
    changed = replace(fixture.manifest.header, executor_worker_id=fixture.values.owner)
    # Preserve no-child metadata to isolate physical executor binding.
    from miniray.output_publication import OutputPublicationManifest
    wrong = OutputPublicationManifest.create(changed, (replace(fixture.manifest.value, transfers=())))
    reply = node._handle_prepare_output_publication(wire.PrepareOutputPublication(wrong, (fixture.values.payload)))
    assert not reply.accepted
    assert record.output_publication_id is None
    assert fixture.journal.publication_ids() == ()
    assert fixture.events == []


def test_early_success_complete_cannot_fence_a_later_valid_prepare():
    fixture, node, record, complete = _node()
    fixture.fault = "owner-register"
    request = wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))
    with pytest.raises(TimeoutError):
        node._handle_prepare_output_publication(request)
    reply = node._handle_complete_worker_lease_inner(complete)
    assert not reply.accepted and record.output_complete_inflight is None
    assert node._handle_prepare_output_publication(request).accepted
    assert node._handle_complete_worker_lease_inner(complete).accepted


def test_payload_retirement_is_independent_of_terminal_outbox_and_physical_replica():
    fixture, node, record, complete = _node()
    node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload)))
    assert node._handle_complete_worker_lease_inner(complete).accepted
    proof = OutputPublicationAdoptionProof(fixture.values.witness, fixture.values.owner, "owner-cas")
    c7 = fixture.adopt_from_owner_table(proof)
    assert node._handle_ack_output_publication_adopted(wire.AckOutputPublicationAdopted(proof, c7)).accepted
    assert fixture.journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
    assert fixture.store.used_bytes > 0
    assert node._drive_output_publications()
    query = protocol.GetWorkerLeaseOutcome(
        fixture.values.lease, fixture.values.task, fixture.values.attempt, fixture.values.executor,
        fixture.values.owner, ((fixture.id.object_id,)),
    )
    outcome = node._handle_get_worker_lease_outcome(query)
    assert outcome.state is protocol.LeaseExecutionState.COMPLETED
    assert outcome.completion_status is protocol.TaskReplyStatus.SUCCEEDED
    assert outcome.output_publication is None and outcome.descriptors == ()
    assert outcome.output_completion == fixture.values.witness
    replay = node._handle_complete_worker_lease_inner(complete)
    assert replay.accepted and replay.output_publication is None
    assert replay.output_completion == fixture.values.witness


def test_first_success_complete_after_worker_loss_cannot_change_terminal_history():
    fixture, node, record, complete = _node()
    assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))).accepted
    node._release_record_locked(record, protocol.LeaseExecutionState.WORKER_LOST)
    query = protocol.GetWorkerLeaseOutcome(
        fixture.values.lease, fixture.values.task, fixture.values.attempt, fixture.values.executor,
        fixture.values.owner, ((fixture.id.object_id,)),
    )
    node._workers[fixture.values.executor].process = SimpleNamespace(is_alive=lambda: False)
    assert node._handle_get_worker_lease_outcome(query).state is protocol.LeaseExecutionState.WORKER_LOST
    reply = node._handle_complete_worker_lease_inner(complete)
    assert not reply.accepted
    assert fixture.journal.snapshot(fixture.id).complete is None
    assert record.output_complete_inflight is None
    assert record.state is protocol.LeaseExecutionState.WORKER_LOST


def test_complete_keeps_both_local_locks_through_witness_and_ledger(monkeypatch):
    fixture, node, record, complete = _node()
    node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload)))
    original = fixture.journal.complete
    observed = []

    def checked(publication_id, witness):
        # Synchronous lock ownership probe; no helper thread or blocking wait.
        assert node._state_lock._is_owned()
        assert fixture.journal._lock._is_owned()
        assert record.state is protocol.LeaseExecutionState.RUNNING
        observed.append(True)
        return original(publication_id, witness)

    monkeypatch.setattr(fixture.journal, "complete", checked)
    assert node._handle_complete_worker_lease_inner(complete).accepted
    assert observed == [True]
    assert record.state is protocol.LeaseExecutionState.COMPLETED


def test_preboundary_complete_error_does_not_block_worker_loss_rollback(monkeypatch):
    fixture, node, record, complete = _node()
    node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload)))
    original = fixture.journal.complete
    monkeypatch.setattr(fixture.journal, "complete", lambda *_: (_ for _ in ()).throw(ValueError("before witness")))
    with pytest.raises(ValueError, match="before witness"):
        node._handle_complete_worker_lease_inner(complete)
    assert record.output_complete_inflight is None
    assert fixture.journal.snapshot(fixture.id).complete is None
    monkeypatch.setattr(fixture.journal, "complete", original)
    node._release_record_locked(record, protocol.LeaseExecutionState.WORKER_LOST)
    node._workers[fixture.values.executor].process = SimpleNamespace(is_alive=lambda: False)
    for _ in range(16):
        if node._drive_output_publications():
            break
    else:
        pytest.fail("uncrossed Complete blocked bounded worker-loss rollback")
    fixture.assert_no_pins_or_bytes()


def test_configured_checkpoints_follow_exact_owner_registration_and_promotions():
    fixture, node, record, complete = _node()
    observed = []

    class Gate:
        config = OutputPublicationGateConfig(0, ("127.0.0.1", 39001))

        def checkpoint(self, arrival):
            assert not node._state_lock._is_owned()
            assert not fixture.journal._lock._is_owned()
            assert not fixture.adapter._lock._is_owned()
            assert fixture.id in fixture.adapter._tickets
            assert arrival.publication_id == fixture.id
            assert arrival.manifest_digest == fixture.manifest.manifest_digest
            saved = fixture.handoffs.query(fixture.id)
            local = fixture.journal.snapshot(fixture.id)
            assert saved.complete is None and local.complete is None
            assert record.state is protocol.LeaseExecutionState.RUNNING
            if arrival.phase is OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK:
                assert saved.manifest == fixture.manifest and local.materialized is False
                assert fixture.store.used_bytes == 0
                assert not local.ready_to_complete
            elif arrival.phase in (OutputPublicationGatePhase.BEFORE_GRAPH_PREPARE,
                                    OutputPublicationGatePhase.AFTER_GRAPH_PREPARE_REPLY):
                assert not local.materialized and fixture.store.used_bytes == 0
                central = fixture.authority.query(ep.GetPublication(fixture.publication.reference)).snapshot
                if arrival.phase is OutputPublicationGatePhase.BEFORE_GRAPH_PREPARE:
                    assert arrival.graph_outcome is GraphReservationOutcome.UNOBSERVED
                    assert central.receipt(ep.PublicationStage.PREPARED) is None
                else:
                    assert arrival.graph_outcome is GraphReservationOutcome.ACCEPTED
                    assert central.receipt(ep.PublicationStage.PREPARED) is not None and central.graph_active
            else:
                assert arrival.phase is OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK
                assert not local.ready_to_complete and fixture.store.used_bytes > 0
                assert fixture.journal.preparation_receipt(fixture.id) is not None
                assert fixture.authority.query(ep.GetPublication(fixture.publication.reference)).snapshot.prepared is None
                for transfer in (fixture.manifest.value).transfers:
                    holds = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id).contained_holds
                    assert transfer.final_hold in holds and transfer.provisional_hold not in holds
            observed.append(arrival.phase)

    node._output_publication_gate = Gate()
    fixture.adapter._test_checkpoint = node._test_output_publication_checkpoint
    assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))).accepted
    assert observed == [OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK,
                        OutputPublicationGatePhase.BEFORE_GRAPH_PREPARE,
                        OutputPublicationGatePhase.AFTER_GRAPH_PREPARE_REPLY,
                        OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK]
    assert fixture.journal.snapshot(fixture.id).ready_to_complete
    assert node._handle_complete_worker_lease_inner(complete).accepted


def test_promoted_prepare_replay_cannot_emit_an_earlier_checkpoint():
    fixture, node, _record, complete = _node()
    calls = []

    class Gate:
        config = OutputPublicationGateConfig(0, ("127.0.0.1", 39001))

        def checkpoint(self, arrival):
            calls.append(arrival.phase)

    node._output_publication_gate = Gate()
    fixture.adapter._test_checkpoint = node._test_output_publication_checkpoint
    request = wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))
    fixture.fault = "promote"
    with pytest.raises(TimeoutError, match="promote"):
        node._handle_prepare_output_publication(request)
    assert calls == [OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK,
                     OutputPublicationGatePhase.BEFORE_GRAPH_PREPARE,
                     OutputPublicationGatePhase.AFTER_GRAPH_PREPARE_REPLY]
    assert not fixture.journal.snapshot(fixture.id).ready_to_complete
    with pytest.raises(RuntimeError, match="acknowledged phase"):
        node._test_output_publication_checkpoint(fixture.manifest, OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK)
    with pytest.raises(OutputPublicationJournalStateError):
        node._test_output_publication_checkpoint(fixture.manifest, OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK)
    assert node._handle_prepare_output_publication(request).accepted
    assert calls == [OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK,
                     OutputPublicationGatePhase.BEFORE_GRAPH_PREPARE,
                     OutputPublicationGatePhase.AFTER_GRAPH_PREPARE_REPLY,
                     OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK]
    assert node._handle_complete_worker_lease_inner(complete).accepted
