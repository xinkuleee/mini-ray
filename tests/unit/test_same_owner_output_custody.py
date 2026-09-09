"""Pure single-output custody when the submitter executes its own retry.

One outer, one child, one 16 KiB ObjectStore and one CPU ledger. Real discovery,
owner handoff/child tables, local owner CAS and Node storage handlers compose
synchronously. Borrowed credentials require actual retain/acquire transitions.
No Core/Node constructor, RPC, threads, waits or processes. Local composition
does not claim scheduler reconstruction, distributed shutdown or GCS evidence.
"""

from __future__ import annotations

from dataclasses import fields, replace
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time

import cloudpickle
import pytest

from miniray import output_protocol as wire, protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, ObjectID, TaskID
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import OutputPublicationManifest
from miniray.output_publication_journal import (
    OutputPublicationAdoptionProof, OutputPublicationJournal,
)
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.output_handoff import OutputHandoffPhase, OutputHandoffTable
from miniray.owner_service import StoredContainedPinOwnerAdapter
from miniray.ownership import (
    ConflictingBorrowerTokenError, ObjectCollectionState, ObjectOwnerTable,
    OutputOwnerPublicationPlan, ReleasedBorrowerTokenError,
    StoredContainedReferenceDisposition as Disposition,
)
from miniray.publication_sources import PreparedContainedTransfer, prepared_contained_transfer_fingerprint
from miniray.ref_transfer import exporting_references, importing_references
from miniray.resources import AllocationToken, ResourceLedger, ResourceVector
from tests.unit.test_output_discovery import _borrowed, _header, _owned
from tests.unit.test_output_publication_node_server import _bind_real_node_storage


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("same-owner custody contract attempted runtime infrastructure")

    for kind, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _Fixture:
    def __init__(self, *, borrowed=False, stored=False, same_owner=True):
        header = _header()
        self.header = replace(header, owner_worker_id=header.executor_worker_id) if same_owner else header
        header = self.header
        self.outer = ObjectOwnerTable()
        self.child_table = ObjectOwnerTable() if borrowed or not same_owner else self.outer
        source_task = TaskID.derive(header.job_id, TaskID.for_driver(header.job_id), 90)
        self.source_hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, header.executor_worker_id,
            source_task, AttemptID(source_task, 0),
        )
        self.child = (_borrowed(header, source=protocol.TaskHoldSource(self.source_hold))
                      if borrowed else _owned(header))
        self.child_table.register(self.child.object_id, local_token="source-live")
        if borrowed:
            root = (header.executor_worker_id, "original-source-handle")
            self.borrower = (header.executor_worker_id, self.child.borrower_token)
            assert self.child_table.add_borrowed_reference(self.child.object_id, root)
            assert self.child_table.retain_borrowed_reference_for_task(self.child.object_id, root, self.source_hold)
            assert self.child_table.release_borrowed_reference(self.child.object_id, root)
            assert self.child_table.acquire_exported_reference(
                self.child.object_id, self.child.borrow_source, self.borrower,
            )
        self.child_before = self.child_table.snapshot(self.child.object_id)
        self.values = ([self.child, self.child],)
        self.session = OutputDiscoverySession(header, inline_threshold=0 if stored else 4096)
        self.outputs = self.session.discover(self.values)
        assert len(self.outputs.slot_payloads) == 1 and len(self.outputs.slot_payloads[0]) < 4096
        self.manifest = self.outputs.manifest
        self.identity = self.manifest.publication_id
        self.transfers = tuple(slot.transfers[0] for slot in self.manifest.slots)
        self.journal = OutputPublicationJournal()
        self.handoffs = OutputHandoffTable()
        self.store = ObjectStore(16 * 1024)
        self.ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        self.token = self.ledger.allocate(ResourceVector({"CPU": 1}), AllocationToken("same-owner-execution"))
        self.pin_adapter = StoredContainedPinOwnerAdapter(self.child_table)
        self.events = []
        self.adapter = OutputPublicationNodeAdapter(
            self.journal, register_owner=self.register_owner,
            report_complete=self.report_complete, report_rollback=self.report_rollback,
            prepare_child=self.prepare,
            promote_child=self.promote, release_child=self.release,
            seal_replica=self.unbound_storage, drop_replica=self.unbound_storage,
        )
        self.node = _bind_real_node_storage(self)
        self.spec = protocol.TaskSpec(
            header.job_id, self.identity.task_id, self.identity.attempt_id,
            protocol.FunctionKey(header.job_id, __name__, "same-owner-output", "v1"),
            (), 1, ResourceVector({"CPU": 1}), header.owner_worker_id,
        )
        self.outer.register_task_outputs(self.spec, local_tokens=tuple(
            "output-{}".format(output.return_index) for output in self.identity.output_ids
        ))

    def unbound_storage(self, *_args):
        pytest.fail("Node storage must be bound before publication")

    def prepare(self, address, request):
        assert address == self.child.owner_address
        reply = self.pin_adapter.prepare(request)
        if reply.disposition is Disposition.PREPARED:
            holds = self.child_table.snapshot(self.child.object_id).contained_holds
            assert request.transfer.provisional_hold in holds and request.transfer.final_hold not in holds
        self.events.append(("prepare", request, reply))
        return reply

    def promote(self, address, request):
        assert address == self.child.owner_address
        reply = self.pin_adapter.promote(request)
        if reply.disposition is Disposition.PROMOTED:
            holds = self.child_table.snapshot(self.child.object_id).contained_holds
            assert request.transfer.provisional_hold not in holds and request.transfer.final_hold in holds
        self.events.append(("promote", request, reply))
        return reply

    def release(self, address, request):
        assert address == self.child.owner_address and request.owner_worker_id == self.child.owner_worker_id
        released = self.child_table.release_contained_reference(request.object_id, request.hold)
        assert self.child_table.contained_release_was_seen(request.object_id, request.hold)
        return protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, released,
        )

    def register_owner(self, manifest):
        request = wire.RegisterOutputHandoff(manifest)
        snapshot = self.handoffs.register(request.manifest, self.identity.attempt_id)
        reply = wire.OutputHandoffReply(request, True, snapshot)
        assert reply.request == request and reply.snapshot.manifest == self.manifest

    def report_complete(self, witness):
        request = wire.ReportOutputHandoffComplete(witness)
        snapshot = self.handoffs.record_complete(request.witness)
        reply = wire.OutputHandoffReply(request, True, snapshot)
        assert reply.request == request and reply.snapshot.complete == witness

    def report_rollback(self, tombstone, *, manifest):
        request = wire.ReportOutputHandoffRollback(manifest, tombstone)
        assert request.tombstone == self.journal.snapshot(self.identity).rollback_tombstone
        snapshot = self.handoffs.abort_manifest(manifest, tombstone.plan.rollback_id)
        reply = wire.OutputHandoffReply(request, True, snapshot)
        assert reply.request == request and reply.snapshot.phase is OutputHandoffPhase.ABORTED

    def commit_lease(self, witness):
        assert witness.publication_id == self.identity and witness.manifest_digest == self.manifest.manifest_digest
        assert self.ledger.release(self.token)


@pytest.mark.parametrize("borrowed,stored", ((False, False), (True, False), (False, True), (True, True)),
                         ids=("owned-inline", "borrowed-inline", "owned-stored", "borrowed-stored"))
def test_same_owner_discovery_promotes_and_collects_one_child_lifetime(borrowed, stored):
    f = _Fixture(borrowed=borrowed, stored=stored)
    slot, = f.manifest.slots
    transfer, = f.transfers
    assert slot.object_id.return_index == 0 and len(slot.transfers) == 1
    assert slot.tier is (protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE)
    assert f.child.object_id.task_id != f.identity.task_id
    assert f.session.source_references == (f.child,)
    assert f.child_table.snapshot(f.child.object_id) == f.child_before
    assert transfer.provisional_hold != transfer.final_hold
    assert transfer.provisional_hold.container_owner_worker_id == transfer.final_hold.container_owner_worker_id == f.header.owner_worker_id
    assert transfer.provisional_hold.transfer_token == "provisional:" + transfer.final_hold.transfer_token
    assert tuple(field.name for field in fields(transfer)) == (
        "contained_object_id", "contained_owner_worker_id", "contained_owner_address",
        "source", "provisional_hold", "final_hold",
    )
    assert pickle.loads(pickle.dumps(f.outputs)) == f.outputs
    restored_holds = []

    def restore(_object_id, _owner, _address, hold):
        restored_holds.append(hold)
        return object()

    with importing_references(restore):
        restored = cloudpickle.loads(f.outputs.slot_payloads[0])
    assert restored[0] is restored[1] and restored_holds == [transfer.final_hold]
    f.adapter.prepare(f.manifest, f.outputs.slot_payloads)
    assert [event[0] for event in f.events] == ["prepare", "promote"]
    assert all(reply.accepted for _stage, _request, reply in f.events)
    active = frozenset((transfer.final_hold,))
    assert f.child_table.snapshot(f.child.object_id).contained_holds == active
    assert f.child_table.contained_release_was_seen(f.child.object_id, transfer.provisional_hold)
    assert not f.child_table.release_contained_reference(f.child.object_id, transfer.provisional_hold)
    assert f.child_table.snapshot(f.child.object_id).contained_holds == active
    f.session.release_sources_after_promotions()
    assert not f.session.source_references and not f.child.closed
    envelope = f.adapter.complete(f.identity, commit_lease=f.commit_lease)
    assert f.ledger.available == f.ledger.total
    assert f.handoffs.query(f.identity).complete is None
    assert f.adapter.report_terminal(f.identity)
    assert f.handoffs.query(f.identity).complete == envelope.complete
    assert f.outer.commit_output_publication(OutputOwnerPublicationPlan(f.identity.execution, envelope)).committed
    proof = OutputPublicationAdoptionProof(envelope.complete, f.header.owner_worker_id, "same-owner-cas")
    f.handoffs.adopt(proof)
    f.journal.retire_completed(proof)
    assert not f.journal.snapshot(f.identity).retained_result_slots
    assert f.store.used_bytes == (len(f.outputs.slot_payloads[0]) if stored else 0)
    # Node reply retirement must not consume the outer's child lifetime.
    assert f.child_table.snapshot(f.child.object_id).contained_holds == active
    assert f.outer.begin_output_publication_collection(slot.object_id, collection_id="too-early") is None
    assert f.outer.release_local_reference(slot.object_id, "output-0")
    collection = f.outer.begin_output_publication_collection(slot.object_id, collection_id="same-owner-gc")
    assert collection is not None
    assert collection.metadata_plan.contained_releases == slot.edges
    release = protocol.ReleaseContainedReference(f.child.object_id, f.child.owner_worker_id, transfer.final_hold)
    reply = f.release(f.child.owner_address, release)
    assert reply.accepted and reply.released and reply.hold == transfer.final_hold
    assert reply.object_id == f.child.object_id and reply.owner_worker_id == f.child.owner_worker_id
    if stored:
        drop = protocol.DropObjectReplica(slot.object_id, f.identity.attempt_id, f.header.owner_worker_id,
                                          f.header.node_incarnation.node_id, slot.checksum)
        dropped = f.node._handle_drop_object_replica(drop)
        assert dropped.status is protocol.DropObjectReplicaStatus.DROPPED
        assert (dropped.object_id, dropped.producer_attempt_id, dropped.owner_worker_id, dropped.node_id, dropped.checksum) == (
            drop.object_id, drop.producer_attempt_id, drop.owner_worker_id, drop.node_id, drop.checksum)
        assert f.node._handle_drop_object_replica(drop).status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    # Current owner collection records the local metadata CAS after actual
    # child release / optional Node Drop above; it takes no fabricated graph ACK.
    assert f.outer.complete_output_publication_collection(collection).collection.collected
    assert f.outer.collection_state(slot.object_id) is ObjectCollectionState.COLLECTED
    assert f.child_table.prepare_stored_contained_reference(transfer, authority_worker_id=f.child.owner_worker_id) is Disposition.ALREADY_PREPARED
    assert f.child_table.promote_stored_contained_reference(transfer, authority_worker_id=f.child.owner_worker_id) is Disposition.ALREADY_PROMOTED
    assert not f.release(f.child.owner_address, release).released
    source_after = f.child_table.snapshot(f.child.object_id)
    assert not source_after.contained_holds
    assert source_after.local_tokens == f.child_before.local_tokens
    assert source_after.borrowed_tokens == f.child_before.borrowed_tokens
    assert source_after.retained_tokens == f.child_before.retained_tokens
    assert f.store.used_bytes == 0 and f.node._sealed_metadata == {}
    assert f.adapter.pending_terminal_reports() == ()


@pytest.mark.parametrize("released_phase", ("provisional", "final"))
def test_same_owner_release_before_prepare_or_promote_never_resurrects_custody(released_phase):
    f = _Fixture()
    transfer = f.transfers[0]
    hold = transfer.provisional_hold if released_phase == "provisional" else transfer.final_hold
    assert not f.child_table.release_contained_reference(f.child.object_id, hold)
    if released_phase == "provisional":
        with pytest.raises(ReleasedBorrowerTokenError):
            f.child_table.prepare_stored_contained_reference(transfer, authority_worker_id=f.child.owner_worker_id)
    else:
        assert f.child_table.prepare_stored_contained_reference(transfer, authority_worker_id=f.child.owner_worker_id) is Disposition.PREPARED
        with pytest.raises(ReleasedBorrowerTokenError):
            f.child_table.promote_stored_contained_reference(transfer, authority_worker_id=f.child.owner_worker_id)
        assert f.child_table.release_contained_reference(f.child.object_id, transfer.provisional_hold)
    assert not f.child_table.snapshot(f.child.object_id).contained_holds
    assert f.child_table.contained_release_was_seen(f.child.object_id, hold)


def test_same_outer_owner_does_not_authorize_an_expired_foreign_borrower():
    f = _Fixture(borrowed=True)
    assert f.child_table.release_borrowed_reference(f.child.object_id, f.borrower)
    before = f.child_table.snapshot(f.child.object_id)
    assert f.source_hold in before.retained_tokens
    with pytest.raises(ConflictingBorrowerTokenError):
        f.child_table.prepare_stored_contained_reference(f.transfers[0], authority_worker_id=f.child.owner_worker_id)
    assert f.child_table.snapshot(f.child.object_id) == before and not before.contained_holds


@pytest.mark.parametrize("fault", ("equal-holds", "wrong-domain", "double-domain", "other-outer-task"))
def test_same_owner_transfer_requires_exact_domain_and_same_container(fault):
    f = _Fixture()
    transfer = f.transfers[0]
    provisional = transfer.provisional_hold
    if fault == "equal-holds":
        provisional = transfer.final_hold
    elif fault == "wrong-domain":
        provisional = replace(provisional, transfer_token="other:" + transfer.final_hold.transfer_token)
    elif fault == "double-domain":
        provisional = replace(provisional, transfer_token="provisional:" + provisional.transfer_token)
    else:
        task = TaskID.derive(f.header.job_id, TaskID.for_driver(f.header.job_id), 91)
        provisional = replace(provisional, container_object_id=ObjectID.for_task(task))
    before = prepared_contained_transfer_fingerprint(transfer)
    with pytest.raises(ValueError):
        replace(transfer, provisional_hold=provisional)
    assert prepared_contained_transfer_fingerprint(transfer) == before
    assert not f.child_table.snapshot(f.child.object_id).contained_holds


def test_distinct_owner_discovery_keeps_unchanged_tokens_payloads_and_manifest_digest():
    f = _Fixture(same_owner=False)
    expected_slots = []
    expected_payloads = []
    for slot, value, transfer in zip(f.manifest.slots, f.values, f.transfers):
        token = "{}:slot:{}:transfer:0".format(f.identity.transaction_id, slot.object_id.return_index)
        expected = PreparedContainedTransfer(
            f.child.object_id, f.child.owner_worker_id, f.child.owner_address, transfer.source,
            ContainedReferenceHold(slot.object_id, f.header.executor_worker_id, token),
            ContainedReferenceHold(slot.object_id, f.header.owner_worker_id, token),
        )
        assert transfer == expected and transfer.provisional_hold.transfer_token == transfer.final_hold.transfer_token
        expected_slots.append(replace(slot, transfers=(expected,)))
        def export(reference):
            assert reference is f.child
            return f.child.object_id, f.child.owner_worker_id, f.child.owner_address, expected.final_hold
        with exporting_references(export):
            expected_payloads.append(cloudpickle.dumps(value))
    assert f.outputs.slot_payloads == tuple(expected_payloads)
    assert OutputPublicationManifest.create(f.header, tuple(expected_slots)) == f.manifest
    assert pickle.loads(pickle.dumps(f.manifest)).manifest_digest == f.manifest.manifest_digest
