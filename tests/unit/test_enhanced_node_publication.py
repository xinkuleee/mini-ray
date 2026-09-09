"""Finite real journal/GCS/child reducers; synchronous callbacks, no I/O.

Each case uses one INLINE Task output and at most two genuine owned children.
Faults lose one actual ACK after its reducer commits. Preparation receipts
come from the real journal, not expected-effect lists or fabricated Complete.
"""

from dataclasses import replace

import pytest

from miniray import enhanced_publication as enhanced, protocol
from miniray.core import ObjectRef
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_handoff import OutputHandoffTable
from miniray.output_publication import (OutputPublicationHeader, OutputPublicationID,
    OutputPublicationNodeIncarnation, OutputPublicationCompleteWitness)
from miniray.output_publication_journal import OutputPublicationJournal, OutputPublicationJournalStateError
from miniray.output_publication_node import OutputPublicationNodeAdapter, OutputPublicationRemoteError
from miniray.owner_service import StoredContainedPinOwnerAdapter
from miniray.ownership import ObjectOwnerTable
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest

pytestmark = pytest.mark.unit


class Publication:
    def __init__(self, *, child=False, child_count=None):
        job = JobID(b'j' * 16)
        task = TaskID.derive(job, TaskID.for_driver(job), 1)
        self.owner, self.executor = WorkerID(b'o' * 16), WorkerID(b'w' * 16)
        execution = TaskExecutionKey(TaskOutputManifest.for_task(task, 1), AttemptID(task, 0))
        header = OutputPublicationHeader(OutputPublicationID(LeaseID(b'l' * 16), execution),
            job, self.executor, self.owner, OutputPublicationNodeIncarnation(NodeID(b'n' * 16), 1234, 1))
        self.child_table = ObjectOwnerTable()
        self.children = tuple(ObjectID.for_task(TaskID.derive(job, task, index + 1))
                              for index in range(child_count if child_count is not None else 1))
        self.child = self.children[0]
        for child_id in self.children:
            self.child_table.register(child_id, local_token='source')
        self.discovery = OutputDiscoverySession(header, inline_threshold=10000)
        value = [ObjectRef(child_id, self.executor, ('127.0.0.1', 31001))
                 for child_id in self.children] if child or child_count else 7
        self.outputs = self.discovery.discover((value,))
        self.manifest = self.outputs.manifest
        self.identity = self.manifest.publication_id
        self.publication = enhanced.TaskPublication(self.manifest, ('127.0.0.1', 31002))
        self.journal, self.gcs, self.owner_handoff = OutputPublicationJournal(), enhanced.PublicationAuthority(), OutputHandoffTable()
        self.calls, self.releases, self.resource_releases = [], [], []
        self.lose = None
        self.lose_promotion = False
        self.adapter = OutputPublicationNodeAdapter(self.journal,
            register_owner=self.register, report_complete=self.owner_handoff.record_complete,
            report_rollback=lambda tombstone, manifest: None,
            publication_value=lambda manifest: self.publication, publication_rpc=self.rpc,
            abort_owner=self.abort, prepare_child=self.prepare_child, promote_child=self.promote_child,
            release_child=self.release_child, seal_replica=self.unexpected, drop_replica=self.unexpected)

    @staticmethod
    def unexpected(*args):
        pytest.fail('INLINE reducer case attempted physical storage or external work')

    def register(self, manifest):
        self.owner_handoff.register(manifest, self.identity.attempt_id)

    def rpc(self, request):
        assert len(self.calls) < 24
        self.calls.append(request)
        reply = self.gcs.apply(request)
        if type(request) is self.lose:
            self.lose = None
            assert reply.accepted
            raise TimeoutError('actual GCS ACK lost')
        return reply

    def abort(self, publication, scope):
        assert self.journal.rollback_scope(self.identity) == scope
        self.owner_handoff.abort(self.identity, scope.rollback_id)
        return enhanced.OwnerAbortReceipt(publication.reference, self.owner, scope.rollback_id)

    def prepare_child(self, address, request):
        assert self.gcs.query(enhanced.GetPublication(self.publication.reference)).snapshot.graph_active
        return protocol.StoredContainedPinReply(request, self.child_table.prepare_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id))

    def promote_child(self, address, request):
        disposition = self.child_table.promote_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id)
        if self.lose_promotion:
            self.lose_promotion = False
            raise TimeoutError('actual promotion ACK lost')
        return protocol.StoredContainedPinReply(request, disposition)

    def release_child(self, address, request):
        snapshot = self.gcs.query(enhanced.GetPublication(self.publication.reference)).snapshot
        assert snapshot.fence is not None and snapshot.graph_active
        assert self.owner_handoff.query(self.identity).abort_reason is not None
        self.releases.append(request)
        return protocol.ReleaseContainedReferenceReply(request.object_id, request.owner_worker_id,
            request.hold, True, self.child_table.release_contained_reference(request.object_id, request.hold))

    def prepare(self):
        self.adapter.prepare(self.manifest, self.outputs.slot_payloads)

    def complete(self):
        return self.adapter.complete(self.identity, commit_lease=self.resource_releases.append)


def test_unknown_intent_ack_has_no_child_effect_and_replays_exact_request():
    p = Publication(child=True)
    p.lose = enhanced.BeginPublication
    with pytest.raises(TimeoutError):
        p.prepare()
    assert not p.child_table.snapshot(p.child).contained_holds
    assert not p.journal.snapshot(p.identity).ready_to_complete
    p.prepare()
    assert p.calls[0] == p.calls[1]
    assert p.journal.snapshot(p.identity).ready_to_complete
    assert p.journal.preparation_receipt(p.identity) == p.gcs.snapshots()[0].prepared
    p.discovery.release_sources_after_promotions()


def test_unknown_arm_ack_keeps_real_preparation_but_does_not_authorize_complete():
    p = Publication(child=True)
    p.lose = enhanced.ArmTask
    with pytest.raises(TimeoutError):
        p.prepare()
    preparation = p.journal.preparation_receipt(p.identity)
    assert preparation == p.gcs.snapshots()[0].prepared
    assert preparation.prepare_replies[0].accepted and preparation.promote_replies[0].accepted
    assert not p.journal.snapshot(p.identity).ready_to_complete
    with pytest.raises(OutputPublicationJournalStateError):
        p.complete()
    assert p.resource_releases == []
    p.prepare()
    p.complete()
    assert len(p.resource_releases) == 1
    p.discovery.release_sources_after_promotions()


def test_complete_and_lost_terminal_ack_preserve_one_local_resource_release():
    p = Publication()
    p.prepare()
    envelope = p.complete()
    assert p.gcs.snapshots()[0].complete is None
    p.lose = enhanced.RecordTerminal
    with pytest.raises(TimeoutError):
        p.adapter.report_terminal(p.identity)
    assert p.gcs.snapshots()[0].complete == envelope.complete
    assert p.owner_handoff.query(p.identity).complete is None
    assert p.complete() == envelope
    assert len(p.resource_releases) == 1
    assert p.adapter.report_terminal(p.identity)
    assert p.owner_handoff.query(p.identity).complete == envelope.complete
    assert p.adapter.pending_terminal_reports() == ()
    p.discovery.release_sources_after_promotions()


def test_unknown_promotion_rollback_fences_before_effects_and_retires_exact_scope():
    p = Publication(child=True)
    p.lose_promotion = True
    with pytest.raises(TimeoutError):
        p.prepare()
    assert p.child_table.snapshot(p.child).contained_holds
    tombstone = p.adapter.rollback(p.identity, 'abort-1', max_effects=3)
    assert tombstone is not None and not p.child_table.snapshot(p.child).contained_holds
    snapshot = p.gcs.snapshots()[0]
    assert not snapshot.graph_active and not snapshot.forward_open and snapshot.complete is None
    closed = snapshot.closed_holds
    assert closed.rollback_scope == p.journal.rollback_scope(p.identity)
    assert closed.rollback_scope.prepare_intents == closed.rollback_scope.promote_intents == (0,)
    assert len(closed.releases) == 2 and len(p.releases) == 2
    assert p.adapter.rollback(p.identity, 'abort-1', max_effects=3) == tombstone
    assert len(p.releases) == 2
    with pytest.raises(OutputPublicationJournalStateError):
        p.prepare()
    p.discovery.abort()


def test_two_children_unknown_first_prepare_then_collected_second_rejects_and_cannot_revive():
    p = Publication(child_count=2)
    first, second = p.children
    first_transfer, second_transfer = p.manifest.slots[0].transfers
    child_endpoint = StoredContainedPinOwnerAdapter(p.child_table)
    prepare_calls, prepare_replies = [], []
    first_ack_lost = False

    def prepare(address, request):
        nonlocal first_ack_lost
        assert address == request.transfer.contained_owner_address
        assert p.gcs.snapshots()[0].graph_active
        reply = child_endpoint.prepare(request)
        prepare_calls.append(request)
        prepare_replies.append(reply)
        if request.transfer == first_transfer and not first_ack_lost:
            first_ack_lost = True
            assert reply.accepted
            # The other owner loses its source lifetime after discovery. Real
            # owner-table collection, not a typed fake rejection, closes it.
            assert p.child_table.publish_inline(second, None, b'second-child')
            assert p.child_table.release_local_reference(second, 'source')
            plan = p.child_table.begin_collection(second, collection_id='close-second-source')
            assert plan is not None and not plan.contained_releases and not plan.locations
            p.child_table.complete_collection(plan)
            raise TimeoutError('first child applied before ACK loss')
        return reply

    p.adapter._prepare_child = prepare
    with pytest.raises(TimeoutError, match='first child applied'):
        p.prepare()
    assert prepare_calls == [protocol.PrepareStoredContainedPin(first_transfer, p.executor)]
    assert p.child_table.snapshot(first).contained_holds == frozenset((first_transfer.provisional_hold,))
    assert not p.child_table.contains(second)
    assert p.gcs.snapshots()[0].graph_active

    with pytest.raises(OutputPublicationRemoteError):
        p.prepare()
    assert prepare_calls[0] == prepare_calls[1]
    assert prepare_calls[2] == protocol.PrepareStoredContainedPin(second_transfer, p.executor)
    assert prepare_replies[1].accepted and not prepare_replies[2].accepted
    assert prepare_replies[2].error_kind is protocol.StoredPublicationRPCErrorKind.INVALID_STATE
    assert p.journal.snapshot(p.identity).complete is None
    assert p.gcs.snapshots()[0].prepared is None
    assert p.gcs.snapshots()[0].graph_active

    # One bounded compensation round preserves the graph until the first
    # child's genuinely active hold also has its exact Release ACK.
    assert p.adapter.rollback(p.identity, 'two-child-abort', max_effects=1) is None
    assert p.gcs.snapshots()[0].graph_active
    assert p.child_table.snapshot(first).contained_holds
    terminal = p.adapter.rollback(p.identity, 'two-child-abort', max_effects=1)
    assert terminal is not None
    closed = p.gcs.snapshots()[0].closed_holds
    assert not p.gcs.snapshots()[0].graph_active
    assert closed.rollback_scope.prepare_intents == (0, 1)
    assert closed.rollback_scope.promote_intents == ()
    assert not closed.rollback_scope.materialization_started
    assert {reply.hold for reply in closed.releases} == {first_transfer.provisional_hold, second_transfer.provisional_hold}
    assert not p.child_table.snapshot(first).contained_holds
    assert p.resource_releases == []

    # A historical first Prepare may replay its original acceptance, but it
    # must not recreate a released pin; late Promote must remain rejected.
    for transfer in (first_transfer, second_transfer):
        child_endpoint.prepare(protocol.PrepareStoredContainedPin(transfer, p.executor))
        late = child_endpoint.promote(protocol.PromoteStoredContainedPin(transfer, p.executor))
        assert not late.accepted
        assert p.child_table.contained_release_was_seen(transfer.contained_object_id, transfer.provisional_hold)
    assert not p.child_table.snapshot(first).contained_holds
    assert not p.child_table.contains(second)
    assert not p.gcs.snapshots()[0].graph_active
    with pytest.raises(OutputPublicationJournalStateError):
        p.prepare()
    p.discovery.abort()
