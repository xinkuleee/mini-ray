"""One real owner-led INLINE publication on an already unstarted Node.

No process, transport, child effect or physical store mutation. One actual
GCS metadata authority provides the required enhanced stage receipts.
The caller owns worker slots/leases/resource ledgers and invokes Complete.
"""
from dataclasses import replace
from types import SimpleNamespace
from threading import Condition, RLock

from miniray import output_protocol as wire, protocol, enhanced_publication as ep
from miniray.core import CoreWorker
from miniray.enhanced_publication_client import PublicationClient
from miniray.ids import JobID
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_handoff import OutputHandoffPhase, OutputHandoffTable
from miniray.output_publication import (
    OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation,
)
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.task_outputs import TaskExecution


def prepare_ref_free_output(node, request, grant, *, job_id=None, values=(7,)):
    assert len(values) == len(request.return_ids) == 1
    identity = OutputPublicationID(request.lease_id, (TaskExecution(request.attempt_id)))
    session = OutputDiscoverySession(OutputPublicationHeader(
        identity, job_id or JobID(b'j' * 16), grant.worker_id, request.requester_worker_id,
        OutputPublicationNodeIncarnation(node.node_id, node._node_pid, node._registration_epoch),
    ), inline_threshold=1024)
    assert request.return_ids == (identity.object_id,)
    outputs = session.discover((values[0]))
    manifest = outputs.manifest
    value = (manifest.value)
    assert value.tier is protocol.ResultStorage.INLINE and not value.transfers and value.size_bytes <= 1024
    journal, handoffs, rollback_reports = OutputPublicationJournal(), OutputHandoffTable(), []
    authority = ep.PublicationAuthority()
    publication_calls = []
    owner_address = request.requester_owner_address or ("ref-free-owner.invalid", 1)
    publication = ep.TaskPublication(manifest, owner_address)
    owner = object.__new__(CoreWorker)
    owner.worker_id, owner.owner_address = request.requester_worker_id, owner_address
    owner._owner_protocol_open = True
    owner._state_lock = RLock()
    owner._completion = Condition(owner._state_lock)
    owner._output_handoffs = handoffs

    def publication_rpc(message):
        assert len(publication_calls) < 16
        publication_calls.append(message)
        return authority.apply(message)

    owner._enhanced_publication_client = PublicationClient(lambda handler, message: publication_rpc(message))

    def abort_owner(value, scope):
        reply = owner.abort_owner_publication(ep.AbortOwnerPublication(value, scope))
        assert reply.accepted and reply.receipt is not None, reply.error
        return reply.receipt

    def forbidden(*_args, **_kwargs):
        raise AssertionError('ref-free INLINE fixture attempted child/store/external work')

    def owner_reply(request_value, snapshot):
        reply = replace(wire.OutputHandoffReply(request_value, True, snapshot))
        assert reply.request == request_value and reply.accepted
        return reply

    def register_owner(value):
        request_value = wire.RegisterOutputHandoff(value)
        assert value == manifest
        return owner_reply(request_value, handoffs.register(value, identity.attempt_id))

    def report_complete(witness):
        snapshot = handoffs.record_complete(witness)
        reply = replace(wire.OutputHandoffCompleteAck(snapshot.complete, True))
        assert reply.accepted and reply.witness == witness
        return reply

    def report_rollback(tombstone, *, manifest):
        request_value = wire.ReportOutputHandoffRollback(manifest, tombstone)
        assert manifest == outputs.manifest
        assert journal.snapshot(identity).rollback_tombstone == tombstone
        assert tuple(ack.effect for ack in tombstone.acknowledgements) == tombstone.plan.effects
        snapshot = handoffs.abort_manifest(manifest, tombstone.plan.rollback_id)
        assert snapshot.phase is OutputHandoffPhase.ABORTED
        reply = owner_reply(request_value, snapshot)
        rollback_reports.append((tombstone, reply))
        return reply

    adapter = OutputPublicationNodeAdapter(journal, register_owner=register_owner,
        report_complete=report_complete, report_rollback=report_rollback,
        publication_value=lambda value: ep.TaskPublication(value, owner_address),
        publication_rpc=publication_rpc, abort_owner=abort_owner,
        prepare_child=forbidden, promote_child=forbidden, release_child=forbidden,
        seal_replica=forbidden, drop_replica=forbidden)
    assert getattr(node, '_output_publication_journal', None) is None
    node._output_publication_journal, node._output_publications = journal, adapter
    if not hasattr(node, '_object_store'):
        node._object_store = ObjectStore(1024)
    node._sealed_metadata = getattr(node, '_sealed_metadata', {})
    node._dependency_pin_cleanups = getattr(node, '_dependency_pin_cleanups', {})
    before = node.resource_ledger.snapshot()
    prepared = node._handle_prepare_output_publication(
        wire.PrepareOutputPublication(manifest, (outputs.payload)))
    assert prepared.accepted and journal.snapshot(identity).ready_to_complete
    assert authority.query(ep.GetPublication(publication.reference)).snapshot.receipt(ep.PublicationStage.ARMED) is not None
    assert handoffs.query(identity).manifest == manifest
    assert handoffs.query(identity).complete is None
    assert node.resource_ledger.snapshot() == before
    session.release_sources_after_promotions()
    return SimpleNamespace(manifest=manifest, journal=journal, handoffs=handoffs,
        adapter=adapter, report_rollback=report_rollback, rollback_reports=rollback_reports,
        authority=authority, publication=publication, owner=owner)
