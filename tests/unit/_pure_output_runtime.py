"""Tiny synchronous INLINE output handoffs for pure Core contracts.

At most four publications, one output each, and 1 KiB per output. Real discovery,
Node journal/adapter and actual Core owner-handoff callbacks produce the
envelope; Core adoption/GC methods remain the owner authority. No Worker/Core constructor,
transport, thread, timer, sleep or value materialization is hidden here.
"""

from dataclasses import fields, is_dataclass

import pytest

from miniray import output_protocol as wire, protocol, enhanced_publication as ep
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import (
    OutputPublicationEnvelope, OutputPublicationHeader, OutputPublicationID,
    OutputPublicationNodeIncarnation,
)
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.output_handoff import OutputHandoffPhase
from miniray.ownership import ObjectCollectionState, OutputOwnerPublicationPlan
from miniray.task_outputs import TaskExecution


def _metadata(value):
    if isinstance(value, (AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID)):
        return
    assert not isinstance(value, (
        bytes, bytearray, memoryview, protocol.ResultDescriptor,
        protocol.ObjectStoreDescriptor, OutputPublicationEnvelope,
    )), "control-plane callback carried result data"
    if is_dataclass(value) and not isinstance(value, type):
        for item in fields(value):
            _metadata(getattr(value, item.name))
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _metadata(item)


class PureOutputRuntime:
    """Synchronous Node/owner composition for small ref-free successful outputs."""

    def __init__(self, core):
        self.core = core
        self.node_address = core.node_address
        self.gcs_address = core.gcs_address
        self.authority = core._test_publication_authority
        self.owner_address = core.owner_address
        assert self.owner_address is not None
        self.incarnation = OutputPublicationNodeIncarnation(core.node_id, 21001, 3)
        self.journal = OutputPublicationJournal()
        self.pushes, self.replies = {}, {}
        self.completions, self.calls = [], []
        self.discoveries = 0

        def forbidden(*_args, **_kwargs):
            pytest.fail("small INLINE output fixture attempted child/store work")

        self.adapter = OutputPublicationNodeAdapter(
            self.journal, register_owner=self._register_owner,
            report_complete=self._report_complete,
            report_rollback=self._report_rollback,
            publication_value=lambda manifest: ep.TaskPublication(manifest, self.owner_address),
            publication_rpc=self._publication_rpc, abort_owner=self._abort_owner,
            prepare_child=forbidden, promote_child=forbidden, release_child=forbidden,
            seal_replica=forbidden, drop_replica=forbidden,
        )

    def _publication_rpc(self, request):
        _metadata(request)
        reply = self.authority.apply(request)
        assert type(reply) is (ep.PublicationReply if type(request) is ep.GetPublication or not reply.accepted else ep.PublicationStageAck) and reply.request == request
        _metadata(reply)
        return reply

    def _abort_owner(self, publication, scope):
        request = ep.AbortOwnerPublication(publication, scope)
        reply = self.core.abort_owner_publication(request)
        assert type(reply) is ep.AbortOwnerPublicationReply
        assert reply.request == request and reply.accepted, reply.error
        assert reply.receipt is not None
        return reply.receipt

    def _owner_rpc(self, request, method):
        _metadata(request)
        self.calls.append((type(request).__name__, request))
        reply = method(request)
        assert type(reply) is wire.OutputHandoffReply
        assert reply.request == request and reply.accepted, reply.error
        _metadata(reply)
        return reply.snapshot

    def _register_owner(self, manifest):
        snapshot = self._owner_rpc(
            wire.RegisterOutputHandoff(manifest), self.core.register_output_handoff,
        )
        assert snapshot.manifest == manifest and snapshot.phase is OutputHandoffPhase.PENDING
        assert snapshot.publication_id == manifest.publication_id

    def _report_complete(self, witness):
        assert self.journal.snapshot(witness.publication_id).complete == witness
        request = wire.ReportOutputHandoffComplete(witness)
        _metadata(request)
        self.calls.append((type(request).__name__, request))
        reply = self.core.report_output_handoff_complete(request)
        assert type(reply) is wire.OutputHandoffCompleteAck and reply.accepted, reply.error
        assert reply.witness == witness
        _metadata(reply)

    def _report_rollback(self, tombstone, *, manifest):
        assert self.journal.snapshot(manifest.publication_id).rollback_tombstone == tombstone
        snapshot = self._owner_rpc(
            wire.ReportOutputHandoffRollback(manifest, tombstone),
            self.core.report_output_handoff_rollback,
        )
        assert snapshot.manifest == manifest and snapshot.phase is OutputHandoffPhase.ABORTED

    def handoff_snapshot(self, identity):
        snapshot = self._owner_rpc(
            wire.GetOutputHandoff(identity), self.core.get_output_handoff,
        )
        assert snapshot is not None and snapshot.publication_id == identity
        return snapshot

    def handles(self, handler):
        return handler in (wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER, "get_worker_deaths", ep.PUBLICATION_HANDLER)

    def complete(self, push, values, *, inline_threshold=1024):
        """Return one exact cached TaskReply; replay never repeats discovery."""
        assert type(push) is protocol.PushTask
        # The default remains INLINE-only with forbidden store callbacks.
        # A bounded test subclass may set zero and supply a real small store.
        assert type(inline_threshold) is int and inline_threshold in (0, 1024)
        execution = TaskExecution.from_task_spec(push.spec)
        identity = OutputPublicationID(push.lease_id, execution)
        previous = self.pushes.get(identity)
        if previous is not None:
            assert previous == push
            return self.replies[identity]
        assert len(self.pushes) < 4 and len(values) == 1
        assert push.spec.owner_worker_id == self.core.worker_id
        session = OutputDiscoverySession(OutputPublicationHeader(
            identity, push.spec.job_id, push.worker_id, push.spec.owner_worker_id, self.incarnation,
        ), inline_threshold=inline_threshold)
        outputs = session.discover((values[0]))
        expected_tier = (protocol.ResultStorage.INLINE if inline_threshold
                         else protocol.ResultStorage.OBJECT_STORE)
        assert (outputs.manifest.value.tier is expected_tier and (not outputs.manifest.value.transfers) and (outputs.manifest.value.size_bytes <= 1024))
        self.discoveries += 1
        self.adapter.prepare(outputs.manifest, (outputs.payload))
        session.release_sources_after_promotions()

        def commit(witness):
            assert witness.publication_id == identity
            assert identity not in (item.publication_id for item in self.completions)
            self.completions.append(witness)

        envelope = self.adapter.complete(identity, commit_lease=commit)
        assert envelope.manifest == outputs.manifest
        assert (envelope.result.object_id) == (
            (execution.object_id)
        )
        assert type(outputs.payload) is bytes
        assert envelope.result.inline_data == (outputs.payload if inline_threshold else None)
        self.pushes[identity] = push
        self.replies[identity] = protocol.TaskReply(
            push.spec.task_id, push.spec.attempt_id, push.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED, ((envelope.result,)),
            output_publication=envelope,
        )
        return self.replies[identity]

    def rpc(self, address, handler, request):
        assert self.handles(handler)
        self.calls.append((handler, request))
        _metadata(request)
        if handler == ep.PUBLICATION_HANDLER:
            assert address == self.gcs_address
            return self._publication_rpc(request)
        if handler == "get_worker_deaths":
            assert address == self.gcs_address and type(request) is protocol.GetWorkerDeaths
            assert request.after_epoch == 0  # this fixture never registers a death
            return protocol.GetWorkerDeathsReply(0, 0, ())
        assert address == self.node_address
        assert type(request) is wire.AckOutputPublicationAdopted
        identity = request.proof.complete.publication_id
        envelope = self.replies[identity].output_publication
        snapshot = self.handoff_snapshot(identity)
        assert snapshot.phase is OutputHandoffPhase.ADOPTED
        assert snapshot.adoption == request.proof
        assert self.core.owner_table.output_owner_publication_receipt(
            OutputOwnerPublicationPlan(envelope.manifest.execution, envelope),
        ).committed
        central = self.authority.query(ep.GetPublication(ep.PublicationRef(identity, envelope.manifest.manifest_digest))).snapshot
        assert central.adoption == request.proof
        assert request.gcs_adoption == central.receipt(ep.PublicationStage.ADOPTED)
        self.journal.retire_completed(request.proof)
        result = wire.AckOutputPublicationAdoptedReply(request, True)
        _metadata(result)
        return result

    def assert_collected(self):
        assert self.discoveries == len(self.pushes) == len(self.completions)
        for identity, reply in self.replies.items():
            snapshot = self.handoff_snapshot(identity)
            assert snapshot.complete == reply.output_publication.complete
            assert snapshot.adoption is not None
            assert self.core.owner_table.collection_state(
                (identity.object_id)
            ) is ObjectCollectionState.COLLECTED
            assert not self.journal.snapshot(identity).result_retained
            central = self.authority.query(ep.GetPublication(ep.PublicationRef(identity, reply.output_publication.manifest.manifest_digest))).snapshot
            assert not central.graph_active and central.closed_holds is not None
            assert central.receipt(ep.PublicationStage.RETIRED) is not None
            if any(item.publication_id == identity for item in self.adapter.pending_terminal_reports()):
                assert self.adapter.report_terminal(identity)
        assert not self.adapter.pending_terminal_reports()
