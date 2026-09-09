"""Tiny synchronous INLINE output handoffs for pure Core contracts.

At most four publications, one output each, and 1 KiB per output. Real discovery,
Node journal/adapter and actual Core owner-handoff callbacks produce the
envelope; Core adoption/GC methods remain the owner authority. No Worker/Core constructor,
transport, thread, timer, sleep or value materialization is hidden here.
"""

from dataclasses import fields, is_dataclass

import pytest

from miniray import output_protocol as wire, protocol
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
from miniray.task_outputs import TaskExecutionKey


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
            prepare_child=forbidden, promote_child=forbidden, release_child=forbidden,
            seal_replica=forbidden, drop_replica=forbidden,
        )

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
        snapshot = self._owner_rpc(
            wire.ReportOutputHandoffComplete(witness), self.core.report_output_handoff_complete,
        )
        assert snapshot.complete == witness

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
        return handler in (wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER, "get_worker_deaths")

    def complete(self, push, values, *, inline_threshold=1024):
        """Return one exact cached TaskReply; replay never repeats discovery."""
        assert type(push) is protocol.PushTask
        # The default remains INLINE-only with forbidden store callbacks.
        # A bounded test subclass may set zero and supply a real small store.
        assert type(inline_threshold) is int and inline_threshold in (0, 1024)
        execution = TaskExecutionKey.from_task_spec(push.spec)
        identity = OutputPublicationID(push.lease_id, execution)
        previous = self.pushes.get(identity)
        if previous is not None:
            assert previous == push
            return self.replies[identity]
        assert len(self.pushes) < 4 and len(identity.output_ids) == 1
        assert push.spec.owner_worker_id == self.core.worker_id
        session = OutputDiscoverySession(OutputPublicationHeader(
            identity, push.spec.job_id, push.worker_id, push.spec.owner_worker_id, self.incarnation,
        ), inline_threshold=inline_threshold)
        outputs = session.discover(tuple(values))
        expected_tier = (protocol.ResultStorage.INLINE if inline_threshold
                         else protocol.ResultStorage.OBJECT_STORE)
        assert all(slot.tier is expected_tier and not slot.transfers
                   and slot.size_bytes <= 1024 for slot in outputs.manifest.slots)
        self.discoveries += 1
        self.adapter.prepare(outputs.manifest, outputs.slot_payloads)
        session.release_sources_after_promotions()

        def commit(witness):
            assert witness.publication_id == identity
            assert identity not in (item.publication_id for item in self.completions)
            self.completions.append(witness)

        envelope = self.adapter.complete(identity, commit_lease=commit)
        assert envelope.manifest == outputs.manifest
        assert tuple(result.inline_data for result in envelope.results) == (
            outputs.slot_payloads if inline_threshold
            else (None,) * len(outputs.slot_payloads)
        )
        self.pushes[identity] = push
        self.replies[identity] = protocol.TaskReply(
            push.spec.task_id, push.spec.attempt_id, push.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED, envelope.results,
            output_publication=envelope,
        )
        return self.replies[identity]

    def rpc(self, address, handler, request):
        assert self.handles(handler)
        self.calls.append((handler, request))
        _metadata(request)
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
                identity.output_ids[0]
            ) is ObjectCollectionState.COLLECTED
            assert not self.journal.snapshot(identity).retained_result_slots
            if any(item.publication_id == identity for item in self.adapter.pending_terminal_reports()):
                assert self.adapter.report_terminal(identity)
        assert not self.adapter.pending_terminal_reports()
