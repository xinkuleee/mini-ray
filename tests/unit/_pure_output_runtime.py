"""Tiny synchronous INLINE output handoffs for pure Core contracts.

At most four publications, four slots each, and 1 KiB per slot. Real discovery,
Node journal/adapter and metadata recovery produce the envelope; Core's actual
adoption/GC methods remain the owner authority. No Worker/Core constructor,
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
from miniray.output_recovery import OutputPublicationRecoveryAuthority
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
    """One in-memory Node/GCS pair for small ref-free successful outputs."""

    def __init__(self, core):
        self.core = core
        self.node_address = core.node_address
        self.gcs_address = ("output-gcs.invalid", 1)
        self.incarnation = OutputPublicationNodeIncarnation(core.node_id, 21001, 3)
        self.journal = OutputPublicationJournal()
        self.recovery = OutputPublicationRecoveryAuthority()
        self.pushes, self.replies = {}, {}
        self.completions, self.calls = [], []
        self.discoveries = 0

        def forbidden(*_args, **_kwargs):
            pytest.fail("small INLINE output fixture attempted child/graph/store work")

        self.adapter = OutputPublicationNodeAdapter(
            self.journal, report_intent=self.recovery.report_intent,
            arm_complete=self.recovery.arm_complete,
            report_terminal=self.recovery.report_terminal,
            report_rollback=self.recovery.report_rollback,
            prepare_child=forbidden, promote_child=forbidden, release_child=forbidden,
            prepare_graph=forbidden, abort_graph=forbidden,
            seal_replica=forbidden, drop_replica=forbidden,
        )

    def handles(self, handler):
        return handler in (
            wire.REPORT_OUTPUT_PUBLICATION_HANDLER, wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER,
            "get_worker_deaths",
        )

    def complete(self, push, values, *, inline_threshold=1024):
        """Return one exact cached TaskReply; replay never repeats discovery."""
        assert type(push) is protocol.PushTask
        # The default remains INLINE-only with forbidden store callbacks.
        # A bounded test subclass may set zero and supply a real small store.
        assert type(inline_threshold) is int and inline_threshold in (0, 1024)
        execution = push.target_execution or TaskExecutionKey.from_task_spec(push.spec)
        identity = OutputPublicationID(push.lease_id, execution)
        previous = self.pushes.get(identity)
        if previous is not None:
            assert previous == push
            return self.replies[identity]
        assert len(self.pushes) < 4 and 1 <= len(identity.output_ids) <= 4
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
            target_execution=push.target_execution, output_publication=envelope,
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
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert address == self.gcs_address
            identity = request.request_identity.publication_id
            envelope = self.replies[identity].output_publication
            plan = OutputOwnerPublicationPlan(envelope.manifest.execution, envelope)
            if type(request) is wire.ReportOutputPublicationTerminal:
                assert request.witness == envelope.complete
                ack = self.recovery.report_terminal(request.witness)
            elif type(request) is wire.ReportOutputPublicationAdopted:
                assert self.core.owner_table.output_owner_publication_receipt(plan).committed
                assert request.proof.complete == envelope.complete
                ack = self.recovery.report_adopted(request.proof)
            else:
                assert type(request) is wire.ReportOutputPublicationSlotCollected
                assert self.core.owner_table.collection_state(request.proof.object_id) is ObjectCollectionState.COLLECTING
                ack = self.recovery.report_slot_collected(request.proof)
            result = wire.OutputRecoveryReply(request, ack)
        else:
            assert address == self.node_address
            identity = request.proof.complete.publication_id
            envelope = self.replies[identity].output_publication
            assert self.recovery.snapshot(identity).adopted == request.proof
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
            snapshot = self.recovery.snapshot(identity)
            assert snapshot.complete == reply.output_publication.complete
            assert snapshot.adopted is not None
            assert tuple(proof.slot_index for proof in snapshot.slot_collections) == tuple(
                range(len(identity.output_ids))
            )
            assert not self.journal.snapshot(identity).retained_result_slots
            assert self.adapter.report_terminal(identity)
        assert not self.adapter.pending_terminal_reports()
