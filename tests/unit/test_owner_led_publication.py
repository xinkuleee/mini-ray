"""Finite owner/Node/child/GCS reducer composition without runtime I/O.

The accepted task identity is an input to the local owner handoff table. Real
Core handlers supply abort, Complete ACK and rollback decisions; one real GCS
authority supplies every publication receipt. No owner adoption CAS is run.
"""

from dataclasses import replace
from threading import Condition, RLock

import cloudpickle
import pytest

from miniray import enhanced_publication as enhanced, output_protocol as wire, protocol
from miniray.core import CoreWorker, ObjectRef
from miniray.enhanced_publication_client import PublicationClient
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_handoff import OutputHandoffPhase, OutputHandoffTable
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation
from miniray.output_publication_journal import (
    OutputPublicationJournal, OutputPublicationJournalState,
    OutputPublicationJournalStateError, OutputPublicationStage,
)
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.task_outputs import TaskExecution


pytestmark = pytest.mark.unit


class _Publication:
    """Real reducers and owner tables; callbacks replace only transport."""

    def __init__(self, *, child=False):
        job = JobID(b"j" * 16)
        task = TaskID.derive(job, TaskID.for_driver(job), 1)
        self.executor = WorkerID(b"w" * 16)
        self.owner = WorkerID(b"o" * 16)
        self.node = OutputPublicationNodeIncarnation(NodeID(b"n" * 16), 1234, 1)
        execution = (TaskExecution(AttemptID(task, 0)))
        header = OutputPublicationHeader(OutputPublicationID(LeaseID(b"l" * 16), execution),
                                         job, self.executor, self.owner, self.node)
        self.child_table = ObjectOwnerTable()
        self.child_id = ObjectID(TaskID.derive(job, task, 1), 0)
        self.child_table.register(self.child_id, local_token="source")
        value = [ObjectRef(self.child_id, self.executor, ("127.0.0.1", 31001))] if child else ("one", "value")
        self.discovery = OutputDiscoverySession(header, inline_threshold=10000)
        self.outputs = self.discovery.discover(value)
        self.manifest = self.outputs.manifest
        self.identity = self.manifest.publication_id
        self.handoffs = OutputHandoffTable()
        self.journal = OutputPublicationJournal()
        self.gcs = enhanced.PublicationAuthority()
        self.gcs_calls = []
        self.publication = enhanced.TaskPublication(self.manifest, ("127.0.0.1", 31002))
        # A narrow Core owner has no constructor, coordinator or transport.
        self.owner_core = owner_core = object.__new__(CoreWorker)
        owner_core.worker_id, owner_core.owner_address = self.owner, self.publication.owner_address
        owner_core._owner_protocol_open = True
        owner_core._state_lock = RLock()
        owner_core._completion = Condition(owner_core._state_lock)
        owner_core._owner_table = ObjectOwnerTable()
        owner_core._owner_table.register(self.identity.object_id, current_attempt=self.identity.attempt_id)
        owner_core._output_handoffs = self.handoffs
        owner_core._enhanced_publication_client = PublicationClient(self.owner_publication_rpc)
        self.events = []
        self.releases = []
        self.fail_once = set()
        self.adapter = OutputPublicationNodeAdapter(
            self.journal, register_owner=self.register, report_complete=self.report_complete,
            report_rollback=self.report_rollback, publication_value=self.publication_value,
            publication_rpc=self.publication_rpc, abort_owner=self.abort_owner,
            prepare_child=self.prepare_child,
            promote_child=self.promote_child, release_child=self.release_child,
            seal_replica=self.unexpected_replica, drop_replica=self.unexpected_replica,
        )

    def lose_reply(self, phase):
        if phase in self.fail_once:
            self.fail_once.remove(phase)
            raise RuntimeError("lost " + phase + " ACK")

    @staticmethod
    def validate_reply(request, snapshot):
        reply = replace(wire.OutputHandoffReply(request, True, snapshot))
        assert reply.request == request and reply.accepted
        return reply.snapshot

    def register(self, manifest):
        self.events.append("owner-register")
        request = wire.RegisterOutputHandoff(manifest)
        snapshot = self.handoffs.register(request.manifest, self.identity.attempt_id)
        self.owner_core._publication_client().remember(self.publication_value(manifest))
        self.lose_reply("register")
        assert self.validate_reply(request, snapshot).manifest == self.manifest

    def publication_value(self, manifest):
        assert manifest == self.manifest
        return enhanced.TaskPublication(manifest, self.owner_core.owner_address)

    def publication_rpc(self, request):
        assert enhanced.request_reference(request) == self.publication.reference
        assert len(self.gcs_calls) < 32, "publication exceeded its finite callback budget"
        self.gcs_calls.append(request)
        return self.gcs.apply(request)

    def owner_publication_rpc(self, handler, request):
        assert handler == enhanced.PUBLICATION_HANDLER
        return self.publication_rpc(request)

    def graph(self):
        return self.gcs.query(enhanced.GetPublication(self.publication.reference)).snapshot

    def abort_owner(self, publication, scope):
        assert scope == self.journal.rollback_scope(self.identity)
        request = enhanced.AbortOwnerPublication(publication, scope)
        reply = self.owner_core.abort_owner_publication(request)
        assert reply.request == request and reply.accepted, reply.error
        return reply.receipt

    def prepare_child(self, address, request):
        self.events.append("child-prepare")
        assert address == request.transfer.contained_owner_address
        assert self.handoffs.query(self.identity).manifest == self.manifest
        assert self.graph().graph_active and self.graph().forward_open
        assert self.graph().receipt(enhanced.PublicationStage.PREPARED) is not None
        disposition = self.child_table.prepare_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id,
        )
        return protocol.StoredContainedPinReply(request, disposition)

    def promote_child(self, address, request):
        self.events.append("child-promote")
        assert address == request.transfer.contained_owner_address
        disposition = self.child_table.promote_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id,
        )
        self.lose_reply("promotion")
        return protocol.StoredContainedPinReply(request, disposition)

    def release_child(self, address, request):
        transfer = (self.manifest.value).transfers[0]
        assert address == transfer.contained_owner_address
        assert request.object_id == self.child_id and request.owner_worker_id == self.executor
        assert request.hold in (transfer.final_hold, transfer.provisional_hold)
        assert self.graph().fence is not None and self.graph().graph_active
        self.releases.append(request)
        released = self.child_table.release_contained_reference(request.object_id, request.hold)
        self.lose_reply("release")
        return protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, released,
        )

    def report_complete(self, witness):
        self.events.append("owner-complete")
        request = wire.ReportOutputHandoffComplete(witness)
        assert self.graph().complete == witness
        reply = self.owner_core.report_output_handoff_complete(request)
        assert type(reply) is wire.OutputHandoffCompleteAck
        assert reply.accepted and reply.witness == witness and reply.error is None
        self.lose_reply("complete-report")
        return reply

    def report_rollback(self, tombstone, *, manifest):
        self.events.append("owner-rollback")
        request = wire.ReportOutputHandoffRollback(manifest, tombstone)
        assert tuple(ack.effect for ack in request.tombstone.acknowledgements) == request.tombstone.plan.effects
        assert self.graph().receipt(enhanced.PublicationStage.RETIRED) is not None
        reply = self.owner_core.report_output_handoff_rollback(request)
        assert reply.request == request and reply.accepted, reply.error
        self.lose_reply("rollback-report")
        assert reply.snapshot.phase is OutputHandoffPhase.ABORTED
        return reply

    @staticmethod
    def unexpected_replica(*args):
        raise AssertionError("INLINE publication must not invoke a replica callback")

    def prepare(self):
        self.adapter.prepare(self.manifest, (self.outputs.payload))

    def complete(self):
        return self.adapter.complete(self.identity, commit_lease=lambda witness: self.events.append("lease-release"))

    def death(self):
        return protocol.WorkerDeathRecord(
            "owner-exited", protocol.WorkerIncarnation(NodeID(b"d" * 16), 2222, 1, self.owner, 3333),
            1, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
        )


def test_reference_free_complete_releases_locally_before_owner_reporting():
    p = _Publication()
    p.prepare()
    assert p.journal.snapshot(p.identity).ready_to_complete
    assert p.graph().prepared == p.journal.preparation_receipt(p.identity)
    assert p.graph().receipt(enhanced.PublicationStage.ARMED) is not None
    envelope = p.complete()
    assert cloudpickle.loads((envelope.result).inline_data) == ("one", "value")
    assert p.events == ["owner-register", "lease-release"]
    assert p.handoffs.query(p.identity).complete is None
    assert p.graph().complete is None
    assert p.graph().receipt(enhanced.PublicationStage.TERMINAL) is None
    assert p.adapter.pending_terminal_reports() == (envelope.complete,)
    assert p.adapter.pending_lease_completions() == ()
    assert p.adapter.report_terminal(p.identity)
    assert p.handoffs.query(p.identity).complete == envelope.complete
    assert p.handoffs.query(p.identity).phase is OutputHandoffPhase.PENDING
    assert p.graph().complete == envelope.complete
    assert p.graph().receipt(enhanced.PublicationStage.TERMINAL) is not None
    assert p.graph().receipt(enhanced.PublicationStage.COMMITTED) is None
    assert p.graph().adoption is None and p.handoffs.query(p.identity).adoption is None
    assert p.owner_core._owner_table.snapshot(p.identity.object_id).state is ObjectState.PENDING
    assert p.journal.snapshot(p.identity).result_retained
    p.discovery.release_sources_after_promotions()


def test_lost_owner_registration_ack_prevents_child_effects_until_exact_replay():
    p = _Publication(child=True)
    p.fail_once.add("register")
    with pytest.raises(RuntimeError, match="lost register ACK"):
        p.prepare()
    assert p.events == ["owner-register"]
    assert not p.child_table.snapshot(p.child_id).contained_holds
    assert not p.journal.snapshot(p.identity).ready_to_complete
    assert not p.gcs_calls and p.graph() is None
    p.prepare()
    assert p.events == ["owner-register", "owner-register", "child-prepare", "child-promote"]
    transfer = (p.manifest.value).transfers[0]
    assert p.child_table.snapshot(p.child_id).contained_holds == frozenset((transfer.final_hold,))
    assert p.journal.snapshot(p.identity).ready_to_complete
    assert p.graph().receipt(enhanced.PublicationStage.ARMED) is not None
    p.discovery.release_sources_after_promotions()


def test_lost_complete_report_ack_replays_witness_without_releasing_lease_twice():
    p = _Publication()
    p.prepare()
    envelope = p.complete()
    p.fail_once.add("complete-report")
    with pytest.raises(RuntimeError, match="lost complete-report ACK"):
        p.adapter.report_terminal(p.identity)
    assert p.handoffs.query(p.identity).complete == envelope.complete
    terminal = p.graph().receipt(enhanced.PublicationStage.TERMINAL)
    assert terminal is not None and p.graph().complete == envelope.complete
    assert p.adapter.pending_terminal_reports() == (envelope.complete,)
    assert p.complete() == envelope
    assert p.events.count("lease-release") == 1
    assert p.adapter.report_terminal(p.identity)
    assert p.adapter.pending_terminal_reports() == ()
    assert p.graph().receipt(enhanced.PublicationStage.TERMINAL) == terminal
    assert p.graph().receipt(enhanced.PublicationStage.COMMITTED) is None
    assert p.owner_core._owner_table.snapshot(p.identity.object_id).state is ObjectState.PENDING
    assert p.journal.snapshot(p.identity).result_retained
    assert not p.adapter.report_terminal(p.identity)
    with pytest.raises(OutputPublicationJournalStateError, match="forbidden after Complete"):
        p.adapter.rollback(p.identity, "late-abort")
    p.discovery.release_sources_after_promotions()


def test_rollback_compensates_unknown_promotion_and_replays_exact_cleanup():
    p = _Publication(child=True)
    p.fail_once.add("promotion")
    with pytest.raises(RuntimeError, match="lost promotion ACK"):
        p.prepare()
    p.fail_once.add("release")
    with pytest.raises(RuntimeError, match="lost release ACK"):
        p.adapter.rollback(p.identity, "rollback-1", max_effects=3)
    assert p.journal.snapshot(p.identity).complete is None
    assert not p.child_table.snapshot(p.child_id).contained_holds
    assert p.graph().fence is not None and p.graph().graph_active
    assert p.graph().receipt(enhanced.PublicationStage.RETIRED) is None
    p.fail_once.add("rollback-report")
    with pytest.raises(RuntimeError, match="lost rollback-report ACK"):
        p.adapter.rollback(p.identity, "rollback-1", max_effects=3)
    assert not p.adapter.rollback_reported(p.identity)
    releases = tuple(p.releases)
    tombstone = p.adapter.rollback(p.identity, "rollback-1", max_effects=3)
    assert tuple(p.releases) == releases
    assert releases[0] == releases[1]
    assert [effect.stage for effect in tombstone.plan.effects] == [
        OutputPublicationStage.SLOT_DROP, OutputPublicationStage.FINAL_RELEASE,
        OutputPublicationStage.PROVISIONAL_RELEASE,
    ]
    assert p.adapter.rollback_reported(p.identity)
    assert p.adapter.pending_rollbacks() == ()
    assert p.journal.snapshot(p.identity).state is OutputPublicationJournalState.RETIRED
    assert p.journal.materialized_result(p.identity) is None
    graph = p.graph()
    assert graph.receipt(enhanced.PublicationStage.RETIRED) is not None
    assert not graph.forward_open and not graph.graph_active and graph.complete is None
    assert graph.closed_holds == p.journal.closed_rollback_holds(p.identity)
    assert graph.closed_holds.rollback_scope.prepare_intents == (0,)
    assert graph.closed_holds.rollback_scope.promote_intents == (0,)
    assert len(graph.closed_holds.releases) == 2
    for hold in ((p.manifest.value).transfers[0].final_hold, (p.manifest.value).transfers[0].provisional_hold):
        assert p.child_table.contained_release_was_seen(p.child_id, hold)
    p.discovery.abort()


def test_owner_death_releases_both_holds_then_sources_without_reversing_complete():
    p = _Publication(child=True)
    p.prepare()
    envelope = p.complete()
    cleanup_calls = []

    def cleanup():
        assert not p.child_table.snapshot(p.child_id).contained_holds
        cleanup_calls.append("sources")
        if len(cleanup_calls) == 1:
            return False
        p.discovery.abort()
        return True

    p.fail_once.add("release")
    with pytest.raises(RuntimeError, match="lost release ACK"):
        p.adapter.finish_owner_death(p.manifest, p.death(), cleanup=cleanup)
    assert cleanup_calls == []
    with pytest.raises(OutputPublicationJournalStateError, match="owner death"):
        p.prepare()
    assert not p.adapter.finish_owner_death(p.manifest, p.death(), cleanup=cleanup)
    assert p.journal.materialized_result(p.identity) is not None
    releases = tuple(p.releases)
    assert releases[0] == releases[1]
    assert len(releases) == 3
    assert p.adapter.finish_owner_death(p.manifest, p.death(), cleanup=cleanup)
    assert tuple(p.releases) == releases
    assert p.adapter.finish_owner_death(p.manifest, p.death(), cleanup=cleanup)
    assert cleanup_calls == ["sources", "sources"]
    snapshot = p.journal.snapshot(p.identity)
    assert snapshot.complete == envelope.complete
    assert snapshot.rollback is None
    assert snapshot.state is OutputPublicationJournalState.RETIRED
    assert not p.discovery.source_references
    assert p.journal.materialized_result(p.identity) is None
    assert p.adapter.pending_terminal_reports() == ()
    graph = p.graph()
    assert graph.fence == p.death() and graph.complete == envelope.complete
    assert graph.receipt(enhanced.PublicationStage.TERMINAL) is not None
    assert graph.receipt(enhanced.PublicationStage.COMMITTED) is None
    assert graph.graph_active and graph.receipt(enhanced.PublicationStage.RETIRED) is None
    closed = p.adapter.owner_death_closed_holds(p.identity)
    assert closed.reference == p.publication.reference and closed.rollback_scope is None
    assert not closed.child_deaths and len(closed.releases) == 2
    transfer = p.manifest.value.transfers[0]
    assert {reply.hold for reply in closed.releases} == {transfer.final_hold, transfer.provisional_hold}
    assert all(reply.accepted for reply in closed.releases)
    # Local Node retirement supplies closure evidence; only this real GCS
    # transition retires the graph, while the actual Complete stays recorded.
    receipt = p.owner_core._publication_client().retire(p.publication, closed.releases, closed.child_deaths)
    assert receipt == p.graph().receipt(enhanced.PublicationStage.RETIRED)
    assert not p.graph().graph_active and p.graph().complete == envelope.complete
    with pytest.raises(OutputPublicationJournalStateError, match="owner death"):
        p.complete()
