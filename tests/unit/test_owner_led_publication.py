"""Owner/Node/child-table composition without sockets, threads, or GCS."""

from dataclasses import replace

import cloudpickle
import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import ObjectRef
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_handoff import OutputHandoffPhase, OutputHandoffTable
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation
from miniray.output_publication_journal import (
    OutputPublicationJournal, OutputPublicationJournalState,
    OutputPublicationJournalStateError, OutputPublicationStage,
)
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.ownership import ObjectOwnerTable
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
        self.events = []
        self.releases = []
        self.fail_once = set()
        self.adapter = OutputPublicationNodeAdapter(
            self.journal, register_owner=self.register, report_complete=self.report_complete,
            report_rollback=self.report_rollback, prepare_child=self.prepare_child,
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
        self.lose_reply("register")
        assert self.validate_reply(request, snapshot).manifest == self.manifest

    def prepare_child(self, address, request):
        self.events.append("child-prepare")
        assert address == request.transfer.contained_owner_address
        assert self.handoffs.query(self.identity).manifest == self.manifest
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
        self.releases.append(request)
        released = self.child_table.release_contained_reference(request.object_id, request.hold)
        self.lose_reply("release")
        return protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, released,
        )

    def report_complete(self, witness):
        self.events.append("owner-complete")
        request = wire.ReportOutputHandoffComplete(witness)
        snapshot = self.handoffs.record_complete(request.witness)
        self.lose_reply("complete-report")
        reply = replace(wire.OutputHandoffCompleteAck(snapshot.complete, True))
        assert reply.accepted and reply.witness == witness

    def report_rollback(self, tombstone, *, manifest):
        self.events.append("owner-rollback")
        request = wire.ReportOutputHandoffRollback(manifest, tombstone)
        assert tuple(ack.effect for ack in request.tombstone.acknowledgements) == request.tombstone.plan.effects
        self.handoffs.abort(self.identity, "publisher-rollback")
        self.lose_reply("rollback-report")
        snapshot = self.validate_reply(request, self.handoffs.query(self.identity))
        assert snapshot.phase is OutputHandoffPhase.ABORTED

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
    envelope = p.complete()
    assert cloudpickle.loads((envelope.result).inline_data) == ("one", "value")
    assert p.events == ["owner-register", "lease-release"]
    assert p.handoffs.query(p.identity).complete is None
    assert p.adapter.pending_terminal_reports() == (envelope.complete,)
    assert p.adapter.pending_lease_completions() == ()
    assert p.adapter.report_terminal(p.identity)
    assert p.handoffs.query(p.identity).complete == envelope.complete
    assert p.handoffs.query(p.identity).phase is OutputHandoffPhase.PENDING
    p.discovery.release_sources_after_promotions()


def test_lost_owner_registration_ack_prevents_child_effects_until_exact_replay():
    p = _Publication(child=True)
    p.fail_once.add("register")
    with pytest.raises(RuntimeError, match="lost register ACK"):
        p.prepare()
    assert p.events == ["owner-register"]
    assert not p.child_table.snapshot(p.child_id).contained_holds
    assert not p.journal.snapshot(p.identity).ready_to_complete
    p.prepare()
    assert p.events == ["owner-register", "owner-register", "child-prepare", "child-promote"]
    transfer = (p.manifest.value).transfers[0]
    assert p.child_table.snapshot(p.child_id).contained_holds == frozenset((transfer.final_hold,))
    assert p.journal.snapshot(p.identity).ready_to_complete
    p.discovery.release_sources_after_promotions()


def test_lost_complete_report_ack_replays_witness_without_releasing_lease_twice():
    p = _Publication()
    p.prepare()
    envelope = p.complete()
    p.fail_once.add("complete-report")
    with pytest.raises(RuntimeError, match="lost complete-report ACK"):
        p.adapter.report_terminal(p.identity)
    assert p.handoffs.query(p.identity).complete == envelope.complete
    assert p.adapter.pending_terminal_reports() == (envelope.complete,)
    assert p.complete() == envelope
    assert p.events.count("lease-release") == 1
    assert p.adapter.report_terminal(p.identity)
    assert p.adapter.pending_terminal_reports() == ()
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
    with pytest.raises(OutputPublicationJournalStateError, match="owner death"):
        p.complete()
