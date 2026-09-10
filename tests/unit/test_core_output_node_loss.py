"""Pure current Core loss custody and stale delayed-work boundaries.

One accepted task, one output/child and optionally one actual local put input.
Real owner/handoff/recovery/finish reducers run with synchronous child releases,
with real enhanced publication authority and Node journal callbacks. No runtime
constructor, thread, process, wait or user task execution occurs.
"""

from dataclasses import replace
import hashlib

import pytest

from miniray import enhanced_publication as ep, output_protocol as wire, protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.core import _DelayedReadyTask, _OutputAdoptionObligation, _OutputNodeLossObligation, _PendingTask, _WAKE_COORDINATOR
from miniray.errors import SystemTaskError
from miniray.ids import LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope, OutputPublicationHeader,
    OutputPublicationID, OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputValue,
)
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_publication_pg_loss_paths import _no_runtime
from tests.unit.test_enhanced_owner_client import runtime as enhanced_runtime


pytestmark = pytest.mark.unit


def test_enhanced_query_arrival_is_observed_before_immutable_custody_choice(enhanced_runtime, monkeypatch):
    from miniray import enhanced_publication as ep
    r = enhanced_runtime
    child = r.leaf(41)
    pending, outer = r.submit()
    envelope = r.prepare(pending, child)
    identity = envelope.publication_id
    incarnation = envelope.manifest.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "query-window-publisher-loss", incarnation.node_id, incarnation.node_pid,
        incarnation.registration_epoch, 1, 7, protocol.NodeDeathReason.PROCESS_EXIT,
        "registered fixture publisher loss boundary",
    )
    r.owner._dead_nodes[incarnation.node_id] = death
    client = r.owner._publication_client()
    query = client.query
    arrivals = []

    def arrive(publication):
        snapshot = query(publication)
        if publication.reference.key == identity and not arrivals:
            assert not r.owner._state_lock._is_owned()
            assert identity not in getattr(r.owner, "_output_loss_choices", {})
            assert snapshot.complete == envelope.complete
            arrivals.append(envelope)
            assert not r.adopt(pending, envelope)
            assert r.owner._output_result_custody[identity] == envelope
        return snapshot

    with monkeypatch.context() as patch:
        patch.setattr(client, "query", arrive)
        assert r.owner._drive_output_node_loss(pending, _OutputNodeLossObligation(identity, death))
    assert arrivals == [envelope] and r.owner._output_loss_choices[identity] is True
    owner = r.owner.owner_table.snapshot(outer.object_id)
    assert owner.state is ObjectState.READY_INLINE and owner.inline_data == envelope.result.inline_data
    assert r.owner._recovery.task_record(pending.task_id).retries_started == 0
    central = r.authority.query(ep.GetPublication(ep.PublicationRef(identity, envelope.manifest.manifest_digest))).snapshot
    assert central.complete == envelope.complete and central.adoption is not None and central.graph_active
    assert not any(handler == "release_contained_reference" for handler, _ in r.calls)
    r.finish_attempt(pending)


def test_enhanced_preterminal_history_cannot_downgrade_actual_complete_custody(enhanced_runtime, monkeypatch):
    from miniray import enhanced_publication as ep
    r = enhanced_runtime
    child = r.leaf(42)
    pending, outer = r.submit()
    identity = OutputPublicationID(LeaseID(b'Q' * 16), pending.execution)
    session = OutputDiscoverySession(OutputPublicationHeader(
        identity, r.owner.job_id, r.publisher.worker_id, r.owner.worker_id, r.incarnation,
    ), inline_threshold=1024)
    prepared = session.discover((child, "actual-local-complete"))
    r.adapter.prepare(prepared.manifest, prepared.payload)
    session.release_sources_after_promotions()
    envelope = r.adapter.complete(identity, commit_lease=r.completions.append)
    r.envelopes[identity] = envelope
    publication = ep.TaskPublication(envelope.manifest, r.owner.owner_address)
    central = r.authority.query(ep.GetPublication(publication.reference)).snapshot
    assert central.prepared is not None and central.complete is None
    assert central.receipt(ep.PublicationStage.ARMED) is not None
    assert r.owner._output_handoff_table().query(identity).complete is None
    assert r.journal.snapshot(identity).complete == envelope.complete
    assert r.owner.owner_table.snapshot(outer.object_id).state is ObjectState.PENDING
    death = protocol.NodeDeathRecord(
        "preterminal-publisher-loss", r.incarnation.node_id, r.incarnation.node_pid,
        r.incarnation.registration_epoch, 1, 7, protocol.NodeDeathReason.PROCESS_EXIT,
        "local Complete survived in owner-delivered envelope",
    )
    r.owner._dead_nodes[r.incarnation.node_id] = death
    with monkeypatch.context() as patch:
        patch.setattr(r.owner, "_retry_system_failure", lambda *_a, **_k: pytest.fail("actual Complete became unknown retry"))
        assert r.owner._drive_output_node_loss(pending, _OutputNodeLossObligation(identity, death, envelope))
    after = r.owner.owner_table.snapshot(outer.object_id)
    assert after.state is ObjectState.READY_INLINE and after.inline_data == prepared.payload
    assert r.owner._recovery.task_record(pending.task_id).retries_started == 0
    central = r.authority.query(ep.GetPublication(publication.reference)).snapshot
    assert central.complete == envelope.complete and central.adoption is not None
    assert r.adapter.report_terminal(identity)
    assert len(r.completions) == 1
    r.finish_attempt(pending)


def _id(kind, value):
    return kind(bytes((value,)) * 16)


def _consume_queued(core, expected_type):
    selected = []
    count = core._submissions.qsize()
    assert count <= 8
    for _ in range(count):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if type(item) is expected_type:
            selected.append(item)
        else:
            assert item is _WAKE_COORDINATOR
    assert len(selected) == 1
    return selected[0]


class _LossFixture:
    def __init__(self, *, known=True, with_input=False):
        self.core = make_pure_core()
        self.source = self.core.put(b"i") if with_input else None
        if with_input:
            assert self.core._submissions.get_nowait() is _WAKE_COORDINATOR
            self.core._submissions.task_done()
        self.pending, self.ref = self.core._register_submission(
            self.core.define_remote_function(lambda *args: None),
            () if self.source is None else (self.source,), {}, ResourceVector(),
            max_retries=3, num_returns=1, _enqueue=True,
        )
        assert self.core._submissions.get_nowait() is self.pending
        self.core._submissions.task_done()
        self.publisher, self.child_owner = _id(NodeID, 81), _id(WorkerID, 82)
        self.child = ObjectID.for_task(_id(TaskID, 83))
        self.transfer = PreparedContainedTransfer(
            self.child, self.child_owner, ("child.invalid", 1234), OwnedContainedSource(self.child_owner),
            ContainedReferenceHold(self.ref.object_id, self.child_owner, "core-loss-child"),
            ContainedReferenceHold(self.ref.object_id, self.core.worker_id, "core-loss-child"),
        )
        self.identity = OutputPublicationID(_id(LeaseID, 84), self.pending.execution)
        self.payload = b"value"
        slot = (OutputValue(protocol.ResultStorage.INLINE, 5, hashlib.sha256(self.payload).hexdigest(), (self.transfer,)))
        self.manifest = OutputPublicationManifest.create(OutputPublicationHeader(
            self.identity, self.core.job_id, self.child_owner, self.core.worker_id,
            OutputPublicationNodeIncarnation(self.publisher, 8101, 1),
        ), slot)
        self.child_table = ObjectOwnerTable()
        self.child_table.register(self.child, local_token="child-source")
        self.child_table.publish_inline(self.child, None, b"child")
        self.journal = OutputPublicationJournal()
        self.publication = ep.TaskPublication(self.manifest, self.core.owner_address)
        self.gcs_calls, self.local_completions = [], []

        def publication_rpc(request):
            assert len(self.gcs_calls) < 40
            reply = self.core._test_publication_authority.apply(request)
            self.gcs_calls.append((request, reply))
            return reply

        def register(manifest):
            reply = self.core.register_output_handoff(wire.RegisterOutputHandoff(manifest))
            assert reply.accepted, reply.error

        def report(witness):
            reply = self.core.report_output_handoff_complete(wire.ReportOutputHandoffComplete(witness))
            assert type(reply) is wire.OutputHandoffCompleteAck and reply.accepted and reply.witness == witness

        def abort(publication, scope):
            reply = self.core.abort_owner_publication(ep.AbortOwnerPublication(publication, scope))
            assert reply.accepted and reply.receipt is not None, reply.error
            return reply.receipt

        def child(address, request):
            assert address == self.transfer.contained_owner_address
            method = (self.child_table.prepare_stored_contained_reference if type(request) is protocol.PrepareStoredContainedPin
                      else self.child_table.promote_stored_contained_reference)
            return protocol.StoredContainedPinReply(request, method(request.transfer, authority_worker_id=request.authority_worker_id))

        def forbidden(*_args):
            pytest.fail("INLINE loss setup attempted unrelated effect")

        self.adapter = OutputPublicationNodeAdapter(
            self.journal, register_owner=register, report_complete=report, report_rollback=forbidden,
            publication_value=lambda manifest: ep.TaskPublication(manifest, self.core.owner_address),
            publication_rpc=publication_rpc, abort_owner=abort, prepare_child=child, promote_child=child,
            release_child=lambda address, request: self.release(address, "release_contained_reference", request),
            seal_replica=forbidden, drop_replica=forbidden,
        )
        self.adapter.prepare(self.manifest, self.payload)
        self.envelope = self.adapter.complete(self.identity, commit_lease=self.local_completions.append)
        self.complete = self.envelope.complete
        assert len(self.local_completions) == 1
        if known:
            assert self.adapter.report_terminal(self.identity)
        self.death = protocol.NodeDeathRecord(
            "core-loss-publisher", self.publisher, 8101, 1, 1, 7, protocol.NodeDeathReason.PROCESS_EXIT, "explicit membership fact",
        )
        self.core._dead_nodes[self.publisher] = self.death
        self.obligation = _OutputNodeLossObligation(self.identity, self.death)
        self.calls = []
        self.core._borrow_rpc = self.release

    def release(self, address, handler, request):
        assert address == self.transfer.contained_owner_address and handler == "release_contained_reference"
        assert not self.core._state_lock._is_owned() and len(self.calls) < 5
        self.calls.append(request)
        released = self.child_table.release_contained_reference(request.object_id, request.hold)
        return protocol.ReleaseContainedReferenceReply(request.object_id, request.owner_worker_id, request.hold, True, released)

    def lose_first_ack(self):
        def release(address, handler, request):
            reply = self.release(address, handler, request)
            if len(self.calls) == 1:
                raise TimeoutError("child release applied before lost ACK")
            return reply
        self.core._borrow_rpc = release

    def close(self):
        self.ref.close()
        if self.source is not None:
            self.source.close()
        close_pure_core(self.core)


@pytest.mark.parametrize("known", (True, False), ids=("known-complete", "completion-unknown"))
def test_no_envelope_cleanup_ack_loss_fences_local_state_until_exact_replay(known):
    f = _LossFixture(known=known)
    core, pending = f.core, f.pending
    f.lose_first_ack()
    try:
        before = core.owner_table.snapshot(f.ref.object_id)
        record_before = replace(core._recovery.task_record(pending.task_id))
        assert not core._execute(pending, pending.spec, output_node_loss=f.obligation)
        assert len(f.calls) == 1 and f.child_table.contained_release_was_seen(f.child, f.transfer.final_hold)
        assert core.owner_table.snapshot(f.ref.object_id) == before
        assert core._recovery.task_record(pending.task_id) == record_before
        assert not core._finish_pending_task(pending)
        delayed = _consume_queued(core, _DelayedReadyTask)
        assert delayed.ready.output_node_loss == replace(f.obligation, round=1)
        assert core._accepted_task_count == 1 and core._task_finish_barriers == {f.ref.object_id: pending}
        assert core._execute(pending, pending.spec, output_node_loss=delayed.ready.output_node_loss) is known
        assert f.calls[0] == f.calls[1] and len(f.calls) == 3
        assert not f.child_table.snapshot(f.child).contained_holds
        assert not core._protocol_unresolved and not core._output_node_cleanup and not core._output_loss_drivers
        after = core.owner_table.snapshot(f.ref.object_id)
        record = core._recovery.task_record(pending.task_id)
        assert after.state is (ObjectState.LOST if known else ObjectState.PENDING)
        assert after.inline_data is None and after.local_tokens == before.local_tokens
        assert record.state is (TaskState.SUCCEEDED if known else TaskState.RETRY_PENDING)
        assert record.retries_started == (0 if known else 1)
        assert core._finish_pending_task(pending) is known
        if known:
            core._start_or_join_reconstruction(pending.object_id, core._objects[pending.object_id])
        successor = _consume_queued(core, _PendingTask)
        assert successor.spec.attempt_id == pending.spec.attempt_id.next()
        state = core.owner_table.snapshot(f.ref.object_id)
        history = replace(core._recovery.task_record(pending.task_id))
        assert core._execute(pending, pending.spec, output_node_loss=delayed.ready.output_node_loss)
        assert not core._finish_pending_task(pending)
        assert core.owner_table.snapshot(f.ref.object_id) == state
        assert core._recovery.task_record(pending.task_id) == history
        assert core._task_finish_barriers == {f.ref.object_id: successor}
        assert core._accepted_task_count == 1 and len(f.calls) == 3
    finally:
        f.close()


@pytest.mark.parametrize("known", (True, False), ids=("new-reconstruction-hold", "retained-system-retry-hold"))
def test_old_delayed_completion_cannot_release_current_attempt_input_hold(known):
    f = _LossFixture(known=known, with_input=True)
    core, pending = f.core, f.pending
    f.lose_first_ack()
    try:
        input_id = f.source.object_id
        original = pending.dependency_hold
        before = core.owner_table.snapshot(input_id)
        assert before.submitted_tokens == frozenset({original}) and before.lineage_tokens
        assert not core._execute(pending, pending.spec, output_node_loss=f.obligation)
        delayed = _consume_queued(core, _DelayedReadyTask)
        assert not core._finish_pending_task(pending)
        assert core.owner_table.snapshot(input_id) == before
        assert core._execute(pending, pending.spec, output_node_loss=delayed.ready.output_node_loss) is known
        if known:
            assert core._finish_pending_task(pending)
            assert not core.owner_table.snapshot(input_id).submitted_tokens
            core._start_or_join_reconstruction(pending.object_id, core._objects[pending.object_id])
        else:
            assert not core._finish_pending_task(pending)
        successor = _consume_queued(core, _PendingTask)
        hold = successor.dependency_hold
        assert successor.protected_dependencies == (input_id,)
        assert (hold != original) is known
        assert hold.origin_attempt_id == (successor.spec.attempt_id if known else pending.spec.attempt_id)
        retained = core.owner_table.snapshot(input_id)
        assert retained.submitted_tokens == frozenset({hold}) and retained.lineage_tokens == before.lineage_tokens
        output_before = core.owner_table.snapshot(f.ref.object_id)
        record_before = replace(core._recovery.task_record(pending.task_id))
        for _ in range(2):
            assert core._execute(pending, pending.spec, output_node_loss=delayed.ready.output_node_loss)
            assert not core._finish_pending_task(pending)
            assert core.owner_table.snapshot(input_id) == retained
            assert core.owner_table.snapshot(f.ref.object_id) == output_before
            assert core._recovery.task_record(pending.task_id) == record_before
            assert core._task_finish_barriers == {f.ref.object_id: successor}
            assert core._accepted_task_count == 1 and len(f.calls) == 3
        assert core._publish_task_error(successor, SystemTaskError("finish bounded retry"))
        assert core._finish_pending_task(successor)
        assert not core.owner_table.snapshot(input_id).submitted_tokens
        assert core.owner_table.snapshot(input_id).lineage_tokens == before.lineage_tokens
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
    finally:
        f.close()


@pytest.mark.parametrize("known", (True, False), ids=("known-complete", "completion-unknown"))
def test_envelope_before_loss_choice_preserves_actual_inline_custody(known):
    f = _LossFixture(known=known)
    try:
        before = f.child_table.snapshot(f.child)
        assert f.core._drive_output_node_loss(f.pending, replace(f.obligation, envelope=f.envelope))
        assert f.core.owner_table.snapshot(f.ref.object_id).inline_data == f.payload
        assert f.child_table.snapshot(f.child) == before and not f.calls
        assert f.core._recovery.task_record(f.pending.task_id).state is TaskState.SUCCEEDED
        assert f.core._recovery.task_record(f.pending.task_id).retries_started == 0
        assert f.core._finish_pending_task(f.pending)
        assert f.core._drive_output_publication_adoption(f.pending, _OutputAdoptionObligation(f.envelope, f.publisher))
        assert not f.calls and not f.core._protocol_unresolved
    finally:
        f.close()


@pytest.mark.parametrize("known", (True, False), ids=("known-complete", "completion-unknown"))
def test_late_envelope_cannot_reverse_a_latched_drop(known):
    f = _LossFixture(known=known)
    arrivals = []
    try:
        def release(address, handler, request):
            if not arrivals:
                assert f.core._output_loss_choices[f.identity] is False
                arrivals.append(f.envelope)
                assert not f.core._drive_output_publication_adoption(f.pending, _OutputAdoptionObligation(f.envelope, f.publisher))
                assert f.identity not in f.core._output_result_custody
            return f.release(address, handler, request)
        f.core._borrow_rpc = release
        assert f.core._drive_output_node_loss(f.pending, f.obligation) is known
        state = f.core.owner_table.snapshot(f.ref.object_id)
        assert state.state is (ObjectState.LOST if known else ObjectState.PENDING) and state.inline_data is None
        assert f.core._recovery.task_record(f.pending.task_id).retries_started == (0 if known else 1)
        assert len(arrivals) == 1 and len(f.calls) == 2
        assert f.identity not in f.core._output_result_custody and not f.core._protocol_unresolved
        assert f.core._drive_output_publication_adoption(f.pending, _OutputAdoptionObligation(f.envelope, f.publisher))
        assert f.core.owner_table.snapshot(f.ref.object_id) == state and len(f.calls) == 2
    finally:
        f.close()


def test_adoption_rpc_error_after_other_lane_finished_cannot_reinsert_old_work():
    from tests.unit.test_core_output_publication import _fixture, _close
    fixture, node, core, pending, reply, calls, rpc = _fixture(refs=False, stored=False)
    observed = []
    try:
        def finish_during_ack(address, handler, request):
            result = rpc(address, handler, request)
            if handler == ep.PUBLICATION_HANDLER:
                return result
            assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
            incarnation = fixture.manifest.header.node_incarnation
            death = protocol.NodeDeathRecord(
                "adoption-ack-node-exit", node.node_id, incarnation.node_pid, incarnation.registration_epoch,
                1, 7, protocol.NodeDeathReason.PROCESS_EXIT, "explicit member fact",
            )
            core._dead_nodes[node.node_id] = death
            assert core._drive_output_node_loss(pending, _OutputNodeLossObligation(fixture.id, death))
            assert core._finish_pending_task(pending)
            observed.append(request)
            raise TimeoutError("old adoption RPC returned after completion")
        core._rpc = finish_during_ack
        assert core._drive_output_publication_adoption(pending, _OutputAdoptionObligation(reply.output_publication, node.node_id))
        assert len(observed) == 1 and not core._protocol_unresolved and not core._output_result_custody
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.READY_INLINE
        assert not any(type(item) is _DelayedReadyTask for item in tuple(core._submissions.queue))
    finally:
        _close(core)
