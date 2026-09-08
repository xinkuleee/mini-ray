"""Pure interleavings for output custody, Node loss and stale Core work.

One two-slot task, optionally one one-byte local input, one in-memory GCS and
one threadless Core. The input-hold cases use at most two task identities,
one cleanup ACK loss, one exact replay and one subsequent attempt. Calls into
the other lane are synchronous hooks inside a fake RPC, not real concurrency.
There are no child refs, sockets, producers, timers, waits or processes.
"""

from dataclasses import replace
import threading

import pytest

from miniray import output_protocol as wire, protocol
from miniray.contained_edges import LineageReferenceEdge
from miniray.core import (
    _DelayedReadyTask, _OutputAdoptionObligation, _OutputNodeLossObligation,
    _ObjectWaiter, _PendingTask,
    _WAKE_COORDINATOR,
)
from miniray.errors import SystemTaskError
from miniray.ids import ObjectID, TaskID
from miniray.output_publication import OutputPublicationEnvelope
from miniray.output_recovery import OutputRecoveryAction, OutputRecoveryOwnerDecision
from miniray.ownership import ObjectState
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import make_pure_core, close_pure_core
from tests.unit.test_output_publication_control import _service, _no_runtime


pytestmark = pytest.mark.unit


def _fixture(monkeypatch, *, known=True, with_input=False):
    service, values = _service(monkeypatch, refs=False)
    values.envelope = OutputPublicationEnvelope(values.manifest, values.witness, values.results)
    registry = service.publications.output_recovery
    registry.report_intent(values.manifest)
    registry.arm_complete(values.publication_id, values.manifest.manifest_digest)
    if known:
        registry.report_terminal(values.witness)
    node = values.header.node_incarnation
    death = service.publications.commit_node_death(lambda: service.nodes.report_death(
        protocol.ReportNodeDeath(
            "core-publisher-exit", node.node_id, node.node_pid, node.registration_epoch,
            1, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed publisher exit",
        )
    )).death
    core = make_pure_core()
    core.worker_id, core.job_id, core.node_id = values.owner, values.job, values.node
    core.gcs_address = ("gcs.invalid", 1)
    input_id = ObjectID.for_task(TaskID(bytes.fromhex("42" * 16))) if with_input else None
    dependency_hold = (protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.SUBMITTED, core.worker_id,
        values.task, values.attempt,
    ) if with_input else None)
    spec = protocol.TaskSpec(
        values.job, values.task, values.attempt,
        protocol.FunctionKey(values.job, __name__, "producer", "v1"),
        () if input_id is None else (protocol.RefArg(input_id, core.worker_id),),
        2, ResourceVector({"CPU": 1}), values.owner, max_retries=3,
    )
    core.owner_table.register_task_outputs(spec, local_tokens=("outer0", "outer1"))
    core._recovery.register_task(spec, max_retries=3)
    core._objects = {object_id: _ObjectWaiter(threading.Event()) for object_id in spec.return_ids()}
    if input_id is not None:
        core.owner_table.register(input_id, current_attempt=None, local_token="input-local")
        assert core.owner_table.publish_inline(input_id, None, b"i")
        assert core._recovery.register_put(input_id)
        core._objects[input_id] = _ObjectWaiter(threading.Event())
        core._objects[input_id].event.set()
        assert core.owner_table.add_submitted_reference(input_id, dependency_hold)
        token = "node-loss-input-lineage"
        assert core.owner_table.add_lineage_reference(input_id, token)
        assert core.owner_table.add_outgoing_lineage_edge(
            spec.return_ids()[0], LineageReferenceEdge(spec.return_ids()[0], input_id, token),
        )
    pending = _PendingTask(
        spec.return_ids()[0], spec,
        protected_dependencies=() if input_id is None else (input_id,),
        dependency_hold=dependency_hold,
    )
    core._accepted_task_count = 1
    core._install_task_finish_barrier_locked(pending)
    core._dead_nodes[values.node] = death
    calls = []

    def rpc(_address, handler, request):
        calls.append((handler, request))
        if handler == wire.GET_OUTPUT_NODE_LOSS_HANDLER:
            return service.get_output_node_loss(request)
        if handler == wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER:
            return service.decide_output_node_loss(request)
        if handler == wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER:
            return service.progress_output_node_loss(request)
        pytest.fail("unexpected runtime effect: " + handler)

    core._rpc = rpc
    obligation = _OutputNodeLossObligation(values.publication_id, death)
    return service, values, core, pending, obligation, calls, rpc


def _close(core):
    for object_id in core._objects:
        for token in core.owner_table.snapshot(object_id).local_tokens:
            core.owner_table.release_local_reference(object_id, token)
    close_pure_core(core)


def _consume_queued(core, expected_type):
    """Consume a bounded pure FIFO snapshot without timers or wait calls."""
    size = core._submissions.qsize()
    assert size <= 4
    selected = []
    for _ in range(size):
        item = core._submissions.get_nowait()
        core._submissions.task_done()
        if isinstance(item, expected_type):
            selected.append(item)
        else:
            assert item is _WAKE_COORDINATOR
    assert len(selected) == 1
    return selected[0]


def _lose_final_cleanup_ack(core, calls, rpc):
    lost = []

    def effect_then_error(address, handler, request):
        reply = rpc(address, handler, request)
        assert len(calls) <= 4
        if handler == wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER and not lost:
            assert reply.snapshot.resolution is not None
            lost.append(reply)
            raise TimeoutError("final output cleanup ACK lost after effect")
        return reply

    core._rpc = effect_then_error
    return lost


@pytest.mark.parametrize("known", (True, False), ids=("known-complete", "completion-unknown"))
def test_no_envelope_cleanup_ack_loss_fences_local_state_until_exact_replay(monkeypatch, known):
    service, values, core, pending, obligation, calls, rpc = _fixture(monkeypatch, known=known)
    registry = service.publications.output_recovery
    before_owner = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
    before_record = replace(core._recovery.task_record(pending.task_id))
    lost = _lose_final_cleanup_ack(core, calls, rpc)
    try:
        assert obligation.envelope is None
        assert not core._execute(pending, pending.spec, output_node_loss=obligation)
        assert len(lost) == 1
        resolved = registry.snapshot(values.publication_id)
        assert resolved == lost[0].snapshot
        assert resolved.recovery_action is (
            OutputRecoveryAction.POSTCOMPLETE_RESOLVE if known
            else OutputRecoveryAction.COMPLETION_UNKNOWN
        )
        resolution = resolved.resolution
        assert resolution is not None and resolution.kept_slots == ()
        assert resolution.complete == (values.witness if known else None)
        assert resolved.owner_decision.complete == resolution.complete
        assert all(slot.decision is OutputRecoveryOwnerDecision.DROP
                   for slot in resolved.owner_decision.slots)
        assert before_record.state is TaskState.PENDING and before_record.retries_started == 0
        # GCS committed the cleanup, but Core has not observed its final ACK.
        assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before_owner
        assert core._recovery.task_record(pending.task_id) == before_record
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {output: pending for output in pending.output_ids}
        assert not core._finish_pending_task(pending)
        assert pending.task_key not in core._finished_tasks
        marker = core._protocol_unresolved[pending.task_key]
        assert marker.phase == "output_node_loss_wait" and marker.pending == pending
        assert marker.obligation == replace(obligation, round=1)
        assert not core._output_result_custody and not core._stored_descriptors
        assert all(not core._objects[output].event.is_set() for output in pending.output_ids)
        assert [handler for handler, _ in calls] == [
            wire.GET_OUTPUT_NODE_LOSS_HANDLER, wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER,
            wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER,
        ]
        delayed = _consume_queued(core, _DelayedReadyTask)
        assert delayed.ready.pending == pending
        assert delayed.ready.output_node_loss == marker.obligation
        # No wall-clock scheduling here: drive the exact saved work manually.
        terminal = core._execute(
            delayed.ready.pending, delayed.ready.spec,
            output_node_loss=delayed.ready.output_node_loss,
        )
        assert terminal is known
        assert calls[-1] == calls[0]  # exact query replay, no repeated effect
        assert len(calls) == 4 and len(lost) == 1
        assert registry.snapshot(values.publication_id) == resolved
        assert not core._protocol_unresolved and not core._output_result_custody
        assert not core._output_loss_drivers and not core._stored_descriptors
        assert values.publication_id in core._output_loss_completed
        record = core._recovery.task_record(pending.task_id)
        if known:
            assert record.state is TaskState.SUCCEEDED
            assert record.current_attempt == pending.spec.attempt_id and record.retries_started == 0
            assert all(core.owner_table.snapshot(output).state is ObjectState.LOST
                       for output in pending.output_ids)
            assert all(core._objects[output].event.is_set() for output in pending.output_ids)
            assert core._accepted_task_count == 1
            assert core._task_finish_barriers == {output: pending for output in pending.output_ids}
            assert tuple(core._submissions.queue) == (_WAKE_COORDINATOR,) * 2
            # Completion is not reconstruction admission; the finish barrier
            # must retire before an explicit request can spend retry budget.
            assert core._start_or_join_reconstruction(pending.object_id, core._objects[pending.object_id]) is None
            assert record.retries_started == 0
            assert core._finish_pending_task(pending)
            assert core._accepted_task_count == 0 and not core._task_finish_barriers
            core._start_or_join_reconstruction(pending.object_id, core._objects[pending.object_id])
        else:
            assert record.state is TaskState.RETRY_PENDING
            assert record.current_attempt == pending.spec.attempt_id.next()
            assert record.retries_started == 1
            assert all(core.owner_table.snapshot(output).state is ObjectState.PENDING
                       for output in pending.output_ids)
            assert all(not core._objects[output].event.is_set() for output in pending.output_ids)
            assert not core._finish_pending_task(pending)
        next_pending = _consume_queued(core, _PendingTask)
        assert next_pending.spec.attempt_id == pending.spec.attempt_id.next()
        assert next_pending.output_ids == pending.output_ids
        assert core._recovery.task_record(pending.task_id).retries_started == 1
        assert core._recovery.task_record(pending.task_id).retries_remaining == 2
        assert core._recovery.active_recovery(pending.task_id) == (
            next_pending.spec.attempt_id if known else None
        )
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers == {output: next_pending for output in pending.output_ids}
        before_replay_owner = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
        before_replay_record = replace(core._recovery.task_record(pending.task_id))
        assert core._execute(pending, pending.spec, output_node_loss=delayed.ready.output_node_loss)
        assert not core._finish_pending_task(pending)
        assert len(calls) == 4 and core._submissions.empty()
        assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before_replay_owner
        assert core._recovery.task_record(pending.task_id) == before_replay_record
    finally:
        _close(core)


@pytest.mark.parametrize("known", (True, False), ids=("new-reconstruction-hold", "retained-system-retry-hold"))
def test_old_delayed_completion_cannot_release_current_attempt_input_hold(monkeypatch, known):
    service, values, core, pending, obligation, calls, rpc = _fixture(
        monkeypatch, known=known, with_input=True,
    )
    input_id = pending.protected_dependencies[0]
    original_hold = pending.dependency_hold
    before_input = core.owner_table.snapshot(input_id)
    assert before_input.submitted_tokens == frozenset((original_hold,))
    assert before_input.lineage_tokens == frozenset(("node-loss-input-lineage",))
    lost = _lose_final_cleanup_ack(core, calls, rpc)
    try:
        assert not core._execute(pending, pending.spec, output_node_loss=obligation)
        delayed = _consume_queued(core, _DelayedReadyTask)
        assert len(lost) == 1
        assert service.publications.output_recovery.snapshot(values.publication_id).resolution is not None
        assert not core._finish_pending_task(pending)
        assert core.owner_table.snapshot(input_id) == before_input
        assert core._accepted_task_count == 1
        assert core._execute(
            pending, pending.spec, output_node_loss=delayed.ready.output_node_loss,
        ) is known
        assert core.owner_table.snapshot(input_id) == before_input
        if known:
            assert core._finish_pending_task(pending)
            assert core.owner_table.snapshot(input_id).submitted_tokens == frozenset()
            assert core.owner_table.snapshot(input_id).lineage_tokens == before_input.lineage_tokens
            core._start_or_join_reconstruction(pending.object_id, core._objects[pending.object_id])
        else:
            assert not core._finish_pending_task(pending)
        next_pending = _consume_queued(core, _PendingTask)
        hold = next_pending.dependency_hold
        assert type(hold) is protocol.TaskReferenceHold
        assert hold.kind is protocol.TaskReferenceHoldKind.SUBMITTED
        assert hold.submitting_worker_id == core.worker_id and hold.task_id == pending.task_id
        assert next_pending.protected_dependencies == (input_id,)
        assert core._recovery.task_record(pending.task_id).retries_started == 1
        if known:
            assert hold != original_hold and hold.origin_attempt_id == next_pending.spec.attempt_id
        else:
            assert hold == original_hold  # system retries retain one logical input hold
        retained_input = core.owner_table.snapshot(input_id)
        assert retained_input.submitted_tokens == frozenset((hold,))
        assert retained_input.lineage_tokens == before_input.lineage_tokens
        assert retained_input.inline_data == b"i" and retained_input.state is ObjectState.READY_INLINE
        before_outputs = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
        before_record = replace(core._recovery.task_record(pending.task_id))
        barriers = dict(core._task_finish_barriers)
        accepted = core._accepted_task_count
        # Old delayed_execute and its later finalizer cannot release the new
        # attempt's live hold, even though whole-task retries share task_key.
        for _ in range(2):
            assert core._execute(
                delayed.ready.pending, delayed.ready.spec,
                output_node_loss=delayed.ready.output_node_loss,
            )
            assert not core._finish_pending_task(pending)
            assert core.owner_table.snapshot(input_id) == retained_input
            assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before_outputs
            assert core._recovery.task_record(pending.task_id) == before_record
            assert core._task_finish_barriers == barriers
            assert core._accepted_task_count == accepted == 1
            assert len(calls) == 4 and core._submissions.empty()
        # The retained hold is real, not a dummy set: only the matching new
        # attempt's terminal transition/finish releases it.
        assert core._publish_task_error(next_pending, SystemTaskError("stop pure retry"))
        assert core._finish_pending_task(next_pending)
        assert core.owner_table.snapshot(input_id).submitted_tokens == frozenset()
        assert core.owner_table.snapshot(input_id).lineage_tokens == before_input.lineage_tokens
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
    finally:
        _close(core)


@pytest.mark.parametrize("known", (True, False), ids=("known-complete", "completion-unknown"))
def test_envelope_arriving_during_query_is_seen_before_immutable_keep_drop(monkeypatch, known):
    service, values, core, pending, obligation, calls, rpc = _fixture(monkeypatch, known=known)
    arrivals = []

    def receive_during_query(address, handler, request):
        reply = rpc(address, handler, request)
        if handler == wire.GET_OUTPUT_NODE_LOSS_HANDLER and not arrivals:
            arrivals.append(values.envelope)
            assert not core._drive_output_publication_adoption(pending, _OutputAdoptionObligation(
                values.envelope, values.node,
            ))
        return reply

    core._rpc = receive_during_query
    try:
        assert core._drive_output_node_loss(pending, obligation)
        snapshot = service.publications.output_recovery.snapshot(values.publication_id)
        assert snapshot.resolution.kept_slots == (0,)
        # The immutable GCS work keeps its original knowledge; a real late
        # envelope supplies a witness to the owner decision, not a retroactive
        # terminal report after the Node-death fence.
        assert snapshot.complete == (values.witness if known else None)
        assert snapshot.owner_decision.complete == snapshot.resolution.complete == values.witness
        assert snapshot.recovery_action is (OutputRecoveryAction.POSTCOMPLETE_RESOLVE if known
                                           else OutputRecoveryAction.COMPLETION_UNKNOWN)
        record = core._recovery.task_record(pending.task_id)
        assert record.state is TaskState.SUCCEEDED and record.retries_started == 0
        assert record.current_attempt == pending.spec.attempt_id
        assert core.owner_table.snapshot(pending.output_ids[0]).inline_data == values.payloads[0]
        assert core.owner_table.snapshot(pending.output_ids[1]).state is ObjectState.LOST
        assert not core._protocol_unresolved and not core._output_result_custody
        assert core._finish_pending_task(pending)
        before_calls = tuple(calls)
        assert core._drive_output_node_loss(pending, obligation)
        assert core._drive_output_publication_adoption(pending, _OutputAdoptionObligation(values.envelope, values.node))
        assert tuple(calls) == before_calls and not core._protocol_unresolved
    finally:
        _close(core)


@pytest.mark.parametrize("known", (True, False), ids=("known-complete", "completion-unknown"))
def test_late_envelope_cannot_reverse_a_latched_drop(monkeypatch, known):
    service, values, core, pending, obligation, calls, rpc = _fixture(monkeypatch, known=known)
    arrivals = []

    def receive_after_decision(address, handler, request):
        if handler == wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER and not arrivals:
            arrivals.append(True)
            choice = core._output_loss_choices[values.publication_id]
            assert all(slot.decision is OutputRecoveryOwnerDecision.DROP for slot in choice.slots)
            assert not core._drive_output_publication_adoption(pending, _OutputAdoptionObligation(
                values.envelope, values.node,
            ))
            assert values.publication_id not in core._output_result_custody
        return rpc(address, handler, request)

    core._rpc = receive_after_decision
    try:
        assert core._drive_output_node_loss(pending, obligation) is known
        snapshot = service.publications.output_recovery.snapshot(values.publication_id)
        assert snapshot.resolution.kept_slots == ()
        assert snapshot.complete == snapshot.owner_decision.complete == snapshot.resolution.complete == (values.witness if known else None)
        assert all(core.owner_table.snapshot(output).state is (ObjectState.LOST if known else ObjectState.PENDING)
                   for output in pending.output_ids)
        record = core._recovery.task_record(pending.task_id)
        assert record.state is (TaskState.SUCCEEDED if known else TaskState.RETRY_PENDING)
        assert record.retries_started == (0 if known else 1)
        assert not core._protocol_unresolved and not core._output_result_custody
        assert core._finish_pending_task(pending) is known
        if not known:
            next_pending = _consume_queued(core, _PendingTask)
            assert next_pending.spec.attempt_id == pending.spec.attempt_id.next()
            assert core._task_finish_barriers == {output: next_pending for output in pending.output_ids}
            before_owner = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
            before_record = replace(record)
            before_calls = tuple(calls)
            assert core._drive_output_publication_adoption(
                pending, _OutputAdoptionObligation(values.envelope, values.node),
            )
            assert tuple(calls) == before_calls and core._submissions.empty()
            assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before_owner
            assert core._recovery.task_record(pending.task_id) == before_record
            assert not core._output_result_custody
    finally:
        _close(core)


def test_adoption_rpc_error_after_other_lane_finished_cannot_reinsert_old_work(monkeypatch):
    service, values, core, pending, obligation, calls, rpc = _fixture(monkeypatch)
    core._dead_nodes.clear()  # owner has not consumed the committed GCS death yet
    finished_during_rpc = []

    def finish_during_report(address, handler, request):
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            assert isinstance(request, wire.ReportOutputPublicationTerminal)
            core._dead_nodes[values.node] = obligation.node_death
            assert core._drive_output_node_loss(pending, obligation)
            assert core._finish_pending_task(pending)
            finished_during_rpc.append(True)
            raise TimeoutError("old adoption RPC returned after completion")
        return rpc(address, handler, request)

    core._rpc = finish_during_report
    try:
        assert core._drive_output_publication_adoption(pending, _OutputAdoptionObligation(values.envelope, values.node))
        assert finished_during_rpc == [True]
        assert not core._protocol_unresolved and not core._output_result_custody
        assert core._accepted_task_count == 0
        # Two object wakes plus the finalizer wake; no delayed old execution.
        assert tuple(core._submissions.queue) == (_WAKE_COORDINATOR,) * 3
        assert core.owner_table.snapshot(pending.output_ids[0]).state is ObjectState.READY_INLINE
        assert core.owner_table.snapshot(pending.output_ids[1]).state is ObjectState.LOST
    finally:
        _close(core)


def test_task_attempt_fences_old_target_receipt_even_when_its_slots_are_unchanged(monkeypatch):
    from tests.unit.test_targeted_output_publication import _Fixture, _envelope
    from miniray.ownership import OutputOwnerPublicationPlan

    values = _Fixture()
    session = values.start()
    envelope = _envelope(values, session)
    attempt = session.execution.attempt_id
    values.owner.mark_lost(values.outputs[1], values.spec.attempt_id)
    values.coordinator.request(values.outputs[1], values.spec.attempt_id)
    values.coordinator.commit_output_publication_success(values.coordinator.validate_output_publication_success(envelope))
    successor = values.coordinator.start(values.spec.task_id)
    assert values.owner.output_owner_publication_receipt(OutputOwnerPublicationPlan(session.execution, envelope)).committed
    assert all(values.owner.snapshot(output).current_attempt == attempt for output in session.target_output_ids)
    core = make_pure_core()
    core.worker_id, core.job_id = values.spec.owner_worker_id, values.spec.job_id
    core._owner_table, core._recovery = values.owner, values.recovery
    pending = _PendingTask(
        session.target_output_ids[0], replace(values.spec, attempt_id=attempt),
        target_execution=session.execution, reconstruction_origin_attempt=attempt,
    )
    node = envelope.manifest.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "old-target-exit", node.node_id, node.node_pid, node.registration_epoch,
        1, 1, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed",
    )
    before = values.owner_snapshot(), values.recovery_snapshot()
    try:
        assert core._drive_output_publication_adoption(pending, _OutputAdoptionObligation(envelope, node.node_id))
        assert core._drive_output_node_loss(pending, _OutputNodeLossObligation(envelope.publication_id, death))
        assert not core._protocol_unresolved and core._submissions.empty()
        assert (values.owner_snapshot(), values.recovery_snapshot()) == before
        assert values.coordinator.current_session(values.spec.task_id) is successor
    finally:
        close_pure_core(core)


@pytest.mark.parametrize("publisher_dies", (False, True))
def test_new_sibling_attempt_does_not_abandon_committed_batch_adoption_tail(monkeypatch, publisher_dies):
    from tests.unit.test_targeted_output_publication import _Fixture, _envelope
    from miniray.core import _DelayedReadyTask
    from miniray.output_recovery import OutputPublicationRecoveryAuthority, OutputRecoveryResolution

    values = _Fixture()
    session = values.start()
    envelope = _envelope(values, session)
    attempt = session.execution.attempt_id
    values.owner.mark_lost(values.outputs[1], values.spec.attempt_id)
    values.coordinator.request(values.outputs[1], values.spec.attempt_id)
    core = make_pure_core()
    core.worker_id, core.job_id = values.spec.owner_worker_id, values.spec.job_id
    core._owner_table, core._recovery = values.owner, values.recovery
    core._targeted_reconstruction = values.coordinator
    pending = _PendingTask(
        session.target_output_ids[0], replace(values.spec, attempt_id=attempt),
        target_execution=session.execution, reconstruction_origin_attempt=attempt,
    )
    core._accepted_task_count = 1
    core._install_task_finish_barrier_locked(pending)
    core._objects = {output: _ObjectWaiter(threading.Event()) for output in values.outputs}
    # Capacity retry is scheduling metadata, not a new execution or hold.
    pending = replace(pending, capacity_round=1)
    registry = OutputPublicationRecoveryAuthority()
    registry.report_intent(envelope.manifest)
    registry.arm_complete(envelope.publication_id, envelope.manifest.manifest_digest)
    core.gcs_address = ("gcs.invalid", 1)
    core._resolve_node_address = lambda _node: ("node.invalid", 1)
    calls, next_pending = [], []
    frozen = []

    def rpc(_address, handler, request):
        calls.append((handler, request))
        if handler == wire.GET_OUTPUT_NODE_LOSS_HANDLER:
            return wire.GetOutputNodeLossReply(request, True, frozen[0], registry.snapshot(envelope.publication_id))
        if handler == wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER:
            decision = request.decision
            ack = registry.decide_owner(
                request.work, decision.owner_worker_id, decision.slots,
                decision_id=decision.decision_id, complete=decision.complete,
            )
            return wire.OutputNodeLossReply(request, ack.snapshot, True)
        if handler == wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER:
            decision = registry.snapshot(envelope.publication_id).owner_decision
            resolution = OutputRecoveryResolution(
                envelope.publication_id, envelope.manifest.manifest_digest, frozen[0].death,
                core.worker_id, "old-batch-no-child-cleanup",
                tuple(slot.slot_index for slot in decision.slots
                      if slot.decision is OutputRecoveryOwnerDecision.KEEP), decision.complete,
            )
            return wire.OutputNodeLossReply(request, registry.resolve_node_loss(request.work, resolution).snapshot, True)
        if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
            if isinstance(request, wire.ReportOutputPublicationTerminal):
                return wire.OutputRecoveryReply(request, registry.report_terminal(request.witness))
            assert isinstance(request, wire.ReportOutputPublicationAdopted)
            ack = registry.report_adopted(request.proof)
            if not next_pending:
                successor = values.coordinator.start(values.spec.task_id)
                other = _PendingTask(
                    successor.target_output_ids[0],
                    replace(values.spec, attempt_id=successor.execution.attempt_id),
                    target_execution=successor.execution,
                    reconstruction_origin_attempt=successor.execution.attempt_id,
                )
                core._install_task_finish_barrier_locked(other)
                core._accepted_task_count += 1
                core._mark_protocol_unresolved(other, "successor-push")
                next_pending.append(other)
                raise TimeoutError("adopted ACK lost after sibling START")
            return wire.OutputRecoveryReply(request, ack)
        assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
        return wire.AckOutputPublicationAdoptedReply(request, True)

    core._rpc = rpc
    obligation = _OutputAdoptionObligation(envelope, envelope.manifest.header.node_incarnation.node_id)
    try:
        assert not core._drive_output_publication_adoption(pending, obligation)
        other = next_pending[0]
        assert pending.task_key in core._protocol_unresolved
        assert other.task_key in core._protocol_unresolved
        assert not core._finish_pending_task(pending)
        before_owner, before_recovery = values.owner_snapshot(), values.recovery_snapshot()
        successor_marker = core._protocol_unresolved[other.task_key]
        delayed = next(item for item in tuple(core._submissions.queue) if isinstance(item, _DelayedReadyTask))
        calls_before = len(calls)
        # The old selected slots still have their exact committed receipt.
        # Only remote adoption ACKs remain, never another CAS/success/wake.
        def forbidden(*_args):
            pytest.fail("metadata-tail replay changed local publication or successor")
        monkeypatch.setattr(values.owner, "commit_output_publication", forbidden)
        monkeypatch.setattr(values.recovery, "validate_task_success", forbidden)
        monkeypatch.setattr(values.coordinator, "complete_lost_output_publication", forbidden)
        if publisher_dies:
            node = envelope.manifest.header.node_incarnation
            death = protocol.NodeDeathRecord(
                "tail-publisher-exit", node.node_id, node.node_pid, node.registration_epoch,
                1, 1, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed",
            )
            frozen.extend(registry.freeze_node_death(death))
            core._dead_nodes[node.node_id] = death
            wake = core._wake_object
            def wake_old_output(output):
                assert output in pending.output_ids
                wake(output)
            monkeypatch.setattr(core, "_wake_object", wake_old_output)
        else:
            monkeypatch.setattr(core, "_wake_object", forbidden)
        assert core._execute(pending, pending.spec, output_adoption=delayed.ready.output_adoption)
        if publisher_dies:
            assert [handler for handler, _request in calls[calls_before:]] == [
                wire.GET_OUTPUT_NODE_LOSS_HANDLER, wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER, wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER,
            ]
            assert core.owner_table.snapshot(pending.output_ids[0]).state is ObjectState.READY_INLINE
            assert core.owner_table.snapshot(pending.output_ids[1]).state is ObjectState.LOST
            assert core.owner_table.snapshot(other.object_id) == before_owner[1]
            assert values.recovery_snapshot() == before_recovery
            assert registry.snapshot(envelope.publication_id).resolution is not None
        else:
            assert [handler for handler, _request in calls[calls_before:]] == [
                wire.REPORT_OUTPUT_PUBLICATION_HANDLER, wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER,
            ]
            assert isinstance(calls[calls_before][1], wire.ReportOutputPublicationAdopted)
            assert (values.owner_snapshot(), values.recovery_snapshot()) == (before_owner, before_recovery)
        assert pending.task_key not in core._protocol_unresolved
        assert core._protocol_unresolved[other.task_key] == successor_marker
        assert core._finish_pending_task(pending) and core._accepted_task_count == 1
        assert core._task_finish_barriers == {other.object_id: other}
    finally:
        close_pure_core(core)
