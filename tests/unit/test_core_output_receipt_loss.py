"""Pure owner receipt/custody arbitration with a dead publishing Node.

Two tiny outputs, no contained children, one in-memory Node and Core. Replies
are typed metadata reducers; callbacks expose fixed interleavings without any
network, background thread, clock wait or producer reexecution.
"""

from dataclasses import replace

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import _OutputNodeLossObligation
from miniray.output_recovery import OutputRecoveryResolution, OutputRecoveryOwnerDecision
from miniray.ownership import ObjectState
from tests.unit._pure_core import close_pure_core
from tests.unit.test_core_output_publication import _fixture, _no_runtime


pytestmark = pytest.mark.unit


def _close(core):
    for output in core._objects:
        for token in core.owner_table.snapshot(output).local_tokens:
            core.owner_table.release_local_reference(output, token)
    close_pure_core(core)


def test_owner_cas_receipt_restores_inline_custody_for_node_loss_after_scratch_cache_disappears(monkeypatch):
    fixture, node, core, pending, reply, _calls, original_rpc = _fixture(refs=False)
    identity, envelope = fixture.id, reply.output_publication
    commit = core.owner_table.commit_output_publication

    def effect_then_error(plan):
        receipt = commit(plan)
        assert receipt.committed
        raise RuntimeError("owner committed before local handoff failed")

    monkeypatch.setattr(core.owner_table, "commit_output_publication", effect_then_error)
    try:
        assert not core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=identity.lease_id)
        assert core.owner_table.snapshot(pending.output_ids[0]).state is ObjectState.READY_INLINE
        # Only the transient cache disappears. Authoritative per-slot data,
        # manifest membership and the committed receipt are deliberately kept.
        core._output_result_custody.clear()
        core._protocol_unresolved.clear()
        incarnation = envelope.manifest.header.node_incarnation
        death = protocol.NodeDeathRecord(
            "receipt-publisher-exit", node.node_id, incarnation.node_pid, incarnation.registration_epoch,
            1, 1, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed exit",
        )
        (work,) = fixture.recovery.freeze_node_death(death)
        core._dead_nodes[node.node_id] = death
        core.owner_table.remove_node_locations(node.node_id)
        assert core.owner_table.snapshot(pending.output_ids[1]).state is ObjectState.LOST
        requests = []

        def rpc(_address, handler, request):
            requests.append(request)
            if handler == wire.GET_OUTPUT_NODE_LOSS_HANDLER:
                return wire.GetOutputNodeLossReply(request, True, work, fixture.recovery.snapshot(identity))
            if handler == wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER:
                decision = request.decision
                ack = fixture.recovery.decide_owner(work, core.worker_id, decision.slots,
                    decision_id=decision.decision_id, complete=decision.complete)
                return wire.OutputNodeLossReply(request, ack.snapshot, True)
            assert handler == wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER
            decision = fixture.recovery.snapshot(identity).owner_decision
            resolution = OutputRecoveryResolution(identity, envelope.manifest.manifest_digest, death, core.worker_id,
                "receipt-loss-cleanup", tuple(slot.slot_index for slot in decision.slots
                if slot.decision is OutputRecoveryOwnerDecision.KEEP), decision.complete)
            return wire.OutputNodeLossReply(request, fixture.recovery.resolve_node_loss(work, resolution).snapshot, True)

        core._rpc = rpc
        monkeypatch.setattr(core, "_retry_system_failure", lambda *_: pytest.fail("known owner result became ordinary retry"))
        assert core._drive_output_node_loss(pending, _OutputNodeLossObligation(identity, death))
        assert len(requests) == 3
        assert core._output_loss_choices[identity].complete == envelope.complete
        assert fixture.recovery.snapshot(identity).resolution.kept_slots == (0,)
        assert core.owner_table.snapshot(pending.output_ids[0]).inline_data == fixture.values.payloads[0]
        assert core.owner_table.snapshot(pending.output_ids[1]).state is ObjectState.LOST
        assert core.owner_table.snapshot(pending.output_ids[1]).canonical_stored_result is None
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert core._finish_pending_task(pending)
    finally:
        _close(core)


@pytest.mark.parametrize("contradiction", ("absent", "precomplete"))
def test_known_complete_cannot_be_erased_by_absent_or_precomplete_recovery_reply(monkeypatch, contradiction):
    fixture, node, core, pending, reply, _calls, _rpc = _fixture(refs=False)
    identity, envelope = fixture.id, reply.output_publication
    incarnation = envelope.manifest.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "contradictory-history", node.node_id, incarnation.node_pid, incarnation.registration_epoch,
        1, 1, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed",
    )
    from miniray.output_recovery import OutputPublicationRecoveryAuthority
    precomplete = OutputPublicationRecoveryAuthority()
    precomplete.report_intent(envelope.manifest)
    (work,) = precomplete.freeze_node_death(death)
    calls = []
    def rpc(_address, handler, request):
        calls.append(request)
        assert handler == wire.GET_OUTPUT_NODE_LOSS_HANDLER
        return (wire.GetOutputNodeLossReply(request, False) if contradiction == "absent" else
                wire.GetOutputNodeLossReply(request, True, work, precomplete.snapshot(identity)))
    core._rpc = rpc
    core._dead_nodes[node.node_id] = death
    monkeypatch.setattr(core, "_retry_system_failure", lambda *_: pytest.fail("contradictory history authorized retry"))
    before = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
    try:
        assert not core._drive_output_node_loss(pending, _OutputNodeLossObligation(identity, death, envelope))
        assert len(calls) == 1
        assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before
        assert core._output_result_custody[identity] == envelope
        assert pending.task_key in core._protocol_unresolved
        assert not core._finish_pending_task(pending)
    finally:
        _close(core)


def test_node_loss_owner_cas_effect_then_error_reuses_resolution_without_second_cleanup(monkeypatch):
    from tests.unit.test_core_output_node_loss import _fixture as loss_fixture
    service, values, core, pending, obligation, calls, _rpc = loss_fixture(monkeypatch)
    original = core.owner_table.resolve_output_node_loss
    owner_calls = []
    def apply_then_error(manifest, resolution, envelope, *, unavailable_nodes=()):
        result = original(manifest, resolution, envelope, unavailable_nodes=unavailable_nodes)
        owner_calls.append((manifest, resolution, envelope, unavailable_nodes))
        if len(owner_calls) == 1:
            raise RuntimeError("Node-loss owner CAS applied before local error")
        return result
    monkeypatch.setattr(core.owner_table, "resolve_output_node_loss", apply_then_error)
    try:
        assert not core._drive_output_node_loss(pending, obligation)
        assert len(owner_calls) == 1
        assert all(core.owner_table.snapshot(output).state is ObjectState.LOST for output in pending.output_ids)
        assert core._recovery.task_record(pending.task_id).state.value != "SUCCEEDED"
        assert pending.task_key in core._protocol_unresolved
        retained = core._protocol_unresolved[pending.task_key].obligation
        assert core._drive_output_node_loss(pending, retained)
        assert len(owner_calls) == 2 and owner_calls[0] == owner_calls[1]
        assert [handler for handler, _request in calls].count(wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER) == 1
        assert core._recovery.task_record(pending.task_id).state.value == "SUCCEEDED"
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert core._finish_pending_task(pending)
    finally:
        _close(core)


def test_owner_retirement_is_an_independent_shutdown_fence_without_protocol_marker():
    fixture, node, core, pending, reply, _calls, _rpc = _fixture(refs=False)
    try:
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert core._finish_pending_task(pending)
        core._accepting = False
        core._reference_mailbox.close_admission()
        assert not core._protocol_unresolved and not core._task_finish_barriers
        core.owner_table.mark_lost(pending.output_ids[1], pending.spec.attempt_id)
        member = core.owner_table.output_owner_publication(pending.output_ids[1])
        core.owner_table.begin_output_publication_retirement(
            (member,), retirement_id="independent-owner-retirement",
            replica_locations={member.object_id: (node.node_id,)},
        )
        assert core.owner_table.has_active_output_retirements()
        assert not core._shutdown_finalizable_locked(require_distributed_clean=True)
        assert not core._protocol_unresolved
        assert core.owner_table.snapshot(member.object_id).output_retirement_id == "independent-owner-retirement"
    finally:
        _close(core)


def test_owner_routed_targeted_retirement_never_holds_core_lock_across_rpc(monkeypatch):
    fixture, node, core, pending, reply, _calls, rpc = _fixture(refs=True)
    original_borrow = core._borrow_rpc
    def unlocked_rpc(address, handler, request):
        assert not core._state_lock._is_owned(), handler
        return rpc(address, handler, request)
    def unlocked_borrow(address, handler, request):
        assert not core._state_lock._is_owned(), handler
        return original_borrow(address, handler, request)
    try:
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert core._finish_pending_task(pending)
        lost = pending.output_ids[1]
        healthy = core.owner_table.snapshot(pending.output_ids[0])
        core.owner_table.mark_lost(lost, pending.spec.attempt_id)
        monkeypatch.setattr(core, "_rpc", unlocked_rpc)
        monkeypatch.setattr(core, "_borrow_rpc", unlocked_borrow)
        outcome = core._start_or_join_reconstruction(lost, core._objects[lost], return_requested_outcome=True)
        assert outcome.disposition.value == "START"
        assert core.owner_table.snapshot(pending.output_ids[0]) == healthy
        assert core.owner_table.snapshot(lost).current_attempt == pending.spec.attempt_id.next()
    finally:
        _close(core)
