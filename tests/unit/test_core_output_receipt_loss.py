"""Pure current owner receipts, surviving bytes and exact cleanup replay.

One accepted output and at most two child transfers in real owner/Node/journal
state. Faults affect only acknowledgements or transient scratch custody; the
committed owner bytes and exact metadata stay authoritative. No constructors,
network, background thread, process, wait or producer reexecution.
"""

from dataclasses import replace

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import _OutputNodeLossObligation
from miniray.ownership import ObjectState
from tests.unit.test_core_output_publication import _fixture, _close
from tests.unit.test_core_output_node_loss import _LossFixture
from tests.unit.test_publication_pg_loss_paths import _no_runtime


pytestmark = pytest.mark.unit


def test_owner_cas_receipt_restores_inline_custody_for_node_loss_after_scratch_cache_disappears(monkeypatch):
    fixture, node, core, pending, reply, calls, rpc = _fixture(refs=False, stored=False)
    identity, envelope = fixture.id, reply.output_publication
    commit = core.owner_table.commit_output_publication
    def effect_then_error(plan):
        receipt = commit(plan)
        assert receipt.committed
        raise RuntimeError("owner committed before local handoff failed")
    monkeypatch.setattr(core.owner_table, "commit_output_publication", effect_then_error)
    try:
        assert not core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=identity.lease_id)
        before = core.owner_table.snapshot(pending.object_id)
        assert before.state is ObjectState.READY_INLINE and before.inline_data == (fixture.values.payload)
        assert core.owner_table.output_owner_result(pending.object_id) == (envelope.result)
        assert core.owner_table.output_owner_publication(pending.object_id).manifest == envelope.manifest
        # Match the original recovery boundary: drop only transient custody,
        # retaining the real CAS receipt, owner bytes, handoff and finish marker.
        core._output_result_custody.clear()
        assert core._task_finish_barriers[pending.object_id] is pending
        incarnation = envelope.manifest.header.node_incarnation
        death = protocol.NodeDeathRecord(
            "receipt-publisher-exit", node.node_id, incarnation.node_pid, incarnation.registration_epoch,
            1, 7, protocol.NodeDeathReason.PROCESS_EXIT, "explicit Node loss fact",
        )
        core._dead_nodes[node.node_id] = death
        monkeypatch.setattr(core, "_retry_system_failure", lambda *_a, **_k: pytest.fail("owner bytes became ordinary retry"))
        assert core._drive_output_node_loss(pending, _OutputNodeLossObligation(identity, death))
        after = core.owner_table.snapshot(pending.object_id)
        assert after.state is ObjectState.READY_INLINE and after.inline_data == before.inline_data
        assert after.output_publication == before.output_publication
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert core._recovery.task_record(pending.task_id).state.value == "SUCCEEDED"
        assert core._finish_pending_task(pending)
    finally:
        _close(core)


@pytest.mark.parametrize("reported", (False, True), ids=("owner-unreported", "owner-known-complete"))
def test_actual_complete_envelope_survives_missing_owner_terminal_report(reported, monkeypatch):
    fixture, node, core, pending, reply, calls, rpc = _fixture(refs=False, stored=False, report_complete=reported)
    identity, envelope = fixture.id, reply.output_publication
    incarnation = envelope.manifest.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "owner-report-missing", node.node_id, incarnation.node_pid, incarnation.registration_epoch,
        1, 7, protocol.NodeDeathReason.PROCESS_EXIT, "explicit Node loss fact",
    )
    try:
        assert (fixture.handoffs.query(identity).complete is not None) is reported
        assert fixture.journal.snapshot(identity).complete == envelope.complete
        core._dead_nodes[node.node_id] = death
        monkeypatch.setattr(core, "_retry_system_failure", lambda *_a, **_k: pytest.fail("actual Complete became ordinary retry"))
        assert core._drive_output_node_loss(pending, _OutputNodeLossObligation(identity, death, envelope))
        after = core.owner_table.snapshot(pending.object_id)
        assert after.state is ObjectState.READY_INLINE and after.inline_data == (fixture.values.payload)
        assert fixture.handoffs.query(identity).complete == envelope.complete
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert core._finish_pending_task(pending)
    finally:
        _close(core)


def test_node_loss_owner_cas_effect_then_error_reuses_resolution_without_second_cleanup(monkeypatch):
    f = _LossFixture(known=True)
    core, pending = f.core, f.pending
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
        assert not core._drive_output_node_loss(pending, f.obligation)
        assert len(owner_calls) == 1 and len(f.calls) == 2
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.LOST
        assert core._recovery.task_record(pending.task_id).state.value != "SUCCEEDED"
        assert pending.task_key in core._protocol_unresolved
        assert core._drive_output_node_loss(pending, core._protocol_unresolved[pending.task_key].obligation)
        assert len(owner_calls) == 2 and owner_calls[0] == owner_calls[1]
        assert len(f.calls) == 2 and not f.child_table.snapshot(f.child).contained_holds
        assert core._recovery.task_record(pending.task_id).state.value == "SUCCEEDED"
        assert core._recovery.task_record(pending.task_id).retries_started == 0
        assert core._finish_pending_task(pending)
    finally:
        f.close()


def test_owner_retirement_is_an_independent_shutdown_fence_without_protocol_marker():
    fixture, node, core, pending, reply, calls, rpc = _fixture(refs=False, stored=True)
    try:
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert core._finish_pending_task(pending)
        core._reference_mailbox.drain()
        core._accepting = False
        assert not core._protocol_unresolved and not core._task_finish_barriers
        assert core.can_finalize_shutdown(require_distributed_clean=True)
        core.owner_table.mark_lost(pending.object_id, pending.spec.attempt_id)
        member = core.owner_table.output_owner_publication(pending.object_id)
        core.owner_table.begin_output_publication_retirement(
            member, retirement_id="independent-owner-retirement",
            replica_locations=(node.node_id,),
        )
        assert core.owner_table.has_active_output_retirements()
        assert not core.can_finalize_shutdown(require_distributed_clean=True)
        assert not core._protocol_unresolved and not core._task_finish_barriers
        assert core.owner_table.snapshot(member.object_id).output_retirement_id == "independent-owner-retirement"
    finally:
        _close(core)


def test_owner_routed_single_output_retirement_never_holds_core_lock_across_rpc(monkeypatch):
    fixture, node, core, pending, reply, calls, rpc = _fixture(refs=True, stored=True)
    borrow = core._borrow_rpc
    def unlocked_rpc(address, handler, request):
        assert not core._state_lock._is_owned(), handler
        return rpc(address, handler, request)
    def unlocked_borrow(address, handler, request):
        assert not core._state_lock._is_owned(), handler
        return borrow(address, handler, request)
    try:
        assert core._publish_reply(pending, reply, expected_node_id=node.node_id, expected_lease_id=fixture.id.lease_id)
        assert core._finish_pending_task(pending)
        core.owner_table.mark_lost(pending.object_id, pending.spec.attempt_id)
        monkeypatch.setattr(core, "_rpc", unlocked_rpc)
        monkeypatch.setattr(core, "_borrow_rpc", unlocked_borrow)
        outcome = core._start_or_join_reconstruction(pending.object_id, core._objects[pending.object_id], return_requested_outcome=True)
        assert outcome.disposition.value == "START"
        assert core.owner_table.snapshot(pending.object_id).current_attempt == pending.spec.attempt_id.next()
        assert not core.owner_table.has_active_output_retirements()
        assert sum(handler == "release_contained_reference" for handler, _ in calls) == 2
        assert sum(handler == "drop_object_replica" for handler, _ in calls) == 1
    finally:
        _close(core)
