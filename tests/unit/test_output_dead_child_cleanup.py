"""Pure Node rollback proof fallback: one tiny output and two child owners, no live RPC or threads."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
import socket
import threading
import time

import pytest

from miniray import protocol
from miniray.node import (
    GCS_GET_WORKER_STATE_HANDLER, NodeServer, RELEASE_CONTAINED_REFERENCE_HANDLER,
)
from miniray.output_publication import OutputPublicationConflictError
from miniray.output_publication_journal import OutputPublicationStage as Stage
from miniray.output_publication_node import OutputPublicationRemoteError
from miniray.transport import TransportTimeout
from tests.unit._pure_output_runtime import _metadata as _assert_metadata
from tests.unit.test_output_publication_node_server import _node


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("dead-child contract attempted real runtime work")

    monkeypatch.setattr(NodeServer, "__init__", forbidden)
    for kind, name in ((threading.Thread, "start"), (threading.Thread, "join"),
                       (threading.Timer, "start"), (threading.Event, "wait"),
                       (threading.Condition, "wait")):
        monkeypatch.setattr(kind, name, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _proof(worker_id, *, reason=protocol.WorkerDeathReason.PROCESS_EXIT, node_id=None,
           node_pid=3201, node_epoch=4, worker_pid=3202):
    incarnation = protocol.WorkerIncarnation(
        node_id or protocol.NodeID(b"g" * 16), node_pid, node_epoch, worker_id, worker_pid,
    )
    death = protocol.WorkerDeathRecord(
        "child-owner-exit", incarnation, 9, 17, reason,
    )
    return protocol.GetWorkerStateReply(
        worker_id, True, 9, protocol.WorkerMembershipState.DEAD, incarnation, death,
    )


def _release_case(*, foreign=False):
    fixture, node, record, _complete = _node()
    transfer = (fixture.manifest.value).transfers[1 if foreign else 0]
    request = protocol.ReleaseContainedReference(
        transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.final_hold,
    )
    proof = (_proof(request.owner_worker_id) if foreign else _proof(
        request.owner_worker_id, node_id=node.node_id, node_pid=node._node_pid,
        node_epoch=node._registration_epoch, worker_pid=node._workers[request.owner_worker_id].pid,
    ))
    if not foreign:
        node._workers[request.owner_worker_id].incarnation = node._copy_output_worker_incarnation(proof.incarnation)
    return fixture, node, record, request, transfer.contained_owner_address, proof


def _install_rpc(node, request, address, candidate, *, failure=None):
    calls = []
    release_failure = failure or TransportTimeout("original child release timed out")

    def rpc(actual_address, handler, message, **options):
        assert not node._state_lock._is_owned()
        assert not node._output_publication_journal._lock._is_owned()
        assert not node._output_publications._lock._is_owned()
        calls.append((actual_address, handler, message))
        if handler == RELEASE_CONTAINED_REFERENCE_HANDLER:
            assert actual_address == address and message == request
            raise release_failure
        assert handler == GCS_GET_WORKER_STATE_HANDLER
        assert actual_address == node._gcs_address
        assert type(message) is protocol.GetWorkerState and message.worker_id == request.owner_worker_id
        if isinstance(candidate, BaseException):
            raise candidate
        return candidate

    node._background_rpc = rpc
    return calls, release_failure


@pytest.mark.parametrize("foreign", (False, True))
@pytest.mark.parametrize("reason", (protocol.WorkerDeathReason.PROCESS_EXIT, protocol.WorkerDeathReason.NODE_EXIT))
def test_failed_release_returns_exact_registered_death_evidence_not_a_fake_ack(foreign, reason):
    fixture, node, _record, request, address, proof = _release_case(foreign=foreign)
    proof = replace(proof, death=replace(proof.death, reason=reason))
    calls, _failure = _install_rpc(node, request, address, proof)
    result = node._release_output_child_pin(address, request)
    assert type(result) is protocol.GetWorkerStateReply
    assert result == proof and result is not proof
    assert result.death is not proof.death
    assert result.incarnation == result.death.incarnation
    assert result.death.reason is reason
    assert len(calls) == 2
    _assert_metadata(result)
    assert not fixture.journal.publication_ids()


@pytest.mark.parametrize("released", (False, True))
def test_live_exact_release_ack_never_queries_or_infers_death(released):
    _fixture, node, _record, request, address, _proof_value = _release_case()
    calls = []

    def rpc(actual_address, handler, message, **options):
        assert (actual_address, handler) == (address, RELEASE_CONTAINED_REFERENCE_HANDLER)
        assert message == request and message is not request
        calls.append(message)
        return protocol.ReleaseContainedReferenceReply(
            message.object_id, message.owner_worker_id, message.hold, True, released,
        )

    node._background_rpc = rpc
    reply = node._release_output_child_pin(address, request)
    assert reply == protocol.ReleaseContainedReferenceReply(
        request.object_id, request.owner_worker_id, request.hold, True, released,
    )
    assert len(calls) == 1


@pytest.mark.parametrize("kind", (
    "alive", "missing", "expected", "timeout", "wrong-type", "wrong-worker",
    "watermark", "incarnation", "local-registration", "epoch-bool",
    "worker-pid-bool", "node-id-type", "death-reason", "found-bool",
))
def test_unproven_death_preserves_original_release_failure(kind):
    _fixture, node, _record, request, address, proof = _release_case()
    candidate = proof
    if kind == "alive":
        candidate = replace(proof, state=protocol.WorkerMembershipState.ALIVE, death=None)
    elif kind == "missing":
        candidate = protocol.GetWorkerStateReply(request.owner_worker_id, False, 9, error="not registered")
    elif kind == "expected":
        candidate = replace(proof, death=replace(proof.death, reason=protocol.WorkerDeathReason.EXPECTED))
    elif kind == "timeout":
        candidate = TransportTimeout("GCS is temporarily unavailable")
    elif kind == "wrong-type":
        candidate = proof.death  # the missing registration/watermark is not inferred
    elif kind == "wrong-worker":
        candidate = _proof(protocol.WorkerID(b"w" * 16))
    elif kind == "watermark":
        object.__setattr__(candidate, "watermark", 8)
    elif kind == "incarnation":
        object.__setattr__(candidate, "incarnation", replace(proof.incarnation, worker_pid=4202))
    elif kind == "local-registration":
        changed = replace(proof.incarnation, node_registration_epoch=5)
        candidate = replace(proof, incarnation=changed, death=replace(proof.death, incarnation=changed))
    elif kind == "epoch-bool":
        object.__setattr__(candidate.death, "death_epoch", True)
    elif kind == "worker-pid-bool":
        object.__setattr__(candidate.death.incarnation, "worker_pid", True)
    elif kind == "node-id-type":
        object.__setattr__(candidate.death.incarnation, "node_id", request.owner_worker_id)
    elif kind == "death-reason":
        object.__setattr__(candidate.death, "reason", "PROCESS_EXIT")
    else:
        object.__setattr__(candidate, "found", 1)
    calls, failure = _install_rpc(node, request, address, candidate)
    with pytest.raises(TransportTimeout) as raised:
        node._release_output_child_pin(address, request)
    assert raised.value is failure
    assert calls and calls[0][1] == RELEASE_CONTAINED_REFERENCE_HANDLER


def test_local_exit_without_gcs_address_is_not_release_authority():
    _fixture, node, _record, request, address, proof = _release_case()
    node._gcs_address = None
    node._dead_worker_exitcodes = {request.owner_worker_id: 17}
    calls, failure = _install_rpc(node, request, address, proof)
    with pytest.raises(TransportTimeout) as raised:
        node._release_output_child_pin(address, request)
    assert raised.value is failure and len(calls) == 1


def test_callback_mutation_cannot_retarget_the_death_query():
    _fixture, node, _record, request, address, _proof_value = _release_case()
    original_owner = request.owner_worker_id
    seen = []

    def rpc(_address, handler, message, **options):
        if handler == RELEASE_CONTAINED_REFERENCE_HANDLER:
            object.__setattr__(message, "owner_worker_id", protocol.WorkerID(b"x" * 16))
            return protocol.ReleaseContainedReferenceReply(
                message.object_id, message.owner_worker_id, message.hold, True, True,
            )
        assert handler == GCS_GET_WORKER_STATE_HANDLER
        seen.append(message.worker_id)
        return protocol.GetWorkerStateReply(original_owner, False, 0, error="not found")

    node._background_rpc = rpc
    with pytest.raises(OutputPublicationConflictError, match="requested hold"):
        node._release_output_child_pin(address, request)
    assert seen == [original_owner] and request.owner_worker_id == original_owner


def test_rejected_release_can_use_independent_exact_death_but_not_endpoint_identity():
    _fixture, node, _record, request, address, proof = _release_case(foreign=True)
    queries = []

    def rpc(actual_address, handler, message, **options):
        if handler == RELEASE_CONTAINED_REFERENCE_HANDLER:
            assert actual_address == address
            return protocol.ReleaseContainedReferenceReply(
                request.object_id, request.owner_worker_id, request.hold, False, False, "old owner port reused",
            )
        assert handler == GCS_GET_WORKER_STATE_HANDLER
        queries.append(message.worker_id)
        return proof

    node._background_rpc = rpc
    assert node._release_output_child_pin(address, request) == proof
    assert queries == [request.owner_worker_id]


def test_rejected_release_without_death_keeps_original_rejection():
    _fixture, node, _record, request, address, proof = _release_case(foreign=True)
    calls = []

    def rpc(_address, handler, message, **options):
        calls.append(handler)
        if handler == RELEASE_CONTAINED_REFERENCE_HANDLER:
            assert message == request
            return protocol.ReleaseContainedReferenceReply(
                request.object_id, request.owner_worker_id, request.hold, False, False, "retained hold rejection",
            )
        assert handler == GCS_GET_WORKER_STATE_HANDLER
        return replace(proof, state=protocol.WorkerMembershipState.ALIVE, death=None)

    node._background_rpc = rpc
    with pytest.raises(OutputPublicationRemoteError, match="retained hold rejection"):
        node._release_output_child_pin(address, request)
    assert calls == [RELEASE_CONTAINED_REFERENCE_HANDLER, GCS_GET_WORKER_STATE_HANDLER]


def test_departed_slot_uses_pending_registration_proof_not_replacement_endpoint():
    _fixture, node, _record, request, address, proof = _release_case()
    node._workers.pop(request.owner_worker_id)
    report = protocol.ReportWorkerDeath(
        proof.death.detection_id, node._copy_output_worker_incarnation(proof.incarnation),
        proof.death.exit_code, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    node._worker_death_reports = {request.owner_worker_id: SimpleNamespace(report=report)}
    _install_rpc(node, request, address, proof)
    assert node._release_output_child_pin(address, request) == proof
    changed_incarnation = replace(proof.incarnation, worker_pid=proof.incarnation.worker_pid + 1)
    changed = replace(proof, incarnation=changed_incarnation, death=replace(proof.death, incarnation=changed_incarnation))
    _calls, failure = _install_rpc(node, request, address, changed)
    with pytest.raises(TransportTimeout) as raised:
        node._release_output_child_pin(address, request)
    assert raised.value is failure


def test_dead_executor_child_cleanup_does_not_discharge_remaining_live_child_hold():
    # Rollback visits transfer intents in reverse order. Put the executor
    # last so its actual death is settled before the independent live owner
    # is intentionally blocked; the two-child dependency remains explicit.
    fixture, node, _record, _complete = _node(reverse_children=True)
    fixture.prepare()
    proof = _proof(
        fixture.values.executor, node_id=node.node_id, node_pid=node._node_pid,
        node_epoch=node._registration_epoch, worker_pid=node._workers[fixture.values.executor].pid,
    )
    node._workers[fixture.values.executor].incarnation = node._copy_output_worker_incarnation(proof.incarnation)
    blocked_live = True
    seen = []

    def rpc(address, handler, message, **options):
        nonlocal blocked_live
        assert not node._state_lock._is_owned()
        assert not fixture.journal._lock._is_owned()
        seen.append((handler, message))
        if handler == GCS_GET_WORKER_STATE_HANDLER:
            if message.worker_id == fixture.values.executor:
                return proof
            live = _proof(fixture.values.foreign_owner)
            return replace(live, state=protocol.WorkerMembershipState.ALIVE, death=None)
        assert handler == RELEASE_CONTAINED_REFERENCE_HANDLER
        if message.owner_worker_id == fixture.values.executor:
            raise TransportTimeout("dead executor endpoint")
        if blocked_live and message.hold.container_object_id == (fixture.manifest.publication_id).object_id:
            raise TransportTimeout("live child owner temporarily unavailable")
        return fixture.release_child(address, message)

    node._background_rpc = rpc
    fixture.adapter._release_child = node._release_output_child_pin
    # The concrete Node factory must use the same wrapper, not a legacy
    # always-successful release lambda hidden inside a separate adapter.
    assert node._make_output_publication_adapter()._release_child == node._release_output_child_pin
    fixture.journal.begin_rollback(fixture.id, "dead-child-rollback")
    with pytest.raises(TransportTimeout, match="live child"):
        fixture.adapter.rollback(fixture.id, "dead-child-rollback", max_effects=16)
    next_effect = fixture.journal.next_rollback_effect(fixture.id)
    assert next_effect.stage is Stage.FINAL_RELEASE
    assert (fixture.manifest.value).transfers[next_effect.transfer_index].contained_owner_worker_id == fixture.values.foreign_owner
    live_table = fixture.child_owners[fixture.values.foreign_owner]
    assert live_table.snapshot(fixture.values.borrowed_child).contained_holds
    assert fixture.journal.snapshot(fixture.id).rollback_tombstone is None
    # The dead owner's earlier slot was discharged using its actual death
    # evidence, but that says nothing about this still-live child's hold.
    assert any(handler == GCS_GET_WORKER_STATE_HANDLER and message.worker_id == fixture.values.executor
               for handler, message in seen)
    blocked_live = False
    terminal = fixture.adapter.rollback(fixture.id, "dead-child-rollback", max_effects=16)
    assert terminal is not None
    assert not live_table.snapshot(fixture.values.borrowed_child).contained_holds
    assert fixture.store.used_bytes == 0
    assert not fixture.adapter.pending_rollbacks()
    deaths = [message for handler, message in seen if handler == GCS_GET_WORKER_STATE_HANDLER
              and message.worker_id == fixture.values.executor]
    assert deaths


def _rollback_at_first_child_effect():
    fixture, node, _record, _complete = _node()
    fixture.prepare()
    rollback_id = "child-proof-consumption"
    # The one stored output Drop precedes child release. No child callback
    # runs during setup; both independent child-owner obligations remain.
    assert len(((fixture.manifest.value,))) == 1
    assert fixture.adapter.rollback(fixture.id, rollback_id, max_effects=1) is None
    effect = fixture.journal.next_rollback_effect(fixture.id)
    assert effect.stage is Stage.FINAL_RELEASE
    transfer = (fixture.manifest.value).transfers[effect.transfer_index]
    request = protocol.ReleaseContainedReference(
        transfer.contained_object_id, transfer.contained_owner_worker_id, transfer.final_hold,
    )
    assert fixture.journal.snapshot(fixture.id).rollback_tombstone is None
    return fixture, node, rollback_id, effect, request, transfer.contained_owner_address


def test_adapter_revalidates_both_worker_pids_after_wrapper_returned_valid_proof():
    fixture, node, rollback_id, effect, request, address = _rollback_at_first_child_effect()
    proof = _proof(request.owner_worker_id)
    calls, _release_failure = _install_rpc(node, request, address, proof)
    wrapper = node._release_output_child_pin
    injected = []

    def corrupt_after_validation(actual_address, message):
        validated = wrapper(actual_address, message)
        assert type(validated) is protocol.GetWorkerStateReply and validated == proof
        # Corrupt both independently copied registrations, retaining equality
        # that a shallow GetWorkerStateReply check would otherwise accept.
        object.__setattr__(validated.incarnation, "worker_pid", True)
        object.__setattr__(validated.death.incarnation, "worker_pid", True)
        assert validated.incarnation == validated.death.incarnation
        injected.append(validated)
        return validated

    fixture.adapter._release_child = corrupt_after_validation
    before = fixture.journal.snapshot(fixture.id)
    with pytest.raises(ValueError, match="worker_pid"):
        fixture.adapter.rollback(fixture.id, rollback_id, max_effects=1)
    after = fixture.journal.snapshot(fixture.id)
    assert len(injected) == 1 and len(calls) == 2
    assert fixture.journal.next_rollback_effect(fixture.id) == effect
    assert after.acknowledgements == before.acknowledgements
    assert after.rollback_tombstone is None and after == before
    assert not fixture.journal.acknowledged(effect)
    assert not fixture.adapter._tickets


def test_adapter_rejects_other_dead_owner_proof_even_from_expected_endpoint():
    fixture, _node_value, rollback_id, effect, request, address = _rollback_at_first_child_effect()
    other_owner = (fixture.values.executor if request.owner_worker_id != fixture.values.executor
                   else fixture.values.foreign_owner)
    proof = _proof(other_owner)
    calls = []

    def wrong_owner_proof(actual_address, message):
        # Intentional consumption-boundary injection: the callback is invoked
        # for the correct endpoint/hold but supplies a different, valid GCS
        # owner-death record.  Endpoint equality cannot authorize that swap.
        assert actual_address == address and message == request
        assert proof.worker_id != message.owner_worker_id
        assert proof.incarnation == proof.death.incarnation
        calls.append((actual_address, message))
        return proof

    fixture.adapter._release_child = wrong_owner_proof
    before = fixture.journal.snapshot(fixture.id)
    with pytest.raises(OutputPublicationConflictError, match="owner death"):
        fixture.adapter.rollback(fixture.id, rollback_id, max_effects=1)
    after = fixture.journal.snapshot(fixture.id)
    assert calls == [(address, request)]
    assert fixture.journal.next_rollback_effect(fixture.id) == effect
    assert after.acknowledgements == before.acknowledgements
    assert after.rollback_tombstone is None and after == before
    assert not fixture.journal.acknowledged(effect)
    assert fixture.child_owners[request.owner_worker_id].snapshot(request.object_id).contained_holds
    assert not fixture.adapter._tickets
