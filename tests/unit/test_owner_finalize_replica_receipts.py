"""Owner finalization contributes exact completed physical deletion receipts.

One bare Node, one 1 KiB store, one real journal/lease ledger, two tiny result
slots and one fake alive Worker per case. The ordinary Prepare handler creates
every intent and write claim. An actual owner-wide fence precedes Finalize; no
dead owner is resealed and no successful deletion receipt is fabricated.

The imported tripwire and extra guard prohibit constructors, processes,
threads, sockets and waits. Faults are synchronous local exceptions/False
returns, corrupted bytes/claim metadata, or one fake Worker's lost reply.
Every cleanup has at most two explicit attempts; there is no progress loop.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import multiprocessing.process
import subprocess
import threading

import pytest

from miniray import output_protocol as wire, protocol
from miniray.node import NodeServer
from miniray.object_manager import PullAction, PullState, UnknownPullError
from miniray.output_publication_journal import (
    OutputPublicationEffect, OutputPublicationJournalState, OutputPublicationStage as Stage,
)
from miniray.resources import ResourceVector
from tests.unit.test_output_owner_death_node import _fixture
from tests.unit.test_output_publication_node_server import _node, _no_runtime as _no_runtime


pytestmark = pytest.mark.unit
_Status = protocol.DropObjectReplicaStatus


class _InjectedLocalFailure(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def _no_additional_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("owner-finalize receipt test attempted runtime infrastructure")

    monkeypatch.setattr(NodeServer, "__init__", forbidden)
    monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(threading.Thread, "join", forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)


def _receipts(node):
    return set(getattr(node, "_replica_drop_receipts", ()))


def _drop(fixture, node, slot_index=1):
    slot = fixture.manifest.slots[slot_index]
    return protocol.DropObjectReplica(
        slot.object_id, fixture.id.attempt_id, fixture.values.owner, node.node_id, slot.checksum,
    )


def _owner_request(fixture, node):
    death = protocol.WorkerDeathRecord(
        "owner-finalize-receipt-death",
        protocol.WorkerIncarnation(
            node.node_id, node._node_pid, node._registration_epoch, fixture.values.owner, 1901,
        ), 1, 5, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    return wire.FinalizeOutputOwnerDeath(fixture.manifest, death)


def _install_owner_wide_fence(fixture, node, request):
    fence = protocol.InstallOwnerDeathFence(
        "owner-finalize-receipt-sweep", request.owner_death, node.node_id,
        scope=protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
    )
    reply = node._handle_install_owner_death_fence(fence)
    assert type(reply) is protocol.InstallOwnerDeathFenceReply
    assert reply.request == fence and reply.accepted and reply.complete
    # These failures precede sealed-metadata publication. The physical claim
    # remains Finalize's work; the sweep must not invent a sealed descriptor.
    assert reply.observations == ()
    assert node._owner_death_fences[fixture.values.owner] == request.owner_death


def _case(monkeypatch, phase):
    if phase == "partial":
        fixture, node, record, _complete, request = _fixture(monkeypatch, phase="partial")
    else:
        fixture, node, record, _complete = _node(refs=False)
        prepare = wire.PrepareOutputPublication(fixture.manifest, fixture.values.payloads)
        stored = fixture.manifest.slots[1]
        with monkeypatch.context() as fault:
            if phase == "intent-only":
                def before_storage(effect, descriptor, payload):
                    assert effect.slot_index == 1 and descriptor.object_id == stored.object_id
                    assert payload == fixture.values.payloads[1]
                    raise _InjectedLocalFailure("before replica create")

                fault.setattr(fixture.adapter, "_seal_replica", before_storage)
            elif phase == "created":
                original = fixture.store.create

                def created_then_error(object_id, size_bytes):
                    assert object_id == stored.object_id and size_bytes == stored.size_bytes
                    original(object_id, size_bytes)
                    raise _InjectedLocalFailure("after replica create")

                fault.setattr(fixture.store, "create", created_then_error)
            else:
                assert phase in ("seal-before-metadata", "corrupt-sealed")
                original_seal = fixture.store.seal

                def sealed_then_error(object_id):
                    assert object_id == stored.object_id
                    original_seal(object_id)
                    raise _InjectedLocalFailure("after replica seal")

                fault.setattr(fixture.store, "seal", sealed_then_error)
                if phase == "corrupt-sealed":
                    original_write = fixture.store.write

                    def corrupt_write(object_id, payload):
                        assert object_id == stored.object_id
                        original_write(object_id, b"X" * len(payload))

                    fault.setattr(fixture.store, "write", corrupt_write)
            with pytest.raises(_InjectedLocalFailure):
                node._handle_prepare_output_publication(prepare)
        request = _owner_request(fixture, node)

    stored = fixture.manifest.slots[1]
    expected_effect = OutputPublicationEffect(fixture.id, fixture.manifest.manifest_digest, Stage.MATERIALIZE, 1)
    snapshot = fixture.journal.snapshot(fixture.id)
    assert expected_effect in snapshot.intents and not fixture.journal.acknowledged(expected_effect)
    assert snapshot.retained_result_slots == (0,) and snapshot.complete is None
    assert record.output_publication_id == fixture.id and record.state is protocol.LeaseExecutionState.RUNNING
    assert fixture.store.capacity_bytes == 1024 and not node._sealed_metadata
    assert not _receipts(node) and not node._dropped_metadata
    if phase == "intent-only":
        assert not fixture.store.contains(stored.object_id, sealed_only=False)
        assert not node._local_replica_write_claims
    else:
        claim = node._local_replica_write_claims[stored.object_id]
        assert claim.effect == expected_effect
        physical = fixture.store.snapshot(stored.object_id)
        assert physical.sealed is (phase in ("seal-before-metadata", "corrupt-sealed"))
        assert physical.size_bytes == stored.size_bytes and physical.pin_count == 0
        if phase == "seal-before-metadata":
            assert fixture.store.get(stored.object_id) == fixture.values.payloads[1]
            # Real local-ready metadata makes both manager failure cuts meaningful.
            decision = node._object_manager.request_pull(
                stored.object_id, locations=(node.node_id,), waiter_token="finalize-local-ready",
                expected_size=stored.size_bytes, expected_checksum=stored.checksum, attempt_id=fixture.id.attempt_id,
            )
            assert decision.action is PullAction.LOCAL_READY
        elif phase == "corrupt-sealed":
            assert fixture.store.get(stored.object_id) != fixture.values.payloads[1]
    _install_owner_wide_fence(fixture, node, request)
    return fixture, node, record, request


def _ack_worker(fixture, node, record, request, calls, *, lose_first=False):
    def rpc(address, handler, message):
        assert address == record.grant.worker_address
        assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER and message == request
        assert not node._state_lock._is_owned()
        assert not fixture.journal._lock._is_owned() and not fixture.adapter._lock._is_owned()
        assert fixture.id in fixture.adapter._tickets
        calls.append(message)
        assert len(calls) <= 2
        if lose_first and len(calls) == 1:
            raise TimeoutError("fake Worker cleaned before its ACK was lost")
        return wire.FinalizeOutputOwnerDeathReply(message, True)

    node._background_rpc = rpc


def _assert_not_finalized(fixture, node, record, request):
    try:
        reply = node._handle_finalize_output_owner_death(request)
    except _InjectedLocalFailure:
        pass  # Either propagation or a typed unclean result preserves custody.
    else:
        assert type(reply) is wire.FinalizeOutputOwnerDeathReply
        assert reply.request == request and not reply.cleaned
    assert not fixture.adapter.owner_death_finished(fixture.id) and not fixture.adapter._tickets
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.state is OutputPublicationJournalState.ACTIVE
    assert snapshot.retained_result_slots == (0,)
    assert record.state is protocol.LeaseExecutionState.ABANDONED
    assert fixture.ledger.available == ResourceVector({"CPU": 1})


def _forbid_stored_work(patch, fixture, node):
    stored_id = fixture.manifest.slots[1].object_id

    def guard_for(original):
        def guarded(object_id, *args, **kwargs):
            if object_id == stored_id:
                pytest.fail("completed physical receipt repeated work on its stored slot")
            return original(object_id, *args, **kwargs)
        return guarded

    for name in ("contains", "snapshot", "get", "create", "write", "seal", "delete", "abort"):
        patch.setattr(fixture.store, name, guard_for(getattr(fixture.store, name)))
    patch.setattr(node._object_manager, "forget_local_replica", guard_for(node._object_manager.forget_local_replica))


def _assert_physical_receipt(fixture, node, monkeypatch):
    drop = _drop(fixture, node)
    expected = (drop.producer_attempt_id, drop.owner_worker_id, drop.checksum)
    assert node._dropped_metadata == {drop.object_id: expected}
    assert len(_receipts(node)) == 1
    assert not node._local_replica_write_claims and not node._sealed_metadata
    assert fixture.store.used_bytes == 0
    before = deepcopy((node._dropped_metadata, _receipts(node)))
    with monkeypatch.context() as guard:
        _forbid_stored_work(guard, fixture, node)
        reply = node._handle_drop_object_replica(drop)
    assert type(reply) is protocol.DropObjectReplicaReply and reply.status is _Status.ALREADY_DROPPED
    assert reply.accepted and not reply.dropped and reply.error is None
    assert protocol.DropObjectReplica(
        reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum,
    ) == drop
    assert (node._dropped_metadata, _receipts(node)) == before


def _assert_finalized(fixture, node, record, request, monkeypatch):
    reply = node._handle_finalize_output_owner_death(request)
    assert type(reply) is wire.FinalizeOutputOwnerDeathReply and reply.request == request and reply.cleaned
    assert fixture.adapter.owner_death_finished(fixture.id) and not fixture.adapter._tickets
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.state is OutputPublicationJournalState.RETIRED and not snapshot.retained_result_slots
    assert snapshot.complete is None and snapshot.rollback_tombstone is None
    assert record.state is protocol.LeaseExecutionState.ABANDONED
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    _assert_physical_receipt(fixture, node, monkeypatch)
    return reply


@pytest.mark.parametrize("phase", ("created", "seal-before-metadata"))
def test_finalize_of_interrupted_physical_write_supplies_generic_drop_receipt(monkeypatch, phase):
    fixture, node, record, request = _case(monkeypatch, phase)
    calls = []
    _ack_worker(fixture, node, record, request, calls)
    first = _assert_finalized(fixture, node, record, request, monkeypatch)
    assert node._handle_finalize_output_owner_death(request) == first
    assert calls == [request]
    with pytest.raises(UnknownPullError):
        node._object_manager.snapshot(fixture.manifest.slots[1].object_id)


def test_uncreated_stored_intent_is_fenced_but_inline_has_no_physical_drop_receipt(monkeypatch):
    fixture, node, record, request = _case(monkeypatch, "intent-only")
    calls = []
    _ack_worker(fixture, node, record, request, calls)
    _assert_finalized(fixture, node, record, request, monkeypatch)
    assert calls == [request]
    inline_drop = _drop(fixture, node, 0)
    assert fixture.manifest.slots[0].tier is protocol.ResultStorage.INLINE
    before = deepcopy((node._dropped_metadata, _receipts(node)))
    reply = node._handle_drop_object_replica(inline_drop)
    assert type(reply) is protocol.DropObjectReplicaReply
    assert reply.status is _Status.REJECTED and not reply.accepted
    assert inline_drop.object_id not in node._dropped_metadata
    assert (node._dropped_metadata, _receipts(node)) == before


@pytest.mark.parametrize("phase,method", (("partial", "abort"), ("seal-before-metadata", "delete")))
@pytest.mark.parametrize("fault", ("false", "exception"))
def test_unfinished_physical_removal_keeps_claim_and_never_finalizes(monkeypatch, phase, method, fault):
    fixture, node, record, request = _case(monkeypatch, phase)
    stored_id = fixture.manifest.slots[1].object_id
    before_claim = deepcopy(node._local_replica_write_claims[stored_id])
    before_store = fixture.store.snapshot(stored_id)
    worker_calls, physical_calls = [], []
    _ack_worker(fixture, node, record, request, worker_calls)

    def refuse(object_id):
        assert object_id == stored_id
        physical_calls.append(object_id)
        assert len(physical_calls) == 1
        if fault == "false":
            return False
        raise _InjectedLocalFailure("physical removal did not finish")

    with monkeypatch.context() as injected:
        injected.setattr(fixture.store, method, refuse)
        _assert_not_finalized(fixture, node, record, request)
    assert physical_calls == [stored_id] and not worker_calls
    assert node._local_replica_write_claims[stored_id] == before_claim
    assert fixture.store.snapshot(stored_id) == before_store
    assert not _receipts(node) and not node._dropped_metadata
    _assert_finalized(fixture, node, record, request, monkeypatch)
    assert worker_calls == [request]


@pytest.mark.parametrize("after_effect", (False, True), ids=("before-forget", "after-forget"))
def test_manager_failure_retains_claim_until_same_finalize_completes_cleanup(monkeypatch, after_effect):
    fixture, node, record, request = _case(monkeypatch, "seal-before-metadata")
    stored_id = fixture.manifest.slots[1].object_id
    claim = deepcopy(node._local_replica_write_claims[stored_id])
    before_pull = node._object_manager.snapshot(stored_id)
    assert before_pull.state is PullState.READY
    original = node._object_manager.forget_local_replica
    calls, worker_calls = [], []
    _ack_worker(fixture, node, record, request, worker_calls)

    def fail_once(object_id, *, attempt_id):
        assert (object_id, attempt_id) == (stored_id, fixture.id.attempt_id)
        calls.append((object_id, attempt_id))
        assert len(calls) <= 2
        if len(calls) == 1:
            if after_effect:
                assert original(object_id, attempt_id=attempt_id)
            raise _InjectedLocalFailure("manager removal has no acknowledgement")
        return original(object_id, attempt_id=attempt_id)

    monkeypatch.setattr(node._object_manager, "forget_local_replica", fail_once)
    _assert_not_finalized(fixture, node, record, request)
    assert not worker_calls and not _receipts(node)
    assert not fixture.store.contains(stored_id, sealed_only=False)
    assert node._local_replica_write_claims[stored_id] == claim
    drop = _drop(fixture, node)
    assert node._dropped_metadata[stored_id] == (drop.producer_attempt_id, drop.owner_worker_id, drop.checksum)
    if after_effect:
        with pytest.raises(UnknownPullError):
            node._object_manager.snapshot(stored_id)
    else:
        assert node._object_manager.snapshot(stored_id) == before_pull
    _assert_finalized(fixture, node, record, request, monkeypatch)
    assert calls == [(stored_id, fixture.id.attempt_id)] * 2
    assert worker_calls == [request]
    with pytest.raises(UnknownPullError):
        node._object_manager.snapshot(stored_id)


def test_worker_ack_loss_preserves_physical_receipt_and_finalize_replay_skips_storage(monkeypatch):
    fixture, node, record, request = _case(monkeypatch, "partial")
    calls = []
    _ack_worker(fixture, node, record, request, calls, lose_first=True)
    with pytest.raises(TimeoutError, match="Worker cleaned"):
        node._handle_finalize_output_owner_death(request)
    assert calls == [request]
    assert not fixture.adapter.owner_death_finished(fixture.id) and not fixture.adapter._tickets
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == (0,)
    _assert_physical_receipt(fixture, node, monkeypatch)
    before = deepcopy((node._dropped_metadata, _receipts(node)))
    with monkeypatch.context() as guard:
        _forbid_stored_work(guard, fixture, node)
        second = node._handle_finalize_output_owner_death(request)
        assert second.cleaned and second.request == request
        assert node._handle_finalize_output_owner_death(request) == second
    assert calls == [request, request]
    assert (node._dropped_metadata, _receipts(node)) == before
    assert fixture.adapter.owner_death_finished(fixture.id)
    assert fixture.journal.snapshot(fixture.id).state is OutputPublicationJournalState.RETIRED
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == ()
    assert record.state is protocol.LeaseExecutionState.ABANDONED


@pytest.mark.parametrize("phase,method", (("partial", "abort"), ("seal-before-metadata", "delete")))
def test_removal_effect_then_exception_replays_from_retained_claim(monkeypatch, phase, method):
    fixture, node, record, request = _case(monkeypatch, phase)
    stored_id = fixture.manifest.slots[1].object_id
    claim = deepcopy(node._local_replica_write_claims[stored_id])
    original = getattr(fixture.store, method)
    worker_calls = []
    _ack_worker(fixture, node, record, request, worker_calls)

    def removed_then_error(object_id):
        assert object_id == stored_id and original(object_id)
        raise _InjectedLocalFailure("physical removal completed before error")

    with monkeypatch.context() as fault:
        fault.setattr(fixture.store, method, removed_then_error)
        _assert_not_finalized(fixture, node, record, request)
    assert not fixture.store.contains(stored_id, sealed_only=False)
    assert node._local_replica_write_claims[stored_id] == claim
    assert not _receipts(node) and not worker_calls
    _assert_finalized(fixture, node, record, request, monkeypatch)
    assert worker_calls == [request]


@pytest.mark.parametrize("corruption", ("bytes", "claim"))
def test_corrupt_bytes_or_another_write_claim_never_authorize_finalization(monkeypatch, corruption):
    fixture, node, record, request = _case(monkeypatch, "corrupt-sealed" if corruption == "bytes" else "partial")
    stored_id = fixture.manifest.slots[1].object_id
    if corruption == "claim":
        original = node._local_replica_write_claims[stored_id]
        node._local_replica_write_claims[stored_id] = replace(
            original, effect=replace(original.effect, slot_index=0),
        )
    before_claims = deepcopy(node._local_replica_write_claims)
    before_store = fixture.store.snapshot(stored_id)
    worker_calls = []
    _ack_worker(fixture, node, record, request, worker_calls)
    _assert_not_finalized(fixture, node, record, request)
    assert not worker_calls and not _receipts(node) and not node._dropped_metadata
    assert node._local_replica_write_claims == before_claims
    assert fixture.store.snapshot(stored_id) == before_store
    if corruption == "bytes":
        assert fixture.store.get(stored_id) == b"X" * fixture.manifest.slots[1].size_bytes
