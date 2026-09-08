"""Exact completed absence is shared by the existing local cleanup authorities.

Each case has one bare Node, one 1 KiB in-memory store, at most two publication
epochs and one physical ObjectID. The imported fixture constructs actual
publication manifests and a real journal; its only materialization intent is
the selected stored slot. No Node constructor, process, thread, socket, timer,
wait, user function or transport runs. A case injects at most one synchronous
write exception and one forget exception, one real store pin, or one malformed
authority/request.

Receipts are obtained only from actual delete/abort or journal-authorized
absence. A higher epoch alone is not a receipt. Owner-wide cases install the
real permanent owner fence and never reseal for that dead owner.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import ids, protocol
from miniray.node import NodeServer
from miniray.object_manager import PullAction, PullState, PullStateError, UnknownPullError
from miniray.output_publication import OutputPublicationConflictError
from miniray.output_publication_journal import (
    OutputPublicationAck, OutputPublicationJournalState,
    OutputPublicationJournalStateError, OutputPublicationStage as Stage,
)
from tests.unit.test_output_replica_node import _Fixture


pytestmark = pytest.mark.unit
_Status = protocol.DropObjectReplicaStatus


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("cross-cleanup pure test attempted runtime infrastructure")

    for kind, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(NodeServer, "__init__", forbidden)


def _receipts(node):
    return set(getattr(node, "_replica_drop_receipts", ()))


def _remember_local_ready(fixture):
    decision = fixture.node._object_manager.request_pull(
        fixture.object_id, locations=(fixture.node.node_id,),
        waiter_token="cross-cleanup-local-ready",
        expected_size=fixture.descriptor.size_bytes,
        expected_checksum=fixture.descriptor.checksum,
        attempt_id=fixture.id.attempt_id,
    )
    assert decision.action is PullAction.LOCAL_READY
    snapshot = fixture.node._object_manager.snapshot(fixture.object_id)
    assert snapshot.state is PullState.READY
    assert snapshot.attempt_id == fixture.id.attempt_id


def _materialize(fixture, phase, monkeypatch):
    if phase == "sealed":
        assert fixture.seal() == fixture.descriptor
        _remember_local_ready(fixture)
    elif phase == "partial":
        store = fixture.node._object_store
        original = store.write

        def partial_write(object_id, payload):
            original(object_id, payload[:2])
            raise RuntimeError("cross-cleanup partial write")

        with monkeypatch.context() as fault:
            fault.setattr(store, "write", partial_write)
            with pytest.raises(RuntimeError, match="cross-cleanup partial write"):
                fixture.seal()
        assert not store.snapshot(fixture.object_id).sealed
        assert fixture.object_id in fixture.node._local_replica_write_claims
        assert not fixture.node._sealed_metadata
    else:
        assert phase == "intent-only"
        assert fixture.effect in fixture.journal.snapshot(fixture.id).intents
        assert not fixture.node._object_store.contains(fixture.object_id, sealed_only=False)
        assert not fixture.node._sealed_metadata
        assert not fixture.node._local_replica_write_claims


def _snapshot(fixture):
    node, object_id = fixture.node, fixture.object_id
    present = node._object_store.contains(object_id, sealed_only=False)
    physical = node._object_store.snapshot(object_id) if present else None
    data = node._object_store.get(object_id) if physical is not None and physical.sealed else None
    try:
        pull = node._object_manager.snapshot(object_id)
    except UnknownPullError:
        pull = None
    return deepcopy((
        node._sealed_metadata, node._dropped_metadata,
        node._local_replica_write_claims, _receipts(node),
        node._owner_death_fences, physical, data, pull, node._object_store.used_bytes,
    ))


def _forbid_physical_work(patch, node):
    def forbidden(*_args, **_kwargs):
        pytest.fail("completed exact receipt touched physical bytes or ObjectManager state")

    for name in ("contains", "snapshot", "get", "create", "write", "seal", "delete", "abort"):
        patch.setattr(node._object_store, name, forbidden)
    patch.setattr(node._object_manager, "forget_local_replica", forbidden)


def _assert_reply(reply, request, status):
    assert type(reply) is protocol.DropObjectReplicaReply
    assert (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id, reply.node_id, reply.checksum) == (
        request.object_id, request.producer_attempt_id, request.owner_worker_id, request.node_id, request.checksum,
    )
    assert reply.status is status
    assert reply.accepted is (status in (_Status.DROPPED, _Status.ALREADY_DROPPED))
    assert reply.dropped is (status is _Status.DROPPED)
    assert (reply.error is None) is reply.accepted


def _generic_replay_without_physical_work(fixture, monkeypatch):
    before = _snapshot(fixture)
    with monkeypatch.context() as guard:
        _forbid_physical_work(guard, fixture.node)
        reply = fixture.node._handle_drop_object_replica(fixture.request)
    _assert_reply(reply, fixture.request, _Status.ALREADY_DROPPED)
    assert _snapshot(fixture) == before
    return reply


def _publication_replay_without_physical_work(fixture, effect, monkeypatch):
    before, journal = _snapshot(fixture), fixture.journal.snapshot(fixture.id)
    with monkeypatch.context() as guard:
        _forbid_physical_work(guard, fixture.node)
        reply = fixture.drop(effect)
    _assert_reply(reply, fixture.request, _Status.ALREADY_DROPPED)
    assert _snapshot(fixture) == before
    assert fixture.journal.snapshot(fixture.id) == journal
    return reply


def _begin_cleanup(fixture, authority):
    if authority == "publication":
        return fixture.begin_drop()
    if authority == "generic":
        return fixture.request
    assert authority == "owner-wide"
    death = protocol.WorkerDeathRecord(
        "cross-cleanup-owner-death",
        protocol.WorkerIncarnation(
            fixture.node.node_id, fixture.node._node_pid, fixture.node._registration_epoch,
            fixture.descriptor.owner_worker_id, 1702,
        ), 1, 17, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    return protocol.InstallOwnerDeathFence(
        "cross-cleanup-owner-sweep", death, fixture.node.node_id,
        scope=protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
    )


def _attempt_cleanup(fixture, authority, operation, status):
    if authority == "owner-wide":
        reply = fixture.node._handle_install_owner_death_fence(operation)
        assert type(reply) is protocol.InstallOwnerDeathFenceReply
        assert reply.request == operation and reply.accepted
        assert len(reply.observations) == 1
        observation, = reply.observations
        assert observation.descriptor == protocol.ObjectStoreDescriptor(
            fixture.object_id, fixture.descriptor.owner_worker_id, fixture.id.attempt_id,
            fixture.node.node_id, fixture.descriptor.size_bytes, fixture.descriptor.checksum,
        )
        expected = {
            _Status.DROPPED: protocol.OwnerDeathReplicaStatus.ABSENT,
            _Status.ALREADY_DROPPED: protocol.OwnerDeathReplicaStatus.ABSENT,
            _Status.PINNED: protocol.OwnerDeathReplicaStatus.PINNED,
            _Status.INCONSISTENT: protocol.OwnerDeathReplicaStatus.CONFLICT,
        }[status]
        assert observation.status is expected
        assert reply.complete is (expected is protocol.OwnerDeathReplicaStatus.ABSENT)
        return reply
    reply = (fixture.drop(operation) if authority == "publication"
             else fixture.node._handle_drop_object_replica(operation))
    _assert_reply(reply, fixture.request, status)
    return reply


@pytest.mark.parametrize("phase", ("sealed", "partial", "intent-only"))
def test_publication_completion_is_a_generic_receipt_across_newer_epochs(monkeypatch, phase):
    older = _Fixture()
    _materialize(older, phase, monkeypatch)
    effect = older.begin_drop()
    _assert_reply(older.drop(effect), older.request, _Status.DROPPED)
    assert not older.node._object_store.contains(older.object_id, sealed_only=False)
    assert not older.node._sealed_metadata and not older.node._local_replica_write_claims
    assert older.node._dropped_metadata[older.object_id] == older.tombstone
    assert len(_receipts(older.node)) == 1  # Written by publication, not a generic warm-up.
    assert older.journal.ack_rollback(OutputPublicationAck(effect))
    assert older.journal.snapshot(older.id).state is OutputPublicationJournalState.RETIRED

    newer = _Fixture(node=older.node, attempt=4)
    assert newer.seal() == newer.descriptor
    _remember_local_ready(newer)
    first = _generic_replay_without_physical_work(older, monkeypatch)
    assert newer.node._sealed_metadata[newer.object_id] == newer.metadata
    assert newer.node._object_store.get(newer.object_id) == newer.payload
    assert newer.node._dropped_metadata[newer.object_id] == older.tombstone

    _assert_reply(newer.node._handle_drop_object_replica(newer.request), newer.request, _Status.DROPPED)
    assert newer.node._dropped_metadata[newer.object_id] == newer.tombstone
    assert _generic_replay_without_physical_work(older, monkeypatch) == first
    assert len(_receipts(older.node)) == 2


def test_generic_completion_satisfies_only_valid_publication_replays_across_epochs(monkeypatch):
    older = _Fixture()
    _materialize(older, "sealed", monkeypatch)
    _assert_reply(older.node._handle_drop_object_replica(older.request), older.request, _Status.DROPPED)
    assert len(_receipts(older.node)) == 1
    effect = older.begin_drop()
    newer = _Fixture(node=older.node, attempt=4)
    assert newer.seal() == newer.descriptor
    _remember_local_ready(newer)

    first = _publication_replay_without_physical_work(older, effect, monkeypatch)
    assert not older.journal.acknowledged(effect)  # Physical proof never ACKs the journal itself.
    assert older.journal.ack_rollback(OutputPublicationAck(effect))
    assert older.journal.snapshot(older.id).state is OutputPublicationJournalState.RETIRED
    assert newer.node._sealed_metadata[newer.object_id] == newer.metadata

    _assert_reply(newer.node._handle_drop_object_replica(newer.request), newer.request, _Status.DROPPED)
    assert newer.node._dropped_metadata[newer.object_id] == newer.tombstone
    assert _publication_replay_without_physical_work(older, effect, monkeypatch) == first
    assert len(_receipts(older.node)) == 2


def test_owner_wide_completion_supplies_generic_receipt_without_resealing_dead_owner(monkeypatch):
    fixture = _Fixture()
    _materialize(fixture, "sealed", monkeypatch)
    operation = _begin_cleanup(fixture, "owner-wide")
    reply = _attempt_cleanup(fixture, "owner-wide", operation, _Status.DROPPED)
    assert len(_receipts(fixture.node)) == 1
    assert fixture.node._dropped_metadata[fixture.object_id] == fixture.tombstone
    assert fixture.node._owner_death_fences[fixture.descriptor.owner_worker_id] == operation.owner_death
    assert fixture.node._handle_install_owner_death_fence(operation) == reply
    _generic_replay_without_physical_work(fixture, monkeypatch)

    # A permanent owner fence makes a same-owner newer-epoch sequence invalid.
    rejected = fixture.node._handle_seal_object(protocol.SealObject.from_data(
        fixture.object_id, fixture.id.attempt_id.next(), fixture.descriptor.owner_worker_id, fixture.payload,
    ))
    assert not rejected.sealed and "fenced by Worker death" in rejected.error
    assert not fixture.node._object_store.contains(fixture.object_id, sealed_only=False)


@pytest.mark.parametrize("fault", (
    "active", "digest", "slot", "publication", "node_id", "node_pid", "registration_epoch",
))
def test_completed_receipt_cannot_bypass_journal_or_node_incarnation(monkeypatch, fault):
    fixture = _Fixture()
    _materialize(fixture, "sealed", monkeypatch)
    _assert_reply(fixture.node._handle_drop_object_replica(fixture.request), fixture.request, _Status.DROPPED)
    effect = (replace(fixture.effect, stage=Stage.SLOT_DROP)
              if fault == "active" else fixture.begin_drop())
    error = OutputPublicationConflictError
    if fault in ("active", "slot", "publication"):
        error = OutputPublicationJournalStateError
    if fault == "digest":
        effect = replace(effect, manifest_digest="0" * 64)
    elif fault == "slot":
        effect = replace(effect, slot_index=0)
    elif fault == "publication":
        effect = replace(effect, publication_id=replace(effect.publication_id, lease_id=ids.LeaseID(b"x" * 16)))
    before, journal = _snapshot(fixture), fixture.journal.snapshot(fixture.id)
    with monkeypatch.context() as guard:
        if fault == "node_id":
            guard.setattr(fixture.node, "node_id", ids.NodeID(b"n" * 16))
        elif fault == "node_pid":
            guard.setattr(fixture.node, "_node_pid", 1703)
        elif fault == "registration_epoch":
            guard.setattr(fixture.node, "_registration_epoch", 3)
        _forbid_physical_work(guard, fixture.node)
        with pytest.raises(error):
            fixture.drop(effect)
    assert _snapshot(fixture) == before
    assert fixture.journal.snapshot(fixture.id) == journal


@pytest.mark.parametrize("changed", ("attempt", "owner", "checksum", "node", "return-index"))
def test_shared_receipt_does_not_acknowledge_another_drop_identity(monkeypatch, changed):
    older = _Fixture()
    _materialize(older, "sealed", monkeypatch)
    effect = older.begin_drop()
    _assert_reply(older.drop(effect), older.request, _Status.DROPPED)
    assert len(_receipts(older.node)) == 1
    assert older.journal.ack_rollback(OutputPublicationAck(effect))
    newer = _Fixture(node=older.node, attempt=4)
    assert newer.seal() == newer.descriptor
    _remember_local_ready(newer)
    request = {
        "attempt": replace(older.request, producer_attempt_id=ids.AttemptID(older.object_id.task_id, 2)),
        "owner": replace(older.request, owner_worker_id=ids.WorkerID(b"o" * 16)),
        "checksum": replace(older.request, checksum="f" * 64),
        "node": replace(older.request, node_id=ids.NodeID(b"n" * 16)),
        "return-index": replace(older.request, object_id=replace(older.object_id, return_index=0)),
    }[changed]
    before = _snapshot(older)
    expected = _Status.REJECTED if changed in ("node", "return-index") else _Status.STALE_EPOCH
    _assert_reply(older.node._handle_drop_object_replica(request), request, expected)
    with pytest.raises(OutputPublicationConflictError):
        older.node._drop_output_publication_replica(effect, request)
    assert _snapshot(older) == before


@pytest.mark.parametrize("newer_deleted", (False, True), ids=("newer-sealed", "newer-deleted"))
def test_newer_epoch_alone_does_not_prove_old_publication_or_generic_absence(monkeypatch, newer_deleted):
    older = _Fixture()  # An intent exists, but nobody has completed its absence fence.
    newer = _Fixture(node=older.node, attempt=4)
    assert newer.seal() == newer.descriptor
    _remember_local_ready(newer)
    if newer_deleted:
        _assert_reply(newer.node._handle_drop_object_replica(newer.request), newer.request, _Status.DROPPED)
    effect = older.begin_drop()
    before = _snapshot(older)
    _assert_reply(older.node._handle_drop_object_replica(older.request), older.request, _Status.STALE_EPOCH)
    _assert_reply(older.drop(effect), older.request, _Status.STALE_EPOCH)
    assert _snapshot(older) == before
    assert not older.journal.acknowledged(effect)
    assert len(_receipts(older.node)) == int(newer_deleted)


@pytest.mark.parametrize("authority,phase", (
    ("publication", "sealed"), ("publication", "partial"),
    ("publication", "intent-only"), ("owner-wide", "sealed"), ("generic", "sealed"),
))
@pytest.mark.parametrize("after_forget", (False, True), ids=("before-manager-effect", "after-manager-effect"))
def test_forget_failure_is_not_a_completed_receipt_until_exact_cleanup_replays(monkeypatch, authority, phase, after_forget):
    fixture = _Fixture()
    _materialize(fixture, phase, monkeypatch)
    operation = _begin_cleanup(fixture, authority)
    original = fixture.node._object_manager.forget_local_replica
    calls = []

    def fail_once(object_id, *, attempt_id):
        assert (object_id, attempt_id) == (fixture.object_id, fixture.id.attempt_id)
        calls.append((object_id, attempt_id))
        assert len(calls) <= 2
        if len(calls) == 1:
            if after_forget:
                original(object_id, attempt_id=attempt_id)
            raise PullStateError("cross-cleanup forget has not acknowledged")
        return original(object_id, attempt_id=attempt_id)

    monkeypatch.setattr(fixture.node._object_manager, "forget_local_replica", fail_once)
    _attempt_cleanup(fixture, authority, operation, _Status.INCONSISTENT)
    assert not _receipts(fixture.node)
    assert not fixture.node._object_store.contains(fixture.object_id, sealed_only=False)
    assert fixture.node._dropped_metadata[fixture.object_id] == fixture.tombstone
    if phase == "sealed" and not after_forget:
        assert fixture.node._object_manager.snapshot(fixture.object_id).state is PullState.READY
    else:
        with pytest.raises(UnknownPullError):
            fixture.node._object_manager.snapshot(fixture.object_id)
    if authority == "publication":
        assert not fixture.journal.acknowledged(operation)
    elif authority == "owner-wide":
        assert operation.request_id not in fixture.node._owner_death_fence_outcomes

    # Do not create a newer epoch during unfinished cleanup or erase its marker.
    _attempt_cleanup(fixture, authority, operation, _Status.ALREADY_DROPPED)
    assert calls == [(fixture.object_id, fixture.id.attempt_id)] * 2
    assert len(_receipts(fixture.node)) == 1
    assert not fixture.node._sealed_metadata and not fixture.node._local_replica_write_claims
    with pytest.raises(UnknownPullError):
        fixture.node._object_manager.snapshot(fixture.object_id)
    _generic_replay_without_physical_work(fixture, monkeypatch)
    assert len(calls) == 2


@pytest.mark.parametrize("authority", ("publication", "owner-wide", "generic"))
def test_live_pin_prevents_all_cleanup_authorities_from_publishing_absence(monkeypatch, authority):
    fixture = _Fixture()
    _materialize(fixture, "sealed", monkeypatch)
    pin = fixture.node._object_store.pin(fixture.object_id, "cross-cleanup-live-reader")
    operation = _begin_cleanup(fixture, authority)
    _attempt_cleanup(fixture, authority, operation, _Status.PINNED)
    _assert_reply(fixture.node._handle_drop_object_replica(fixture.request), fixture.request, _Status.PINNED)
    assert not _receipts(fixture.node) and not fixture.node._dropped_metadata
    assert fixture.node._object_store.get(fixture.object_id) == fixture.payload
    assert fixture.node._object_store.snapshot(fixture.object_id).pin_count == 1
    assert fixture.node._object_manager.snapshot(fixture.object_id).state is PullState.READY

    assert fixture.node._object_store.unpin(fixture.object_id, pin)
    _attempt_cleanup(fixture, authority, operation, _Status.DROPPED)
    assert len(_receipts(fixture.node)) == 1
    assert fixture.node._dropped_metadata[fixture.object_id] == fixture.tombstone
    _generic_replay_without_physical_work(fixture, monkeypatch)


def test_generic_pending_forget_blocks_reseal_until_publication_finishes_same_cleanup(monkeypatch):
    fixture = _Fixture()
    _materialize(fixture, "sealed", monkeypatch)
    operation = fixture.begin_drop()
    original = fixture.node._object_manager.forget_local_replica
    calls = []

    def fail_first(object_id, *, attempt_id):
        calls.append((object_id, attempt_id))
        assert len(calls) <= 2
        assert (object_id, attempt_id) == (fixture.object_id, fixture.id.attempt_id)
        if len(calls) == 1:
            raise PullStateError("pending generic manager cleanup")
        return original(object_id, attempt_id=attempt_id)

    monkeypatch.setattr(fixture.node._object_manager, "forget_local_replica", fail_first)
    _assert_reply(fixture.node._handle_drop_object_replica(fixture.request), fixture.request, _Status.INCONSISTENT)
    assert not _receipts(fixture.node)
    assert not fixture.node._object_store.contains(fixture.object_id, sealed_only=False)
    assert fixture.node._sealed_metadata[fixture.object_id] == fixture.metadata
    assert fixture.node._object_manager.snapshot(fixture.object_id).state is PullState.READY
    newer_seal = protocol.SealObject.from_data(
        fixture.object_id, fixture.id.attempt_id.next(), fixture.descriptor.owner_worker_id, b"newer bytes",
    )
    before = _snapshot(fixture)
    rejected = fixture.node._handle_seal_object(newer_seal)
    assert not rejected.sealed
    assert _snapshot(fixture) == before

    # The other existing authority completes the exact absent-byte work marker.
    _assert_reply(fixture.drop(operation), fixture.request, _Status.ALREADY_DROPPED)
    assert len(calls) == 2 and len(_receipts(fixture.node)) == 1
    assert not fixture.node._sealed_metadata and not fixture.node._local_replica_write_claims
    assert fixture.journal.ack_rollback(OutputPublicationAck(operation))
    assert fixture.node._handle_seal_object(newer_seal).sealed
    assert fixture.node._sealed_metadata[fixture.object_id][0] == newer_seal.attempt_id
    assert fixture.node._object_store.get(fixture.object_id) == newer_seal.data
    _generic_replay_without_physical_work(fixture, monkeypatch)
    _publication_replay_without_physical_work(fixture, operation, monkeypatch)
    assert len(calls) == 2


def test_generic_completion_retires_exact_partial_claim_after_publication_forget_failure(monkeypatch):
    fixture = _Fixture()
    _materialize(fixture, "partial", monkeypatch)
    operation = fixture.begin_drop()
    original = fixture.node._object_manager.forget_local_replica
    calls = []

    def fail_first(object_id, *, attempt_id):
        calls.append((object_id, attempt_id))
        assert len(calls) <= 2
        assert (object_id, attempt_id) == (fixture.object_id, fixture.id.attempt_id)
        if len(calls) == 1:
            raise PullStateError("pending publication partial cleanup")
        return original(object_id, attempt_id=attempt_id)

    monkeypatch.setattr(fixture.node._object_manager, "forget_local_replica", fail_first)
    _assert_reply(fixture.drop(operation), fixture.request, _Status.INCONSISTENT)
    assert not _receipts(fixture.node)
    assert fixture.object_id in fixture.node._local_replica_write_claims
    assert not fixture.node._object_store.contains(fixture.object_id, sealed_only=False)
    assert fixture.node._dropped_metadata[fixture.object_id] == fixture.tombstone
    assert not fixture.journal.acknowledged(operation)

    _assert_reply(fixture.node._handle_drop_object_replica(fixture.request), fixture.request, _Status.ALREADY_DROPPED)
    assert len(calls) == 2 and len(_receipts(fixture.node)) == 1
    assert not fixture.node._local_replica_write_claims and not fixture.node._sealed_metadata
    _publication_replay_without_physical_work(fixture, operation, monkeypatch)
    assert fixture.journal.ack_rollback(OutputPublicationAck(operation))
    newer = _Fixture(node=fixture.node, attempt=4)
    assert newer.seal() == newer.descriptor
    _remember_local_ready(newer)
    _generic_replay_without_physical_work(fixture, monkeypatch)
    assert len(calls) == 2


@pytest.mark.parametrize("authority", ("generic", "publication", "owner-wide"))
@pytest.mark.parametrize("after_delete", (False, True), ids=("before-delete", "after-delete"))
def test_delete_unknown_keeps_fence_and_metadata_until_exact_cleanup(monkeypatch, authority, after_delete):
    fixture = _Fixture()
    _materialize(fixture, "sealed", monkeypatch)
    operation = _begin_cleanup(fixture, authority)
    store = fixture.node._object_store
    original = store.delete

    def lose_delete(object_id):
        assert object_id == fixture.object_id
        if after_delete:
            assert original(object_id)
        raise RuntimeError("physical delete did not return")

    with monkeypatch.context() as fault:
        fault.setattr(store, "delete", lose_delete)
        _attempt_cleanup(fixture, authority, operation, _Status.INCONSISTENT)
    assert not _receipts(fixture.node)
    assert fixture.node._sealed_metadata[fixture.object_id] == fixture.metadata
    assert fixture.node._dropped_metadata[fixture.object_id] == fixture.tombstone
    assert store.contains(fixture.object_id) is not after_delete
    assert fixture.node._object_manager.snapshot(fixture.object_id).state is PullState.READY
    # Even the exact old Seal must not acknowledge deleted or deletion-fenced
    # metadata; a newer Seal cannot displace the unfinished cleanup either.
    for attempt in (fixture.id.attempt_id, fixture.id.attempt_id.next()):
        seal = protocol.SealObject.from_data(
            fixture.object_id, attempt, fixture.descriptor.owner_worker_id, fixture.payload,
        )
        assert not fixture.node._handle_seal_object(seal).sealed
    _attempt_cleanup(fixture, authority, operation,
                     _Status.ALREADY_DROPPED if after_delete else _Status.DROPPED)
    assert not fixture.node._sealed_metadata and not fixture.node._local_replica_write_claims
    assert not store.contains(fixture.object_id, sealed_only=False)
    with pytest.raises(UnknownPullError):
        fixture.node._object_manager.snapshot(fixture.object_id)
    _generic_replay_without_physical_work(fixture, monkeypatch)


@pytest.mark.parametrize("phase", ("partial", "intent-only"))
@pytest.mark.parametrize("after_forget", (False, True), ids=("before-forget", "after-forget"))
def test_pending_absence_claim_blocks_new_seal_and_pull_until_exact_handoff(monkeypatch, phase, after_forget):
    fixture = _Fixture()
    _materialize(fixture, phase, monkeypatch)
    operation = fixture.begin_drop()
    manager = fixture.node._object_manager
    original = manager.forget_local_replica

    def lose_forget(object_id, *, attempt_id):
        if after_forget:
            original(object_id, attempt_id=attempt_id)
        raise PullStateError("absence cleanup did not return")

    with monkeypatch.context() as fault:
        fault.setattr(manager, "forget_local_replica", lose_forget)
        _assert_reply(fixture.drop(operation), fixture.request, _Status.INCONSISTENT)
    assert not _receipts(fixture.node)
    assert not fixture.node._object_store.contains(fixture.object_id, sealed_only=False)
    assert fixture.object_id in fixture.node._local_replica_write_claims
    assert fixture.node._dropped_metadata[fixture.object_id] == fixture.tombstone
    newer = _Fixture(node=fixture.node, attempt=4)
    before = _snapshot(fixture)
    assert not fixture.node._handle_seal_object(protocol.SealObject.from_data(
        newer.object_id, newer.id.attempt_id, newer.descriptor.owner_worker_id, newer.payload,
    )).sealed
    with pytest.raises(OutputPublicationConflictError, match="uncommitted writer"):
        newer.seal()
    descriptor = protocol.ObjectStoreDescriptor(
        newer.object_id, newer.descriptor.owner_worker_id, newer.id.attempt_id,
        newer.node.node_id, newer.descriptor.size_bytes, newer.descriptor.checksum,
    )
    with pytest.raises(RuntimeError, match="unfinished publication custody"):
        newer.node._localize_one_dependency(descriptor)
    assert _snapshot(fixture) == before
    # Generic GC can finish the exact fenced absence, but never invents the
    # publication rollback ACK or retires another materializer's identity.
    _assert_reply(fixture.node._handle_drop_object_replica(fixture.request), fixture.request, _Status.ALREADY_DROPPED)
    assert not fixture.node._local_replica_write_claims
    _publication_replay_without_physical_work(fixture, operation, monkeypatch)
    assert not fixture.journal.acknowledged(operation)
    assert fixture.journal.ack_rollback(OutputPublicationAck(operation))
    assert newer.seal() == newer.descriptor
    _generic_replay_without_physical_work(fixture, monkeypatch)


def test_generic_absence_receipt_cannot_retire_a_rebound_partial_claim(monkeypatch):
    fixture = _Fixture()
    _materialize(fixture, "partial", monkeypatch)
    operation = fixture.begin_drop()
    manager = fixture.node._object_manager

    def reject_forget(*_args, **_kwargs):
        raise PullStateError("claim still needs manager cleanup")

    with monkeypatch.context() as fault:
        fault.setattr(manager, "forget_local_replica", reject_forget)
        _assert_reply(fixture.drop(operation), fixture.request, _Status.INCONSISTENT)
    claims = fixture.node._local_replica_write_claims
    original = claims[fixture.object_id]
    claims[fixture.object_id] = replace(original, effect=replace(original.effect, slot_index=0))
    before = _snapshot(fixture)
    _assert_reply(fixture.node._handle_drop_object_replica(fixture.request), fixture.request, _Status.INCONSISTENT)
    assert _snapshot(fixture) == before and not _receipts(fixture.node)
