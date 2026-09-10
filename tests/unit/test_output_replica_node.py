"""Bounded local storage bridge contracts: one <=1 KiB store, no runtime."""

from __future__ import annotations

import socket
import threading
import time
from dataclasses import replace

import pytest

from miniray import ids, protocol
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_publication import OutputPublicationConflictError, OutputPublicationManifest
from miniray.output_publication_journal import (
    OutputPublicationAck, OutputPublicationEffect, OutputPublicationJournal,
    OutputPublicationJournalState, OutputPublicationJournalStateError,
    OutputPublicationStage as Stage,
)
from tests.unit.test_output_publication_node_server import _Values


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("local replica contract attempted runtime work")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _Fixture:
    def __init__(self, *, node=None, attempt=3):
        values = _Values(refs=False, stored=True)
        publication_id = replace(values.publication_id, execution=replace(
            values.execution, attempt_id=ids.AttemptID(values.task, attempt),
        ))
        self.manifest = OutputPublicationManifest.create(
            replace(values.header, publication_id=publication_id), (values.value),
        )
        self.id = publication_id
        self.payload = (values.payload)
        self.descriptor = (values.result)
        self.object_id = publication_id.object_id
        assert self.descriptor.object_id == self.object_id
        if node is None:
            node = object.__new__(NodeServer)
            node.node_id = values.node
            node._node_pid = 1701
            node._registration_epoch = 2
            node._state_lock = threading.RLock()
            node._object_store = ObjectStore(1024)
            node._object_manager = ObjectManager(node.node_id, node._object_store)
            node._sealed_metadata = {}
            node._dropped_metadata = {}
            node._local_replica_write_claims = {}
            node._object_localization_locks = {}
            node._owner_death_fences = {}
            node._output_publication_journal = OutputPublicationJournal()
        self.node = node
        self.journal = node._output_publication_journal
        self.journal.open(self.manifest)
        self.journal.ack_owner_registered(OutputPublicationAck(self.journal.begin_owner_register(self.id)))
        self.effect = self.journal.begin_materialize(self.id)
        self.request = protocol.DropObjectReplica(
            self.object_id, self.id.attempt_id, self.descriptor.owner_worker_id,
            self.descriptor.node_id, self.descriptor.checksum,
        )

    @property
    def metadata(self):
        return (self.id.attempt_id, self.descriptor.owner_worker_id,
                self.descriptor.size_bytes, self.descriptor.checksum)

    @property
    def tombstone(self):
        return (self.id.attempt_id, self.descriptor.owner_worker_id, self.descriptor.checksum)

    def seal(self):
        return self.node._seal_output_publication_replica(self.effect, self.descriptor, self.payload)

    def begin_drop(self):
        plan = self.journal.begin_rollback(self.id, "local-storage-rollback")
        assert len(plan.effects) == 1
        return plan.effects[0]

    def drop(self, effect):
        return self.node._drop_output_publication_replica(effect, self.request)


def test_single_output_seal_is_idempotent_and_uses_ordinary_metadata():
    fixture = _Fixture()
    assert fixture.effect.transfer_index is None and fixture.object_id.return_index == 0
    assert fixture.seal() == fixture.descriptor
    assert fixture.seal() == fixture.descriptor
    assert fixture.node._sealed_metadata == {fixture.object_id: fixture.metadata}
    assert fixture.node._object_store.get(fixture.object_id) == fixture.payload
    assert fixture.node._object_store.used_bytes == len(fixture.payload)
    assert fixture.node._local_replica_write_claims == {}
    assert not fixture.journal.acknowledged(fixture.effect)


def test_partial_write_claim_allows_exact_rollback_and_ack_replay(monkeypatch):
    fixture = _Fixture()
    store = fixture.node._object_store
    write = store.write

    def fail_write(object_id, payload):
        write(object_id, payload[:2])
        raise RuntimeError("partial write failed")

    monkeypatch.setattr(store, "write", fail_write)
    with pytest.raises(RuntimeError, match="partial write"):
        fixture.seal()
    assert not store.snapshot(fixture.object_id).sealed
    assert fixture.node._sealed_metadata == {}
    assert fixture.object_id in fixture.node._local_replica_write_claims
    drop = fixture.begin_drop()
    assert fixture.drop(drop).status is protocol.DropObjectReplicaStatus.DROPPED
    assert store.used_bytes == 0
    assert fixture.node._local_replica_write_claims == {}
    assert fixture.node._dropped_metadata[fixture.object_id] == fixture.tombstone
    assert fixture.journal.snapshot(fixture.id).state is OutputPublicationJournalState.ROLLING_BACK
    fixture.journal.ack_rollback(OutputPublicationAck(drop))
    assert fixture.drop(drop).status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED


@pytest.mark.parametrize("replay_seal", (False, True))
def test_seal_before_metadata_failure_keeps_exact_write_custody(monkeypatch, replay_seal):
    fixture = _Fixture()
    store = fixture.node._object_store
    seal = store.seal

    def fail_seal(object_id):
        seal(object_id)
        raise RuntimeError("seal ACK lost")

    monkeypatch.setattr(store, "seal", fail_seal)
    with pytest.raises(RuntimeError, match="ACK lost"):
        fixture.seal()
    assert store.contains(fixture.object_id)
    assert fixture.node._sealed_metadata == {}
    assert fixture.object_id in fixture.node._local_replica_write_claims
    if replay_seal:
        assert fixture.seal() == fixture.descriptor
        assert fixture.node._sealed_metadata[fixture.object_id] == fixture.metadata
        assert fixture.node._local_replica_write_claims == {}
    assert fixture.drop(fixture.begin_drop()).status is protocol.DropObjectReplicaStatus.DROPPED
    assert store.used_bytes == 0


@pytest.mark.parametrize("sealed", (False, True))
def test_unknown_partial_or_sealed_bytes_are_never_claimed_or_deleted(sealed):
    fixture = _Fixture()
    store = fixture.node._object_store
    store.create(fixture.object_id, len(fixture.payload))
    store.write(fixture.object_id, fixture.payload)
    if sealed:
        store.seal(fixture.object_id)
    with pytest.raises(OutputPublicationJournalStateError):
        fixture.seal()
    assert fixture.node._local_replica_write_claims == {}
    reply = fixture.drop(fixture.begin_drop())
    assert reply.status is protocol.DropObjectReplicaStatus.INCONSISTENT
    assert store.used_bytes == len(fixture.payload)
    assert fixture.node._dropped_metadata == {}


def test_other_attempt_sealed_replica_is_preserved():
    older = _Fixture()
    newer = _Fixture(node=older.node, attempt=4)
    newer.seal()
    with pytest.raises(OutputPublicationConflictError):
        older.seal()
    assert older.drop(older.begin_drop()).status is protocol.DropObjectReplicaStatus.STALE_EPOCH
    assert older.node._sealed_metadata[older.object_id] == newer.metadata
    assert older.node._object_store.get(older.object_id) == newer.payload


def test_drop_before_seal_fences_old_attempt_but_allows_new_attempt():
    older = _Fixture()
    drop = older.begin_drop()
    assert older.drop(drop).status is protocol.DropObjectReplicaStatus.DROPPED
    assert older.node._dropped_metadata[older.object_id] == older.tombstone
    with pytest.raises(OutputPublicationJournalStateError):
        older.seal()
    older.journal.ack_rollback(OutputPublicationAck(drop))
    assert older.drop(drop).status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    newer = _Fixture(node=older.node, attempt=4)
    assert newer.seal() == newer.descriptor
    # Exact completed cleanup remains ACK-able without touching the new copy.
    assert older.drop(drop).status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert older.node._object_store.get(older.object_id) == newer.payload


@pytest.mark.parametrize("partial", (False, True))
def test_manager_cleanup_failure_retains_exact_replay_work(monkeypatch, partial):
    fixture = _Fixture()
    store = fixture.node._object_store
    if partial:
        write = store.write

        def fail_write(object_id, payload):
            write(object_id, payload[:1])
            raise RuntimeError("write interrupted")

        monkeypatch.setattr(store, "write", fail_write)
        with pytest.raises(RuntimeError):
            fixture.seal()
    else:
        fixture.seal()
    manager = fixture.node._object_manager
    forget = manager.forget_local_replica
    calls = []

    def fail_once(object_id, *, attempt_id):
        calls.append((object_id, attempt_id))
        forget(object_id, attempt_id=attempt_id)
        if len(calls) == 1:
            raise RuntimeError("manager ACK lost")

    monkeypatch.setattr(manager, "forget_local_replica", fail_once)
    drop = fixture.begin_drop()
    assert fixture.drop(drop).status is protocol.DropObjectReplicaStatus.INCONSISTENT
    assert store.used_bytes == 0
    assert fixture.node._dropped_metadata[fixture.object_id] == fixture.tombstone
    assert not fixture.journal.acknowledged(drop)
    if partial:
        assert fixture.object_id in fixture.node._local_replica_write_claims
    else:
        assert fixture.node._sealed_metadata[fixture.object_id] == fixture.metadata
    assert fixture.drop(drop).status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert fixture.node._sealed_metadata == fixture.node._local_replica_write_claims == {}
    assert calls == [(fixture.object_id, fixture.id.attempt_id)] * 2
    fixture.journal.ack_rollback(OutputPublicationAck(drop))
    assert fixture.drop(drop).status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED


def test_pinned_sealed_replica_needs_unpin_before_cleanup():
    fixture = _Fixture()
    fixture.seal()
    pin = fixture.node._object_store.pin(fixture.object_id, "unit-pin")
    drop = fixture.begin_drop()
    assert fixture.drop(drop).status is protocol.DropObjectReplicaStatus.PINNED
    assert fixture.node._dropped_metadata == {}
    fixture.node._object_store.unpin(fixture.object_id, pin)
    assert fixture.drop(drop).status is protocol.DropObjectReplicaStatus.DROPPED


@pytest.mark.parametrize("field", ("node_id", "_node_pid", "_registration_epoch"))
def test_wrong_node_incarnation_never_changes_local_storage(field):
    fixture = _Fixture()
    value = ids.NodeID(b"n" * 16) if field == "node_id" else 999
    setattr(fixture.node, field, value)
    with pytest.raises(OutputPublicationConflictError, match="incarnation"):
        fixture.seal()
    with pytest.raises(OutputPublicationConflictError, match="incarnation"):
        fixture.drop(fixture.begin_drop())
    assert fixture.node._object_store.used_bytes == 0
    assert fixture.node._dropped_metadata == fixture.node._local_replica_write_claims == {}


@pytest.mark.parametrize("changed", ("digest", "stage-child", "attempt", "owner", "bytes", "nested"))
def test_malformed_materialization_identity_is_rejected_before_claim(changed):
    fixture = _Fixture()
    effect, descriptor, payload = fixture.effect, fixture.descriptor, fixture.payload
    if changed == "digest":
        effect = replace(effect, manifest_digest="0" * 64)
    elif changed == "stage-child":
        effect = replace(effect)
        object.__setattr__(effect, "transfer_index", 0)
    elif changed == "attempt":
        effect = replace(effect, publication_id=replace(effect.publication_id, execution=replace(
            effect.publication_id.execution, attempt_id=ids.AttemptID(fixture.id.task_id, 77),
        )))
    elif changed == "owner":
        descriptor = replace(descriptor, owner_worker_id=ids.WorkerID(b"o" * 16))
    elif changed == "bytes":
        payload = b"x" * len(payload)
    else:
        object.__setattr__(descriptor.object_id, "return_index", True)
    with pytest.raises((TypeError, ValueError)):
        fixture.node._seal_output_publication_replica(effect, descriptor, payload)
    assert fixture.node._object_store.used_bytes == 0
    assert fixture.node._local_replica_write_claims == {}


def test_drop_requires_exact_next_rollback_effect_and_deep_request_identity():
    fixture = _Fixture()
    drop = replace(fixture.effect, stage=Stage.SLOT_DROP)
    with pytest.raises(OutputPublicationJournalStateError):
        fixture.drop(drop)
    fixture.journal.begin_materialize(fixture.id)
    fixture.journal.begin_rollback(fixture.id, "ordered-rollback")
    wrong = replace(drop)
    object.__setattr__(wrong, "transfer_index", 0)
    with pytest.raises(OutputPublicationConflictError):
        fixture.node._drop_output_publication_replica(wrong, fixture.request)
    request = replace(fixture.request, producer_attempt_id=ids.AttemptID(fixture.id.task_id, 88))
    with pytest.raises(OutputPublicationConflictError):
        fixture.node._drop_output_publication_replica(drop, request)
    request = replace(fixture.request, owner_worker_id=ids.WorkerID(b"m" * 16))
    object.__setattr__(request.owner_worker_id, "value", bytearray(b"m" * 16))
    with pytest.raises(TypeError):
        fixture.node._drop_output_publication_replica(drop, request)
    assert fixture.node._dropped_metadata == {}
    assert fixture.node._object_store.used_bytes == 0


def test_seal_reply_cannot_mutate_internal_metadata_and_drop_reply_cannot_mutate_tombstone():
    fixture = _Fixture()
    descriptor = fixture.seal()
    object.__setattr__(descriptor.owner_worker_id, "value", b"q" * 16)
    assert fixture.node._sealed_metadata[fixture.object_id] == fixture.metadata
    reply = fixture.drop(fixture.begin_drop())
    object.__setattr__(reply.owner_worker_id, "value", b"r" * 16)
    object.__setattr__(reply.producer_attempt_id, "attempt_number", 88)
    assert fixture.node._dropped_metadata[fixture.object_id] == fixture.tombstone
