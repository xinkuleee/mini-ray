"""Defensive sealed-byte integrity checks across existing cleanup authorities.

Each case seals one eight-byte object through the real Node handler into a
1 KiB store, then injects corrupt private sealed bytes or a read exception.
Some cases retain a real pin while checking repair and later release. These
are defensive fault models, not evidence that supported public fail-stop
operations can produce inconsistent metadata. No process, thread, socket,
timer, real wait or model runtime is started.
"""

from __future__ import annotations

import hashlib
import multiprocessing.process
import socket
import subprocess
import threading
import time
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.core import CoreWorker
from miniray.ids import WorkerID
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.transport import TCPServer
from miniray.worker import WorkerServer
from tests.unit.test_node_owner_death_fence import _bare_node, _death, _descriptor


pytestmark = pytest.mark.unit
_ORIGINAL = b"expected"
_CORRUPT = b"expecteD"


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("replica-integrity pure test attempted runtime infrastructure")

    for kind, method in (
        (NodeServer, "__init__"), (WorkerServer, "__init__"),
        (CoreWorker, "__init__"), (TCPServer, "__init__"),
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr("miniray.node.rpc_request", forbidden)
    monkeypatch.setattr("miniray.core.rpc_request", forbidden)
    monkeypatch.setattr("miniray.worker.rpc_request", forbidden)


def _sealed_node():
    node = _bare_node()
    node._object_store = ObjectStore(1024)
    node._object_manager = ObjectManager(node.node_id, node._object_store)
    owner = WorkerID(bytes([26]) * 16)
    descriptor = _descriptor(node.node_id, owner, 27, _ORIGINAL)
    request = protocol.SealObject.from_data(
        descriptor.object_id, descriptor.producer_attempt_id, owner, _ORIGINAL,
    )
    sealed = node._handle_seal_object(request)
    assert sealed.sealed and sealed.checksum == descriptor.checksum
    assert node.object_store.get(descriptor.object_id) == _ORIGINAL
    metadata = dict(node._sealed_metadata)
    assert metadata == {descriptor.object_id: (
        descriptor.producer_attempt_id, owner, len(_ORIGINAL), descriptor.checksum,
    )}
    return node, descriptor, metadata


def _corrupted_node():
    node, descriptor, metadata = _sealed_node()
    assert len(_ORIGINAL) == len(_CORRUPT) == 8
    assert hashlib.sha256(_CORRUPT).hexdigest() != descriptor.checksum
    # Fault injection is deliberately below the public immutable-store API.
    # No owner/attempt/checksum field is forged or changed.
    node.object_store._entries[descriptor.object_id].sealed_data = _CORRUPT
    return node, descriptor, metadata


def _assert_preserved(node, descriptor, metadata):
    assert node._sealed_metadata == metadata
    assert node.object_store.object_ids(sealed_only=False) == (descriptor.object_id,)
    assert node.object_store.get(descriptor.object_id) == _CORRUPT
    assert node.object_store.used_bytes == descriptor.size_bytes == 8
    physical = node.object_store.snapshot(descriptor.object_id)
    assert physical.sealed and physical.size_bytes == descriptor.size_bytes
    assert physical.pin_count == 0
    assert not node._dropped_metadata
    assert not getattr(node, "_replica_drop_receipts", set())


def _restore_original(node, descriptor):
    # Simulate explicit repair of the same physical bytes, not a new Seal or
    # producer epoch; owner-wide death fences correctly prohibit resealing.
    node.object_store._entries[descriptor.object_id].sealed_data = _ORIGINAL
    assert node.object_store.get(descriptor.object_id) == _ORIGINAL


def test_generic_drop_preserves_same_size_corruption_until_explicit_repair():
    node, descriptor, metadata = _corrupted_node()
    request = protocol.DropObjectReplica(
        descriptor.object_id, descriptor.producer_attempt_id,
        descriptor.owner_worker_id, node.node_id, descriptor.checksum,
    )
    for _ in range(2):
        reply = node._handle_drop_object_replica(request)
        assert type(reply) is protocol.DropObjectReplicaReply
        assert (reply.object_id, reply.producer_attempt_id, reply.owner_worker_id,
                reply.node_id, reply.checksum) == (
            request.object_id, request.producer_attempt_id, request.owner_worker_id,
            request.node_id, request.checksum,
        )
        assert reply.status is protocol.DropObjectReplicaStatus.INCONSISTENT
        assert not reply.accepted and not reply.dropped and reply.error
        _assert_preserved(node, descriptor, metadata)

    _restore_original(node, descriptor)
    dropped = node._handle_drop_object_replica(request)
    assert dropped.status is protocol.DropObjectReplicaStatus.DROPPED
    assert dropped.accepted and dropped.dropped
    assert node.object_store.used_bytes == 0 and not node._sealed_metadata
    assert node._dropped_metadata[descriptor.object_id] == (
        descriptor.producer_attempt_id, descriptor.owner_worker_id, descriptor.checksum,
    )
    assert node._replica_drop_receipts == {node._replica_drop_key(request)}
    replay = node._handle_drop_object_replica(request)
    assert replay.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
    assert replay.accepted and not replay.dropped


def test_owner_wide_sweep_preserves_same_size_corruption_until_explicit_repair():
    node, descriptor, metadata = _corrupted_node()
    request = protocol.InstallOwnerDeathFence(
        "corrupt-owner-wide", _death(descriptor.owner_worker_id), node.node_id,
        scope=protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
    )
    expected = (protocol.OwnerDeathReplicaObservation(
        descriptor, protocol.OwnerDeathReplicaStatus.CONFLICT,
    ),)
    for _ in range(2):
        reply = node._handle_install_owner_death_fence(request)
        assert type(reply) is protocol.InstallOwnerDeathFenceReply
        assert reply.request == request and reply.accepted
        assert reply.observations == expected
        assert not reply.complete and not reply.retryable
        assert request.request_id not in node._owner_death_fence_outcomes
        assert node._owner_death_fences[descriptor.owner_worker_id] == request.owner_death
        _assert_preserved(node, descriptor, metadata)

    _restore_original(node, descriptor)
    completed = node._handle_install_owner_death_fence(request)
    assert completed.request == request and completed.accepted and completed.complete
    assert completed.observations == (protocol.OwnerDeathReplicaObservation(
        descriptor, protocol.OwnerDeathReplicaStatus.ABSENT,
    ),)
    assert node.object_store.used_bytes == 0 and not node._sealed_metadata
    assert node._dropped_metadata[descriptor.object_id] == (
        descriptor.producer_attempt_id, descriptor.owner_worker_id, descriptor.checksum,
    )
    assert node._replica_drop_receipts == {
        node._replica_drop_key(completed.observations[0].drop_request),
    }
    assert node._handle_install_owner_death_fence(request) is completed


def test_publication_exact_freezes_conflict_without_deleting_corrupt_bytes():
    node, descriptor, metadata = _corrupted_node()
    request = protocol.InstallOwnerDeathFence(
        "corrupt-publication-exact", _death(descriptor.owner_worker_id),
        node.node_id, (descriptor,),
    )
    first = node._handle_install_owner_death_fence(request)
    assert type(first) is protocol.InstallOwnerDeathFenceReply
    assert first.request == request and first.accepted and first.complete
    assert first.observations == (protocol.OwnerDeathReplicaObservation(
        descriptor, protocol.OwnerDeathReplicaStatus.CONFLICT,
    ),)
    assert node._handle_install_owner_death_fence(request) is first
    _assert_preserved(node, descriptor, metadata)

    _restore_original(node, descriptor)
    # PUBLICATION_EXACT completes the immutable witness, not physical GC. An
    # exact replay must retain its original observation even after repair.
    assert node._handle_install_owner_death_fence(request) is first
    fresh = replace(request, request_id="repaired-publication-exact")
    observed = node._handle_install_owner_death_fence(fresh)
    assert observed.request == fresh and observed.accepted and observed.complete
    assert observed.observations == (protocol.OwnerDeathReplicaObservation(
        descriptor, protocol.OwnerDeathReplicaStatus.PRESENT,
    ),)
    assert node._sealed_metadata == metadata
    assert node.object_store.get(descriptor.object_id) == _ORIGINAL
    assert node.object_store.used_bytes == 8
    assert not node._dropped_metadata and not getattr(node, "_replica_drop_receipts", set())


@pytest.mark.parametrize("authority", ("generic", "owner-wide"))
@pytest.mark.parametrize("fault", ("get-error", "length-drift"))
def test_read_integrity_failure_preserves_live_pin_until_repair_and_release(
    monkeypatch, authority, fault,
):
    node, descriptor, metadata = _sealed_node()
    store = node.object_store
    pin = store.pin(descriptor.object_id, "integrity-live-reader")
    drop = protocol.DropObjectReplica(
        descriptor.object_id, descriptor.producer_attempt_id,
        descriptor.owner_worker_id, node.node_id, descriptor.checksum,
    )
    request = (drop if authority == "generic" else protocol.InstallOwnerDeathFence(
        "integrity-reader-fault", _death(descriptor.owner_worker_id), node.node_id,
        scope=protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
    ))

    def invoke():
        return (node._handle_drop_object_replica(request) if authority == "generic"
                else node._handle_install_owner_death_fence(request))

    reads = []

    def unreadable(object_id):
        assert object_id == descriptor.object_id
        reads.append(object_id)
        assert len(reads) <= 2
        raise RuntimeError("injected local replica read failure")

    if fault == "length-drift":
        # Keep the allocated snapshot size and sealed metadata untouched.
        # Checking only snapshot.size_bytes cannot detect this actual length.
        store._entries[descriptor.object_id].sealed_data = b"short"
    with monkeypatch.context() as fault_patch:
        if fault == "get-error":
            fault_patch.setattr(store, "get", unreadable)
        failed = invoke()

    if authority == "generic":
        assert type(failed) is protocol.DropObjectReplicaReply
        assert failed.status is protocol.DropObjectReplicaStatus.INCONSISTENT
        assert not failed.accepted and not failed.dropped and failed.error
    else:
        assert type(failed) is protocol.InstallOwnerDeathFenceReply
        assert failed.request == request and failed.accepted
        assert failed.observations == (protocol.OwnerDeathReplicaObservation(
            descriptor, protocol.OwnerDeathReplicaStatus.CONFLICT,
        ),)
        assert not failed.complete and not failed.retryable
        assert request.request_id not in node._owner_death_fence_outcomes
    if fault == "get-error":
        # Sweep observation and its independently validated physical tail both
        # fail; an unreadable replica must never become an absence ACK.
        assert reads == [descriptor.object_id] * (1 if authority == "generic" else 2)
        assert store.get(descriptor.object_id) == _ORIGINAL
    else:
        assert reads == [] and store.get(descriptor.object_id) == b"short"
        assert len(store.get(descriptor.object_id)) != descriptor.size_bytes
    assert node._sealed_metadata == metadata
    assert store.used_bytes == descriptor.size_bytes == 8
    physical = store.snapshot(descriptor.object_id)
    assert physical.sealed and physical.size_bytes == 8 and physical.pin_count == 1
    assert not node._dropped_metadata and not getattr(node, "_replica_drop_receipts", set())

    if fault == "length-drift":
        _restore_original(node, descriptor)
    pinned = invoke()
    if authority == "generic":
        assert pinned.status is protocol.DropObjectReplicaStatus.PINNED
        assert not pinned.accepted and not pinned.dropped
    else:
        assert pinned.request == request and pinned.accepted
        assert pinned.observations == (protocol.OwnerDeathReplicaObservation(
            descriptor, protocol.OwnerDeathReplicaStatus.PINNED, 1,
        ),)
        assert not pinned.complete and pinned.retryable
        assert request.request_id not in node._owner_death_fence_outcomes
    assert node._sealed_metadata == metadata and store.get(descriptor.object_id) == _ORIGINAL
    assert store.snapshot(descriptor.object_id).pin_count == 1
    assert not node._dropped_metadata and not getattr(node, "_replica_drop_receipts", set())

    assert store.unpin(descriptor.object_id, pin)
    completed = invoke()
    if authority == "generic":
        assert completed.status is protocol.DropObjectReplicaStatus.DROPPED
        assert completed.accepted and completed.dropped
    else:
        assert completed.request == request and completed.accepted and completed.complete
        assert completed.observations == (protocol.OwnerDeathReplicaObservation(
            descriptor, protocol.OwnerDeathReplicaStatus.ABSENT,
        ),)
    assert store.used_bytes == 0 and not node._sealed_metadata
    assert node._dropped_metadata[descriptor.object_id] == (
        descriptor.producer_attempt_id, descriptor.owner_worker_id, descriptor.checksum,
    )
    assert node._replica_drop_receipts == {node._replica_drop_key(drop)}
    replay = invoke()
    if authority == "generic":
        assert replay.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
        assert replay.accepted and not replay.dropped
    else:
        assert replay is completed
