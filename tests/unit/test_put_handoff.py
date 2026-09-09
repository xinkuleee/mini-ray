"""Pure put discovery: one container, <=3 handles and <=4 KiB per case.

Detached ObjectRefs provide discovery metadata only; these tests do not claim
owner admission, RPC publication or GC evidence. No runtime is constructed and
all child-owner effects are forbidden. Actual cloudpickle handles aliases and
Python container cycles; no graph algorithm or fake Task/lease is involved.
"""

from __future__ import annotations

import hashlib
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time
import weakref
from dataclasses import replace

import cloudpickle
import pytest

from miniray import control, core as core_module, node, protocol, transport, worker
from miniray.contained_edges import ContainedReferenceHold
from miniray.core import CoreWorker, ObjectRef
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable
from miniray.publication_sources import BorrowedContainedSource, OwnedContainedSource
from miniray.put_handoff import PutManifest, discover_put
from miniray.ref_transfer import current_exporter, exporting_references, importing_references


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure put discovery attempted runtime or reference effects")

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "shutdown"),
        (node.NodeServer, "__init__"), (worker.WorkerServer, "__init__"),
        (control.GCSLite, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Thread, "__init__"), (threading.Thread, "start"),
        (threading.Thread, "join"), (threading.Timer, "__init__"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (threading.Condition, "wait_for"), (threading.Barrier, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
        (ObjectOwnerTable, "prepare_stored_contained_reference"),
        (ObjectOwnerTable, "promote_stored_contained_reference"),
        (ObjectOwnerTable, "add_contained_reference"),
        (ObjectOwnerTable, "release_contained_reference"), (ObjectRef, "close"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (control, core_module, node, worker):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)


def _identity():
    job, owner = JobID.random(), WorkerID.random()
    outer = ObjectID.for_task(TaskID.for_put(job, owner, 0))
    return outer, owner, ("put-owner.invalid", 20001)


def _owned(owner, route):
    return ObjectRef(ObjectID.for_task(TaskID.random()), owner, route)


class _Counted:
    def __init__(self, calls):
        self.calls = calls

    def __reduce__(self):
        self.calls.append("serialized")
        return int, (7,)


class _TemporaryReference:
    def __init__(self, owner, route, observers):
        self.owner, self.route, self.observers = owner, route, observers

    def __reduce__(self):
        ref = _owned(self.owner, self.route)
        self.observers.append(weakref.ref(ref))
        return list, ((ref,),)


class _CannotSerialize:
    def __reduce__(self):
        raise ValueError("later element failed")


def test_single_value_serializes_once_and_preserves_python_container_cycle():
    outer, owner, route = _identity()
    calls = []
    value = [_Counted(calls)]
    value.append(value)
    prepared = discover_put(value, outer, owner, route, 4096)
    assert calls == ["serialized"]
    assert prepared.sources == prepared.manifest.transfers == prepared.manifest.edges == ()
    assert prepared.manifest.object_id == outer
    assert prepared.manifest.owner_worker_id == owner
    assert prepared.manifest.tier is protocol.ResultStorage.INLINE
    assert prepared.manifest.size_bytes == len(prepared.payload) <= 4096
    assert prepared.manifest.checksum == hashlib.sha256(prepared.payload).hexdigest()
    decoded = cloudpickle.loads(prepared.payload)
    assert decoded[0] == 7 and decoded[1] is decoded
    # Boundary equality is INLINE; this separate put is just above its budget.
    another = ObjectID.for_task(TaskID.random())
    raw = b"small"
    byte_count = len(cloudpickle.dumps(raw))
    assert discover_put(raw, another, owner, route, byte_count).manifest.tier is protocol.ResultStorage.INLINE
    assert discover_put(raw, another, owner, route, byte_count - 1).manifest.tier is protocol.ResultStorage.OBJECT_STORE


def test_python_aliases_share_one_transfer_but_distinct_handles_keep_their_holds():
    outer, owner, route = _identity()
    first = _owned(owner, route)
    second = ObjectRef(first.object_id, owner, route)
    assert first == second and first is not second
    prepared = discover_put([first, first, second], outer, owner, route, 4096)
    assert len(prepared.sources) == len(prepared.manifest.transfers) == 2
    assert prepared.sources[0] is first and prepared.sources[1] is second
    left, right = prepared.manifest.transfers
    assert left.contained_object_id == right.contained_object_id == first.object_id
    assert left.final_hold != right.final_hold
    for transfer in (left, right):
        assert transfer.provisional_hold.container_object_id == outer
        assert transfer.final_hold.container_owner_worker_id == owner
        assert transfer.provisional_hold.transfer_token == "provisional:" + transfer.final_hold.transfer_token
        assert transfer.provisional_hold != transfer.final_hold
    imported = []

    def restore(*identity):
        imported.append(identity)
        return object()

    with importing_references(restore):
        decoded = cloudpickle.loads(prepared.payload)
    assert decoded[0] is decoded[1] and decoded[0] is not decoded[2]
    assert tuple(item[3] for item in imported) == (left.final_hold, right.final_hold)


@pytest.mark.parametrize("source_kind", ("contained", "task"))
def test_local_and_foreign_discovery_retains_complete_source_metadata(source_kind):
    outer, owner, route = _identity()
    own = _owned(owner, route)
    foreign = _owned(WorkerID.random(), ("child-owner.invalid", 20002))
    upstream = TaskID.random()
    original_source = (
        protocol.ContainedTransferSource(ContainedReferenceHold(
            ObjectID.for_task(upstream), WorkerID.random(), "upstream-transfer",
        )) if source_kind == "contained"
        else protocol.TaskHoldSource(protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, owner, upstream, AttemptID(upstream, 0),
        ))
    )
    foreign._borrower_token = "live-source-token"
    foreign._borrow_source = original_source
    prepared = discover_put({"own": own, "foreign": foreign}, outer, owner, route, 0)
    assert prepared.manifest.tier is protocol.ResultStorage.OBJECT_STORE
    assert prepared.sources[0] is own and prepared.sources[1] is foreign
    local_transfer, foreign_transfer = prepared.manifest.transfers
    assert local_transfer.source == OwnedContainedSource(owner)
    assert local_transfer.contained_owner_address == route
    assert foreign_transfer.source == BorrowedContainedSource(owner, foreign.borrower_token, original_source)
    assert foreign_transfer.contained_owner_address == foreign.owner_address
    assert foreign_transfer.source.original_source is not original_source
    assert pickle.loads(pickle.dumps(prepared.manifest)) == prepared.manifest
    with pytest.raises(TypeError, match="local custody"):
        cloudpickle.dumps(prepared)


def test_late_serialization_failure_has_no_effect_and_restores_outer_export_scope():
    outer, owner, route = _identity()
    observers = []
    prepared = discover_put(_TemporaryReference(owner, route, observers), outer, owner, route, 4096)
    assert len(observers) == 1 and observers[0]() is prepared.sources[0]
    del prepared
    assert observers[0]() is None
    observers.clear()

    def unexpected_export(_reference):
        pytest.fail("nested put discovery leaked its exporter")

    with exporting_references(unexpected_export):
        with pytest.raises(ValueError, match="later element failed"):
            discover_put(
                [_TemporaryReference(owner, route, observers), _CannotSerialize()],
                outer, owner, route, 4096,
            )
        assert current_exporter() is unexpected_export
    assert current_exporter() is None
    assert len(observers) == 1 and observers[0]() is None


def test_manifest_deep_copy_and_revalidation_reject_nested_identity_tampering():
    outer, owner, route = _identity()
    child = _owned(owner, route)
    prepared = discover_put([child], outer, owner, route, 4096)
    transfer = prepared.manifest.transfers[0]
    assert transfer.contained_object_id is not child.object_id
    assert transfer.contained_object_id.task_id is not child.object_id.task_id
    assert transfer.final_hold.container_owner_worker_id is not owner
    with pytest.raises(ValueError, match="payload disagrees"):
        replace(prepared, payload=prepared.payload + b"x")
    # Shallow replacement must re-enter validation all the way to nested IDs.
    corrupted = replace(prepared.manifest)
    object.__setattr__(corrupted.transfers[0].contained_object_id.task_id, "value", b"bad")
    with pytest.raises(ValueError):
        replace(corrupted)
    assert replace(prepared.manifest) == prepared.manifest
    with pytest.raises(ValueError, match="tokens must be unique"):
        PutManifest(
            outer, owner, prepared.manifest.tier, len(prepared.payload),
            prepared.manifest.checksum, (transfer, transfer),
        )
