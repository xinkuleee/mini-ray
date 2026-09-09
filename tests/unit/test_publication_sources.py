"""Pure shared-capability extraction and current wire identity.

Each case has one transfer or one slot and tiny bytes. No Core, Node, service,
thread, process, socket, timer or wait is created. Old module aliases and unbound string holds are retired; current wire
identity and exact typed-source validation remain.
"""

from dataclasses import replace
import builtins
import hashlib
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time

import pytest

from miniray import output_discovery, output_protocol, output_publication, protocol, ref_transfer
from miniray import publication_sources as shared
from miniray.contained_edges import ContainedReferenceEdge, ContainedReferenceHold
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("source capability contract attempted runtime infrastructure")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait"),
                         (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr("miniray.core.CoreWorker.__init__", forbidden)
    monkeypatch.setattr("miniray.transport.TCPServer.__init__", forbidden)


def _id(kind, byte):
    return kind(bytes((byte,)) * 16)


def _transfer(kind):
    outer = ObjectID(_id(TaskID, 0x11), 3)
    child = ObjectID(_id(TaskID, 0x22), 1)
    executor, owner = _id(WorkerID, 0x33), _id(WorkerID, 0x44)
    child_owner = executor if kind == "owned" else _id(WorkerID, 0x55)
    if kind == "owned":
        source = shared.OwnedContainedSource(executor)
    else:
        if kind == "task":
            task = _id(TaskID, 0x66)
            original = protocol.TaskHoldSource(protocol.TaskReferenceHold(
                protocol.TaskReferenceHoldKind.RETAINED, _id(WorkerID, 0x77),
                task, AttemptID(task, 2),
            ))
        elif kind == "typed-contained":
            original = protocol.ContainedTransferSource(ContainedReferenceHold(
                ObjectID(_id(TaskID, 0x88), 2), _id(WorkerID, 0x99), "upstream-pin",
            ))
        else:
            raise AssertionError("unsupported source fixture")
        source = shared.BorrowedContainedSource(executor, "live-borrower", original)
    return shared.PreparedContainedTransfer(
        child, child_owner, ("127.0.0.1", 32001), source,
        ContainedReferenceHold(outer, executor, "publication-pin"),
        ContainedReferenceHold(outer, owner, "publication-pin"),
    )


def _incarnation():
    return shared.PublicationNodeIncarnation(_id(NodeID, 0xAA), 12345, 7)




def test_incarnation_has_one_neutral_type_with_unchanged_death_projection():
    assert output_publication.OutputPublicationNodeIncarnation is shared.PublicationNodeIncarnation
    node = _incarnation()
    death = protocol.NodeDeathRecord(
        "shared-node-exit", node.node_id, node.node_pid, node.registration_epoch,
        8, -9, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed Node exit",
    )
    assert shared.PublicationNodeIncarnation.from_death(death) == node
    assert replace(node) == node and hash(replace(node)) == hash(node)
    with pytest.raises(TypeError, match="NodeDeathRecord"):
        shared.PublicationNodeIncarnation.from_death(object())


@pytest.mark.parametrize("kind,expected", (
    ("owned", "968169808988c5cc2ff03f6f1f5f49baf3ed729f444570fbc8087d24c2a1a122"),
    ("task", "8270910ba04a1aab2774e021f245ac19d9abafc840fc41ffd31f825f9b0e521c"),
    ("typed-contained", "ad5916f44d98f9e4b8dcdc51f9b685c1e4260890240d563c0f2b96368cb339b9"),
))
def test_source_fingerprint_preserves_v1_framing_golden(kind, expected):
    transfer = _transfer(kind)
    fingerprint = shared.prepared_contained_transfer_fingerprint(transfer)
    assert fingerprint.hex() == expected
    assert len(fingerprint) == 32
    assert transfer.edge == ContainedReferenceEdge(
        transfer.final_hold.container_object_id, transfer.contained_object_id,
        transfer.contained_owner_worker_id, transfer.contained_owner_address,
        transfer.final_hold.transfer_token,
    )
    if kind != "owned":
        assert transfer.source.owner_table_token == (_id(WorkerID, 0x33), "live-borrower")


@pytest.mark.parametrize("message_type", (protocol.PrepareStoredContainedPin, protocol.PromoteStoredContainedPin))
def test_shared_pin_validation_does_not_import_legacy_authorities(monkeypatch, message_type):
    transfer = _transfer("task")
    original_import = builtins.__import__

    def imported(name, *args, **kwargs):
        assert name.rsplit(".", 1)[-1] not in (
            "stored_publication", "stored_node_loss", "inline_contained_publication",
        ), "source validation loaded a legacy publication authority"
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", imported)
    rebuilt = replace(transfer, source=replace(transfer.source))
    request = message_type(rebuilt, rebuilt.contained_owner_worker_id)
    assert type(request.transfer) is shared.PreparedContainedTransfer
    assert shared.prepared_contained_transfer_fingerprint(rebuilt) == (
        shared.prepared_contained_transfer_fingerprint(transfer)
    )


@pytest.mark.parametrize("kind", ("owned", "task", "typed-contained"))
@pytest.mark.parametrize("version", (0, pickle.HIGHEST_PROTOCOL))
def test_shared_values_and_pin_messages_pickle_with_exact_identity(kind, version):
    transfer = _transfer(kind)
    for value in (transfer.source, transfer, _incarnation()):
        encoded = pickle.dumps(value, protocol=version)
        assert b"miniray.publication_sources" in encoded
        assert b"miniray.stored_publication" not in encoded
        restored = pickle.loads(encoded)
        assert type(restored) is type(value) and restored == value
        assert hash(restored) == hash(value)
    for message_type in (protocol.PrepareStoredContainedPin, protocol.PromoteStoredContainedPin):
        request = message_type(transfer, transfer.contained_owner_worker_id)
        restored = pickle.loads(pickle.dumps(request, protocol=version))
        assert type(restored) is message_type and restored == request
        assert type(restored.transfer) is shared.PreparedContainedTransfer
        assert shared.prepared_contained_transfer_fingerprint(restored.transfer) == (
            shared.prepared_contained_transfer_fingerprint(transfer)
        )




@pytest.mark.parametrize("value", (0, -1, True, 1.5))
def test_neutral_incarnation_keeps_positive_integer_validation(value):
    node = _incarnation()
    with pytest.raises(ValueError, match="positive integer"):
        replace(node, node_pid=value)
    with pytest.raises(ValueError, match="positive integer"):
        replace(node, registration_epoch=value)


def test_source_and_transfer_validation_still_reject_inexact_custody():
    transfer = _transfer("task")
    with pytest.raises(TypeError, match="contained or task-hold"):
        replace(transfer.source, original_source="unbound")
    with pytest.raises(ValueError, match="non-empty"):
        replace(transfer.source, borrower_token="")
    with pytest.raises(ValueError, match="bound"):
        replace(transfer, contained_owner_address=("127.0.0.1", True))
    with pytest.raises(ValueError, match="share outer and token"):
        replace(transfer, final_hold=replace(transfer.final_hold, transfer_token="other"))
    with pytest.raises(ValueError, match="custody change"):
        replace(transfer, final_hold=replace(
            transfer.final_hold, container_owner_worker_id=transfer.provisional_hold.container_owner_worker_id,
        ))
    with pytest.raises(ValueError, match="owned source"):
        replace(_transfer("owned"), contained_owner_worker_id=_id(WorkerID, 0xFF))


@pytest.mark.parametrize("message_type", (protocol.PrepareStoredContainedPin, protocol.PromoteStoredContainedPin))
def test_pin_wire_roundtrip_still_revalidates_corrupted_source(message_type):
    transfer = _transfer("task")
    request = message_type(transfer, transfer.contained_owner_worker_id)
    object.__setattr__(request.transfer.source, "borrower_token", "")
    with pytest.raises(ProtocolError, match="transfer is invalid"):
        pickle.loads(pickle.dumps(request))


def test_output_wire_uses_shared_leaf_and_preserves_deep_manifest_validation():
    transfer = _transfer("task")
    task = transfer.final_hold.container_object_id.task_id
    outer = ObjectID(task, 0)
    transfer = replace(
        transfer, provisional_hold=replace(transfer.provisional_hold, container_object_id=outer),
        final_hold=replace(transfer.final_hold, container_object_id=outer),
    )
    execution = TaskExecutionKey(TaskOutputManifest.for_task(task, 1), AttemptID(task, 0))
    header = output_publication.OutputPublicationHeader(
        output_publication.OutputPublicationID(_id(LeaseID, 0x12), execution),
        _id(JobID, 0x13), transfer.source.borrower_worker_id,
        transfer.final_hold.container_owner_worker_id, _incarnation(),
    )
    payload = b"tiny"
    slot = output_publication.OutputSlotManifest(
        outer, protocol.ResultStorage.INLINE, len(payload), hashlib.sha256(payload).hexdigest(), (transfer,),
    )
    manifest = output_publication.OutputPublicationManifest.create(header, (slot,))
    request = output_protocol.PrepareOutputPublication(manifest, (payload,))
    assert pickle.loads(pickle.dumps(request)) == request
    assert type(request.manifest.header.node_incarnation) is shared.PublicationNodeIncarnation
    assert type(request.manifest.slots[0].transfers[0]) is shared.PreparedContainedTransfer
    object.__setattr__(request.manifest.slots[0].transfers[0].source, "borrower_token", "")
    with pytest.raises((TypeError, ValueError, ProtocolError), match="borrower_token"):
        pickle.loads(pickle.dumps(request))


def test_current_modules_share_leaf_identity_and_legacy_hold_is_rejected():
    assert output_discovery.PreparedContainedTransfer is shared.PreparedContainedTransfer
    assert output_publication.PreparedContainedTransfer is shared.PreparedContainedTransfer
    assert ref_transfer.OwnedContainedSource is shared.OwnedContainedSource
    assert ref_transfer.BorrowedContainedSource is shared.BorrowedContainedSource
    with pytest.raises(ProtocolError):
        protocol.ContainedTransferSource("unbound-legacy-pin")
