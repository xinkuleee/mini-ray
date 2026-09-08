"""Pure Pin/Release wire validation; fixed descriptor identities only.

One source, one requester and at most two copies of one three-byte descriptor
per case. Constructors, replace and pickle alone run: no Node/Core, physical
store, transfer, thread, process, socket, wait or runtime import. These tests
validate DTO identity, not proof that a source really pinned or unpinned bytes.
"""

from dataclasses import fields, replace
import hashlib
import pickle

import pytest

from miniray import protocol
from miniray.ids import AttemptID, NodeID, ObjectID, TaskID, WorkerID


pytestmark = pytest.mark.unit


def _descriptor():
    task = TaskID(b"t" * 16)
    return protocol.ObjectStoreDescriptor(
        ObjectID.for_task(task), WorkerID(b"o" * 16), AttemptID(task, 0),
        NodeID(b"s" * 16), 3, hashlib.sha256(b"abc").hexdigest(),
    )


def _message(kind):
    descriptor = _descriptor()
    requester = NodeID(b"r" * 16)
    if kind == "pin":
        return protocol.PinObjectForTransfer("exact-transfer", descriptor, requester)
    if kind == "pin-reply":
        return protocol.PinObjectForTransferReply("exact-transfer", descriptor, True)
    if kind == "release":
        return protocol.ReleaseObjectPin("exact-transfer", descriptor.object_id, requester)
    assert kind == "release-reply"
    return protocol.ReleaseObjectPinReply("exact-transfer", descriptor.object_id, descriptor.node_id, True, False)


def _corrupt(value, path, replacement):
    parts = path.split(".")
    for part in parts[:-1]:
        value = getattr(value, part)
    object.__setattr__(value, parts[-1], replacement)


@pytest.mark.parametrize("kind", ("pin", "pin-reply", "release", "release-reply"))
def test_wire_roundtrip_preserves_current_schema_and_detaches_nested_ids(kind):
    message = _message(kind)
    rebuilt = replace(message)
    restored = pickle.loads(pickle.dumps(message))
    assert rebuilt == restored == message and rebuilt is not message
    expected_fields = {
        "pin": ("transfer_id", "descriptor", "requester_node_id"),
        "pin-reply": ("transfer_id", "descriptor", "pinned", "error"),
        "release": ("transfer_id", "object_id", "requester_node_id"),
        "release-reply": ("transfer_id", "object_id", "node_id", "accepted", "released", "error"),
    }
    assert tuple(field.name for field in fields(message)) == expected_fields[kind]
    if kind.startswith("pin"):
        assert rebuilt.descriptor is not message.descriptor
        assert rebuilt.descriptor.object_id is not message.descriptor.object_id
        assert rebuilt.descriptor.producer_attempt_id is not message.descriptor.producer_attempt_id
        assert rebuilt.descriptor.owner_worker_id is not message.descriptor.owner_worker_id
        object.__setattr__(message.descriptor, "checksum", "d" * 64)
    else:
        assert rebuilt.object_id is not message.object_id
        assert rebuilt.object_id.task_id is not message.object_id.task_id
        object.__setattr__(message.object_id, "return_index", 7)
    if kind in ("pin", "release"):
        assert rebuilt.requester_node_id is not message.requester_node_id
        object.__setattr__(message.requester_node_id, "value", b"q" * 16)
    if kind == "release-reply":
        assert rebuilt.node_id is not message.node_id
        object.__setattr__(message.node_id, "value", b"q" * 16)
    assert rebuilt == restored and rebuilt != message


@pytest.mark.parametrize(("kind", "path", "replacement"), (
    ("pin", "descriptor.object_id.return_index", False),
    ("pin", "descriptor.owner_worker_id.value", b"short"),
    ("pin", "descriptor.producer_attempt_id.attempt_number", True),
    ("pin", "requester_node_id.value", b"short"),
    ("pin-reply", "descriptor.size_bytes", True),
    ("pin-reply", "descriptor.checksum", "z" * 64),
    ("pin-reply", "descriptor.node_id.value", b"short"),
    ("release", "object_id.task_id.value", b"short"),
    ("release", "requester_node_id.value", b"short"),
    ("release-reply", "object_id.return_index", False),
    ("release-reply", "node_id.value", b"short"),
))
def test_nested_corruption_is_rejected_on_replace_and_wire(kind, path, replacement):
    message = _message(kind)
    _corrupt(message, path, replacement)
    with pytest.raises(protocol.ProtocolError):
        replace(message)
    with pytest.raises(protocol.ProtocolError):
        pickle.loads(pickle.dumps(message))


def test_pin_and_release_constructor_inputs_are_not_retained_by_alias():
    descriptor = _descriptor()
    requester = NodeID(b"r" * 16)
    pin = protocol.PinObjectForTransfer("constructor-input", descriptor, requester)
    pin_reply = protocol.PinObjectForTransferReply(pin.transfer_id, descriptor, True)
    release = protocol.ReleaseObjectPin(pin.transfer_id, descriptor.object_id, requester)
    release_reply = protocol.ReleaseObjectPinReply(pin.transfer_id, descriptor.object_id, descriptor.node_id, True, True)
    saved = tuple(pickle.loads(pickle.dumps(item)) for item in (pin, pin_reply, release, release_reply))
    object.__setattr__(descriptor.object_id.task_id, "value", b"q" * 16)
    object.__setattr__(descriptor.owner_worker_id, "value", b"v" * 16)
    object.__setattr__(descriptor.node_id, "value", b"w" * 16)
    object.__setattr__(requester, "value", b"z" * 16)
    assert (pin, pin_reply, release, release_reply) == saved


def test_reply_flags_keep_idempotent_closed_session_distinct_from_physical_release():
    pin = _message("pin-reply")
    release = _message("release-reply")
    closed = replace(release, accepted=True, released=False)
    removed = replace(release, accepted=True, released=True)
    rejected = replace(release, accepted=False, released=False, error="identity conflict")
    assert closed.accepted and not closed.released
    assert removed.accepted and removed.released
    assert not rejected.accepted and not rejected.released
    for message in (closed, removed, rejected, replace(pin, pinned=False, error="closed transfer")):
        assert pickle.loads(pickle.dumps(message)) == message
    for changes in (dict(accepted=False, released=True, error="conflict"), dict(accepted=False),
                    dict(accepted=True, error="contradiction"), dict(released=1), dict(accepted=1)):
        with pytest.raises(protocol.ProtocolError):
            replace(release, **changes)
    for changes in (dict(pinned=False), dict(pinned=True, error="contradiction"), dict(pinned=1)):
        with pytest.raises(protocol.ProtocolError):
            replace(pin, **changes)


def test_existing_nonempty_transfer_ids_and_zero_length_descriptors_remain_valid():
    descriptor = replace(_descriptor(), size_bytes=0, checksum=hashlib.sha256(b"").hexdigest())
    # IDs are opaque nonempty strings, not UUID syntax; source=requester is
    # also not a wire-level impossibility. Do not silently strengthen either.
    pin = protocol.PinObjectForTransfer(" ", descriptor, descriptor.node_id)
    assert pin.transfer_id == " " and pin.descriptor.size_bytes == 0
    assert pickle.loads(pickle.dumps(pin)) == pin
    for kind in ("pin", "pin-reply", "release", "release-reply"):
        message = _message(kind)
        with pytest.raises(protocol.ProtocolError):
            replace(message, transfer_id="")


def test_missing_fields_disguised_descriptors_and_wrong_wire_arity_are_rejected():
    class DisguisedDescriptor(protocol.ObjectStoreDescriptor):
        pass

    descriptor = _descriptor()
    disguised = DisguisedDescriptor(*(getattr(descriptor, field.name) for field in fields(descriptor)))
    with pytest.raises(protocol.ProtocolError):
        protocol.PinObjectForTransfer("exact-transfer", disguised, NodeID(b"r" * 16))
    for kind in ("pin", "pin-reply"):
        message = _message(kind)
        object.__delattr__(message.descriptor, "owner_worker_id")
        with pytest.raises(protocol.ProtocolError):
            replace(message)
        with pytest.raises(protocol.ProtocolError):
            pickle.loads(pickle.dumps(message))
    for kind in ("pin", "pin-reply", "release", "release-reply"):
        message = _message(kind)
        values = tuple(getattr(message, field.name) for field in fields(message))
        for wrong in (values[:-1], values + (None,)):
            with pytest.raises(protocol.ProtocolError):
                protocol._rebuild_validated_wire_message(type(message), wrong)
