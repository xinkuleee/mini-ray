"""Pure single-output identities; metadata validation is not publication authority."""

from __future__ import annotations

import hashlib
import pickle
from dataclasses import fields, is_dataclass, replace
from enum import Enum

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.errors import InvalidIDError, ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationConflictError,
    OutputPublicationEnvelope, OutputPublicationError, OutputPublicationHeader,
    OutputPublicationID, OutputPublicationManifest, OutputPublicationNodeIncarnation,
    OutputSlotManifest,
)
from miniray.publication_sources import (
    BorrowedContainedSource, OwnedContainedSource, PreparedContainedTransfer,
    PublicationNodeIncarnation,
)
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest


pytestmark = pytest.mark.unit


def _id(kind, byte: int):
    return kind(bytes((byte,)) * 16)


class _Fixture:
    def __init__(self, *, refs: bool = True, stored: bool = False) -> None:
        self.job = _id(JobID, 1)
        self.task = _id(TaskID, 2)
        self.attempt = AttemptID(self.task, 3)
        self.lease = _id(LeaseID, 4)
        self.executor = _id(WorkerID, 5)
        self.owner = _id(WorkerID, 6)
        self.foreign_owner = _id(WorkerID, 7)
        self.node = _id(NodeID, 8)
        self.child = ObjectID.for_task(_id(TaskID, 9))
        self.borrowed_child = ObjectID.for_task(_id(TaskID, 10))
        self.full = TaskOutputManifest.for_task(self.task, 1)
        self.execution = TaskExecutionKey(self.full, self.attempt)
        self.publication_id = OutputPublicationID(self.lease, self.execution)
        self.header = OutputPublicationHeader(
            self.publication_id, self.job, self.executor, self.owner,
            OutputPublicationNodeIncarnation(self.node, 1701, 2),
        )
        self.payloads = (b"stored-result" if stored else b"inline-result",)
        self.original_source = protocol.TaskHoldSource(protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, self.executor,
            _id(TaskID, 11), AttemptID(_id(TaskID, 11), 2),
        ))
        values = []
        for index, object_id in enumerate(self.publication_id.output_ids):
            transfers = (
                (self.owned(object_id), self.borrowed(object_id)) if refs else ()
            )
            payload = self.payloads[index]
            values.append(OutputSlotManifest(
                object_id, protocol.ResultStorage.OBJECT_STORE if stored
                else protocol.ResultStorage.INLINE, len(payload),
                hashlib.sha256(payload).hexdigest(), transfers,
            ))
        self.slots = tuple(values)
        self.manifest = OutputPublicationManifest.create(self.header, self.slots)
        self.witness = OutputPublicationCompleteWitness.for_manifest(self.manifest)
        self.results = tuple(
            protocol.ResultDescriptor(
                slot.object_id, slot.tier, slot.size_bytes, self.owner, self.node,
                slot.checksum, self.payloads[index] if slot.tier is protocol.ResultStorage.INLINE else None,
            ) for index, slot in enumerate(self.slots)
        )
        self.envelope = OutputPublicationEnvelope(self.manifest, self.witness, self.results)

    def owned(self, outer: ObjectID) -> PreparedContainedTransfer:
        return PreparedContainedTransfer(
            self.child, self.executor, ("127.0.0.1", 30101),
            OwnedContainedSource(self.executor),
            ContainedReferenceHold(outer, self.executor, "same-owned-token"),
            ContainedReferenceHold(outer, self.owner, "same-owned-token"),
        )

    def borrowed(self, outer: ObjectID) -> PreparedContainedTransfer:
        return PreparedContainedTransfer(
            self.borrowed_child, self.foreign_owner, ("127.0.0.1", 30102),
            BorrowedContainedSource(self.executor, "borrow-token", self.original_source),
            ContainedReferenceHold(outer, self.executor, "same-borrowed-token"),
            ContainedReferenceHold(outer, self.owner, "same-borrowed-token"),
        )


def _assert_metadata(value: object) -> None:
    if type(value) in (JobID, TaskID, LeaseID, NodeID, WorkerID):
        assert type(value.value) is bytes and len(value.value) == 16
        return
    assert not isinstance(value, (bytes, bytearray, memoryview, protocol.ResultDescriptor))
    if value is None or isinstance(value, (str, int, Enum)):
        return
    if is_dataclass(value) and not isinstance(value, type):
        for item in fields(value):
            _assert_metadata(getattr(value, item.name))
    elif isinstance(value, tuple):
        for item in value:
            _assert_metadata(item)
    else:
        raise AssertionError(type(value).__name__)


@pytest.mark.parametrize("stored", (False, True))
def test_single_output_metadata_and_data_plane_keep_both_storage_tiers(stored):
    fixture = _Fixture(stored=stored)
    manifest = fixture.manifest
    assert manifest.execution == fixture.execution
    assert manifest.slots == fixture.slots
    assert manifest.ordered_edges == fixture.slots[0].edges
    assert len(manifest.ordered_edges) == 2
    assert fixture.envelope.results == fixture.results
    assert fixture.envelope.results[0].inline_data == (None if stored else fixture.payloads[0])
    assert fixture.envelope.publication_id == fixture.publication_id
    assert OutputPublicationNodeIncarnation is PublicationNodeIncarnation
    assert not hasattr(manifest, "to_graph_manifest")
    for metadata in (fixture.publication_id, fixture.header, manifest, fixture.witness):
        _assert_metadata(metadata)


def test_two_distinct_holds_for_one_child_remain_separate_metadata():
    fixture = _Fixture()
    first = fixture.slots[0].transfers[0]
    second = replace(first,
        provisional_hold=replace(first.provisional_hold, transfer_token="second-hold"),
        final_hold=replace(first.final_hold, transfer_token="second-hold"))
    slot = replace(fixture.slots[0], transfers=(first, second))
    manifest = OutputPublicationManifest.create(fixture.header, (slot,))
    assert first.contained_object_id == second.contained_object_id
    assert first.final_hold != second.final_hold
    assert manifest.ordered_edges == (first.edge, second.edge)
    # Distinct metadata is not evidence of child Release or physical GC.
    assert pickle.loads(pickle.dumps(manifest)) == manifest


def test_no_refs_is_a_real_nonempty_output_manifest():
    fixture = _Fixture(refs=False)
    assert fixture.manifest.ordered_edges == ()
    assert len(fixture.manifest.slots) == 1
    assert len(fixture.envelope.results) == 1
    with pytest.raises(OutputPublicationConflictError, match="single execution"):
        OutputPublicationManifest.create(fixture.header, ())


def test_reference_free_and_contained_outputs_share_one_protocol_type():
    plain, contained = _Fixture(refs=False), _Fixture(refs=True)
    assert type(plain.manifest) is type(contained.manifest) is OutputPublicationManifest
    assert type(plain.envelope) is type(contained.envelope) is OutputPublicationEnvelope
    assert plain.manifest.ordered_edges == () and len(contained.manifest.ordered_edges) == 2


def test_identity_requires_only_canonical_index_zero():
    fixture = _Fixture()
    publication = fixture.publication_id
    assert tuple(item.return_index for item in publication.output_ids) == (0,)
    assert publication.full_output_ids == fixture.full.output_ids
    assert tuple(slot.object_id for slot in fixture.manifest.slots) == publication.output_ids
    assert tuple(item.object_id for item in fixture.envelope.results) == publication.output_ids
    for slots in ((), fixture.slots + fixture.slots):
        with pytest.raises(OutputPublicationConflictError, match="single execution"):
            OutputPublicationManifest.create(fixture.header, slots)
    renumbered = replace(fixture.slots[0], object_id=ObjectID(fixture.task, 1), transfers=())
    with pytest.raises(OutputPublicationConflictError, match="single execution"):
        OutputPublicationManifest.create(fixture.header, (renumbered,))


def test_typed_top_level_boundaries_reject_wrong_identity_and_payload_objects():
    fixture = _Fixture()
    for lease, execution in (
        (fixture.node, fixture.execution), (fixture.lease, fixture.full),
    ):
        with pytest.raises(TypeError):
            OutputPublicationID(lease, execution)
    for field, value in (
        ("publication_id", fixture.execution), ("job_id", fixture.task),
        ("executor_worker_id", fixture.node), ("owner_worker_id", fixture.job),
        ("node_incarnation", fixture.node),
    ):
        with pytest.raises(TypeError):
            replace(fixture.header, **{field: value})
    with pytest.raises(TypeError, match="OutputSlotManifest"):
        OutputPublicationManifest.create(fixture.header, fixture.results)
    with pytest.raises(TypeError, match="slots"):
        OutputPublicationManifest.create(fixture.header, b"result-bytes")
    with pytest.raises(TypeError, match="OutputPublicationManifest"):
        OutputPublicationCompleteWitness.for_manifest(fixture.envelope)
    with pytest.raises(TypeError, match="OutputPublicationCompleteWitness"):
        OutputPublicationEnvelope(fixture.manifest, fixture.header, fixture.results)
    with pytest.raises(TypeError, match="results"):
        OutputPublicationEnvelope(fixture.manifest, fixture.witness, b"result-bytes")


def test_concrete_wire_types_reject_subclass_hidden_payloads():
    fixture = _Fixture()

    class PayloadWorkerID(WorkerID):
        pass

    hidden = PayloadWorkerID(bytes(fixture.owner))
    object.__setattr__(hidden, "payload", b"must-not-enter-control-plane")
    with pytest.raises(TypeError, match="WorkerID"):
        replace(fixture.header, owner_worker_id=hidden)


def test_identity_digest_binds_lease_attempt_and_handoff_domain():
    fixture = _Fixture()
    variants = (
        replace(fixture.publication_id, lease_id=_id(LeaseID, 40)),
        replace(fixture.publication_id, execution=fixture.execution.for_attempt(fixture.attempt.next())),
    )
    assert len({fixture.publication_id.transaction_id, *(item.transaction_id for item in variants)}) == 3
    assert fixture.publication_id.transaction_id.startswith("output-publication-handoff:")
    for publication_id in variants:
        manifest = OutputPublicationManifest.create(replace(fixture.header, publication_id=publication_id), fixture.slots)
        assert manifest.manifest_digest != fixture.manifest.manifest_digest
    other_task = _id(TaskID, 41)
    other = OutputPublicationID(fixture.lease, TaskExecutionKey(
        TaskOutputManifest.for_task(other_task, 1), AttemptID(other_task, fixture.attempt.attempt_number)))
    assert other.transaction_id != fixture.publication_id.transaction_id


def test_manifest_digest_binds_header_node_incarnation_tier_and_each_source_field():
    fixture = _Fixture()
    plain = _Fixture(refs=False)
    for header in (
        replace(plain.header, job_id=_id(JobID, 21)),
        replace(plain.header, executor_worker_id=_id(WorkerID, 22)),
        replace(plain.header, owner_worker_id=_id(WorkerID, 23)),
        replace(plain.header, node_incarnation=replace(plain.header.node_incarnation, node_id=_id(NodeID, 24))),
        replace(plain.header, node_incarnation=replace(plain.header.node_incarnation, node_pid=1702)),
        replace(plain.header, node_incarnation=replace(plain.header.node_incarnation, registration_epoch=3)),
    ):
        changed = OutputPublicationManifest.create(header, plain.slots)
        assert changed.manifest_digest != plain.manifest.manifest_digest
        with pytest.raises(OutputPublicationConflictError, match="manifest_digest"):
            replace(plain.manifest, header=header)
    first = fixture.slots[0]
    transfer = first.transfers[1]
    source = transfer.source
    original_hold = source.original_source.hold
    transfer_variants = (
        replace(transfer, contained_owner_address=("127.0.0.1", 30103)),
        replace(transfer, source=replace(source, borrower_token="changed-borrower")),
        replace(transfer, source=replace(source, original_source=protocol.TaskHoldSource(replace(
            original_hold, origin_attempt_id=original_hold.origin_attempt_id.next()
        )))),
        replace(transfer, source=replace(source, original_source=protocol.TaskHoldSource(replace(
            original_hold, kind=protocol.TaskReferenceHoldKind.SUBMITTED
        )))),
        replace(transfer, provisional_hold=replace(transfer.provisional_hold, transfer_token="new-token"),
                final_hold=replace(transfer.final_hold, transfer_token="new-token")),
    )
    slot_variants = (
        replace(first, tier=protocol.ResultStorage.OBJECT_STORE),
        replace(first, size_bytes=first.size_bytes + 1),
        replace(first, checksum="ab" * 32),
        replace(first, transfers=first.transfers[::-1]),
        *(replace(first, transfers=(first.transfers[0], item)) for item in transfer_variants),
    )
    for slot in slot_variants:
        changed = OutputPublicationManifest.create(fixture.header, (slot,))
        assert changed.manifest_digest != fixture.manifest.manifest_digest
        with pytest.raises(OutputPublicationConflictError, match="manifest_digest"):
            replace(fixture.manifest, slots=(slot,))


def test_borrowed_contained_source_fingerprint_binds_complete_typed_hold():
    fixture = _Fixture()
    transfer = fixture.slots[0].transfers[1]
    source = transfer.source
    originals = (
        protocol.ContainedTransferSource(ContainedReferenceHold(
            ObjectID.for_task(_id(TaskID, 12)), fixture.foreign_owner, "source-pin"
        )),
        protocol.ContainedTransferSource(ContainedReferenceHold(
            ObjectID.for_task(_id(TaskID, 13)), fixture.foreign_owner, "source-pin"
        )),
        protocol.ContainedTransferSource(ContainedReferenceHold(
            ObjectID.for_task(_id(TaskID, 12)), fixture.foreign_owner, "another-source-pin"
        )),
    )
    digests = []
    for original in originals:
        changed_transfer = replace(transfer, source=replace(source, original_source=original))
        slot = replace(fixture.slots[0], transfers=(changed_transfer,))
        manifest = OutputPublicationManifest.create(fixture.header, (slot,))
        _assert_metadata(manifest)
        digests.append(manifest.manifest_digest)
    assert len(set(digests)) == len(originals)
    with pytest.raises(ProtocolError):
        protocol.ContainedTransferSource("source-pin")
    # A constructor rejection is separate from revalidating a forged frozen
    # source received inside an otherwise complete publication value.
    forged = replace(transfer, source=replace(source))
    object.__setattr__(forged.source.original_source, "hold", "source-pin")
    with pytest.raises((TypeError, ValueError, ProtocolError)):
        replace(fixture.slots[0], transfers=(forged,))


@pytest.mark.parametrize("field,value", (
    ("tier", "INLINE"), ("size_bytes", True), ("size_bytes", -1),
    ("size_bytes", 1 << 64), ("checksum", "g" * 64),
    ("checksum", " " * 64), ("checksum", b"ab" * 32),
    ("transfers", b"not-transfers"),
))
def test_slot_rejects_malformed_leaf_values(field, value):
    with pytest.raises((TypeError, ValueError)):
        replace(_Fixture().slots[0], **{field: value})


def test_slot_rejects_wrong_container_and_conflicting_duplicate_hold():
    fixture = _Fixture()
    first = fixture.slots[0]
    other_outer = ObjectID.for_task(_id(TaskID, 44))
    wrong = replace(first.transfers[0],
        provisional_hold=replace(first.transfers[0].provisional_hold, container_object_id=other_outer),
        final_hold=replace(first.transfers[0].final_hold, container_object_id=other_outer))
    with pytest.raises(OutputPublicationConflictError, match="belong to its output slot"):
        replace(first, transfers=(wrong,))
    with pytest.raises(OutputPublicationConflictError, match="multiple transfer slots"):
        replace(first, transfers=(first.transfers[0], first.transfers[0]))
    same_hold_different_source = replace(
        first.transfers[1], source=replace(first.transfers[1].source, borrower_token="other-proof")
    )
    with pytest.raises(OutputPublicationConflictError, match="multiple transfer slots"):
        replace(first, transfers=(first.transfers[1], same_hold_different_source))


def test_header_rejects_source_custody_drift_and_shared_child_owner_conflict():
    fixture = _Fixture()
    for header in (
        replace(fixture.header, executor_worker_id=_id(WorkerID, 21)),
        replace(fixture.header, owner_worker_id=_id(WorkerID, 22)),
    ):
        with pytest.raises(OutputPublicationConflictError, match="custody"):
            OutputPublicationManifest.create(header, fixture.slots)
    borrowed = fixture.slots[0].transfers[1]
    wrong_source = replace(borrowed, source=replace(borrowed.source, borrower_worker_id=_id(WorkerID, 23)))
    wrong_slot = replace(fixture.slots[0], transfers=(wrong_source,))
    with pytest.raises(OutputPublicationConflictError, match="source.*executor"):
        OutputPublicationManifest.create(fixture.header, (wrong_slot,))
    changed_owner = replace(borrowed, contained_owner_worker_id=_id(WorkerID, 24),
        provisional_hold=replace(borrowed.provisional_hold, transfer_token="other-hold"),
        final_hold=replace(borrowed.final_hold, transfer_token="other-hold"))
    other_slot = replace(fixture.slots[0], transfers=(borrowed, changed_owner))
    with pytest.raises(OutputPublicationConflictError, match="conflicting owners"):
        OutputPublicationManifest.create(fixture.header, (other_slot,))


def test_complete_witness_accepts_only_success_and_exact_manifest_binding():
    fixture = _Fixture()
    for status in (protocol.TaskReplyStatus.SYSTEM_ERROR, protocol.TaskReplyStatus.CANCELLED):
        with pytest.raises(OutputPublicationError, match="SUCCEEDED"):
            replace(fixture.witness, status=status)
    with pytest.raises(TypeError, match="TaskReplyStatus"):
        replace(fixture.witness, status="SUCCEEDED")
    for witness in (
        replace(fixture.witness, manifest_digest="ab" * 32),
        replace(fixture.witness, publication_id=replace(fixture.publication_id, lease_id=_id(LeaseID, 31))),
    ):
        with pytest.raises(OutputPublicationConflictError, match="Complete witness"):
            OutputPublicationEnvelope(fixture.manifest, witness, fixture.results)


@pytest.mark.parametrize("case", ("object", "missing", "duplicate", "owner", "node", "size", "checksum", "tier"))
def test_envelope_rejects_result_manifest_drift(case):
    fixture = _Fixture(stored=True)
    first = fixture.results[0]
    results = {
        "object": (replace(first, object_id=ObjectID.for_task(_id(TaskID, 32))),), "missing": (),
        "duplicate": (first, first),
        "owner": (replace(first, owner_worker_id=_id(WorkerID, 31)),),
        "node": (replace(first, node_id=_id(NodeID, 32)),),
        "size": (replace(first, size_bytes=first.size_bytes + 1),),
        "checksum": (replace(first, checksum="ab" * 32),),
        "tier": (replace(first, storage=protocol.ResultStorage.INLINE, inline_data=fixture.payloads[0]),),
    }[case]
    with pytest.raises(OutputPublicationConflictError):
        OutputPublicationEnvelope(fixture.manifest, fixture.witness, results)


@pytest.mark.parametrize("tamper", (
    "id-bytes", "task-id-type", "attempt-number", "attempt-task",
    "empty-manifest", "nonzero-output", "node-id", "node-epoch",
    "transfer-owner", "transfer-port", "hold-id", "hold-owner",
    "source-id", "source-token", "task-source-kind", "task-source-origin",
))
def test_manifest_create_revalidates_deep_tampered_values(tamper):
    fixture = _Fixture()
    header = fixture.header
    transfer = fixture.slots[0].transfers[1]
    if tamper == "id-bytes":
        object.__setattr__(header.publication_id.lease_id, "value", b"short")
    elif tamper == "task-id-type":
        object.__setattr__(header.publication_id.execution.manifest, "task_id", fixture.job)
    elif tamper == "attempt-number":
        object.__setattr__(header.publication_id.execution.attempt_id, "attempt_number", True)
    elif tamper == "attempt-task":
        object.__setattr__(header.publication_id.execution.attempt_id, "task_id", _id(TaskID, 40))
    elif tamper == "empty-manifest":
        object.__setattr__(header.publication_id.execution.manifest, "output_ids", ())
    elif tamper == "nonzero-output":
        object.__setattr__(header.publication_id.execution.manifest, "output_ids", (ObjectID(fixture.task, 1),))
    elif tamper == "node-id":
        object.__setattr__(header.node_incarnation, "node_id", fixture.owner)
    elif tamper == "node-epoch":
        object.__setattr__(header.node_incarnation, "registration_epoch", 0)
    elif tamper == "transfer-owner":
        object.__setattr__(transfer, "contained_owner_worker_id", fixture.node)
    elif tamper == "transfer-port":
        object.__setattr__(transfer, "contained_owner_address", ("127.0.0.1", True))
    elif tamper == "hold-id":
        object.__setattr__(transfer.final_hold.container_object_id, "return_index", True)
    elif tamper == "hold-owner":
        object.__setattr__(transfer.final_hold, "container_owner_worker_id", fixture.node)
    elif tamper == "source-id":
        object.__setattr__(transfer.source, "borrower_worker_id", fixture.node)
    elif tamper == "source-token":
        object.__setattr__(transfer.source, "borrower_token", b"payload-token")
    elif tamper == "task-source-kind":
        object.__setattr__(transfer.source.original_source.hold, "kind", "RETAINED")
    else:
        object.__setattr__(transfer.source.original_source.hold.origin_attempt_id, "task_id", _id(TaskID, 41))
    with pytest.raises((TypeError, ValueError, ProtocolError, InvalidIDError)):
        OutputPublicationManifest.create(header, fixture.slots)


def test_envelope_revalidates_descriptor_payload_checksum_and_nested_ids():
    for attribute, value in (
        ("inline_data", b"wrong-result"), ("size_bytes", True),
        ("storage", "INLINE"), ("owner_worker_id", _id(NodeID, 1)),
    ):
        fixture = _Fixture()
        object.__setattr__(fixture.results[0], attribute, value)
        with pytest.raises((TypeError, ValueError, ProtocolError)):
            OutputPublicationEnvelope(fixture.manifest, fixture.witness, fixture.results)


def test_complete_witness_and_manifest_copy_revalidate_tampered_manifest():
    for construct in (
        OutputPublicationCompleteWitness.for_manifest,
        lambda value: replace(value),
    ):
        fixture = _Fixture()
        object.__setattr__(fixture.manifest.slots[0], "checksum", "ff" * 32)
        with pytest.raises(OutputPublicationConflictError, match="manifest_digest"):
            construct(fixture.manifest)
    fixture = _Fixture()
    object.__setattr__(fixture.witness, "status", "SUCCEEDED")
    with pytest.raises(TypeError, match="TaskReplyStatus"):
        OutputPublicationEnvelope(fixture.manifest, fixture.witness, fixture.results)


def test_constructed_manifest_detaches_mutable_aliases_from_input_values():
    fixture = _Fixture()
    original_digest = fixture.manifest.manifest_digest
    source = fixture.slots[0].transfers[1].source
    object.__setattr__(source, "borrower_token", "changed-after-construction")
    object.__setattr__(fixture.header.node_incarnation, "node_pid", 9999)
    assert fixture.manifest.header.node_incarnation.node_pid == 1701
    assert fixture.manifest.slots[0].transfers[1].source.borrower_token == "borrow-token"
    assert replace(fixture.manifest).manifest_digest == original_digest


def test_pickle_roundtrip_reenters_validation_without_using_pickle_as_digest():
    fixture = _Fixture()
    assert pickle.loads(pickle.dumps(fixture.envelope)) == fixture.envelope
    assert pickle.loads(pickle.dumps(fixture.manifest)) == fixture.manifest
    object.__setattr__(fixture.envelope.results[0], "inline_data", b"changed-bytes")
    with pytest.raises(ProtocolError, match="inline result"):
        pickle.loads(pickle.dumps(fixture.envelope))


def test_rebuilt_values_have_deterministic_digest_and_canonical_hex():
    first, second = _Fixture(), _Fixture()
    assert first.manifest == second.manifest
    assert first.manifest.manifest_digest == second.manifest.manifest_digest
    assert first.publication_id.transaction_id == second.publication_id.transaction_id
    slots = tuple(replace(slot, checksum=slot.checksum.upper()) for slot in first.slots)
    rebuilt = OutputPublicationManifest.create(replace(first.header), list(slots))
    assert rebuilt == first.manifest
    assert replace(first.manifest, manifest_digest=first.manifest.manifest_digest.upper()) == first.manifest
    # Known handoff framing uses canonical index zero, not a graph scope.
    framed = hashlib.sha256(b"miniray-output-publication-handoff-v1\0")
    for value in (bytes(first.lease), bytes(first.task), first.attempt.attempt_number.to_bytes(8, "big"),
                  bytes(first.task), (0).to_bytes(8, "big")):
        framed.update(len(value).to_bytes(8, "big"))
        framed.update(value)
    assert first.publication_id.transaction_id == "output-publication-handoff:" + framed.hexdigest()
