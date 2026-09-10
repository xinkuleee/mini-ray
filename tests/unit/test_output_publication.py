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
    OutputValue,
)
from miniray.publication_sources import (
    BorrowedContainedSource, OwnedContainedSource, PreparedContainedTransfer,
    PublicationNodeIncarnation,
)
from miniray.task_outputs import TaskExecution


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
        self.execution = (TaskExecution(self.attempt))
        self.publication_id = OutputPublicationID(self.lease, self.execution)
        self.header = OutputPublicationHeader(
            self.publication_id, self.job, self.executor, self.owner,
            OutputPublicationNodeIncarnation(self.node, 1701, 2),
        )
        (self.payload) = (b'stored-result' if stored else b'inline-result')
        self.original_source = protocol.TaskHoldSource(protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, self.executor,
            _id(TaskID, 11), AttemptID(_id(TaskID, 11), 2),
        ))
        object_id = (self.publication_id.object_id)
        transfers = (self.owned(object_id), self.borrowed(object_id)) if refs else ()
        (self.value) = (OutputValue(
            protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE,
            len(self.payload), hashlib.sha256(self.payload).hexdigest(), transfers,
        ))
        self.manifest = OutputPublicationManifest.create(self.header, (self.value))
        self.witness = OutputPublicationCompleteWitness.for_manifest(self.manifest)
        (self.result) = (protocol.ResultDescriptor(
            object_id, self.value.tier, self.value.size_bytes, self.owner, self.node,
            self.value.checksum, None if stored else self.payload,
        ))
        self.envelope = OutputPublicationEnvelope(self.manifest, self.witness, (self.result))

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
    assert (manifest.value) == (fixture.value)
    assert (manifest.value.edges) == (fixture.value).edges
    assert len((manifest.value.edges)) == 2
    assert (fixture.envelope.result) == (fixture.result)
    assert (fixture.envelope.result).inline_data == (None if stored else (fixture.payload))
    assert fixture.envelope.publication_id == fixture.publication_id
    assert OutputPublicationNodeIncarnation is PublicationNodeIncarnation
    assert not hasattr(manifest, "to_graph_manifest")
    for metadata in (fixture.publication_id, fixture.header, manifest, fixture.witness):
        _assert_metadata(metadata)


def test_two_distinct_holds_for_one_child_remain_separate_metadata():
    fixture = _Fixture()
    first = (fixture.value).transfers[0]
    second = replace(first,
        provisional_hold=replace(first.provisional_hold, transfer_token="second-hold"),
        final_hold=replace(first.final_hold, transfer_token="second-hold"))
    slot = replace((fixture.value), transfers=(first, second))
    manifest = OutputPublicationManifest.create(fixture.header, slot)
    assert first.contained_object_id == second.contained_object_id
    assert first.final_hold != second.final_hold
    assert (manifest.value.edges) == (first.edge, second.edge)
    # Distinct metadata is not evidence of child Release or physical GC.
    assert pickle.loads(pickle.dumps(manifest)) == manifest


def test_no_refs_is_a_real_nonempty_output_manifest():
    fixture = _Fixture(refs=False)
    assert (fixture.manifest.value.edges) == ()
    assert (type(fixture.manifest.value) is OutputValue)
    assert (fixture.envelope.result.object_id) == (fixture.publication_id.object_id)
    with pytest.raises(TypeError, match=('OutputValue')):
        OutputPublicationManifest.create(fixture.header, ())


def test_reference_free_and_contained_outputs_share_one_protocol_type():
    plain, contained = _Fixture(refs=False), _Fixture(refs=True)
    assert type(plain.manifest) is type(contained.manifest) is OutputPublicationManifest
    assert type(plain.envelope) is type(contained.envelope) is OutputPublicationEnvelope
    assert (plain.manifest.value.edges) == () and len((contained.manifest.value.edges)) == 2


def test_identity_requires_only_canonical_index_zero():
    fixture = _Fixture()
    publication = fixture.publication_id
    assert (publication.object_id) == (ObjectID.for_task(fixture.task, 0))
    assert (publication.execution.object_id) == (publication.object_id)
    assert (fixture.envelope.result.object_id) == (publication.object_id)
    assert tuple((field.name) for field in (fields(TaskExecution))) == (('attempt_id',))
    assert tuple(field.name for field in fields(OutputValue)) == ('tier', 'size_bytes', 'checksum', 'transfers')
    assert tuple(field.name for field in fields(OutputPublicationManifest)) == ('header', 'value', 'manifest_digest')
    assert tuple(field.name for field in fields(OutputPublicationEnvelope)) == ('manifest', 'complete', 'result')
    for value in (((), (fixture.value,), (fixture.value, fixture.value))):
        with pytest.raises(TypeError, match=('OutputValue')):
            OutputPublicationManifest.create(fixture.header, value)
    with pytest.raises(TypeError):
        replace(fixture.value, object_id=ObjectID(fixture.task, 1))
    renumbered = (replace(fixture.result, object_id=ObjectID(fixture.task, 1)))
    with pytest.raises(OutputPublicationConflictError, match=('result descriptor')):
        (OutputPublicationEnvelope(fixture.manifest, fixture.witness, renumbered))


def test_typed_top_level_boundaries_reject_wrong_identity_and_payload_objects():
    fixture = _Fixture()
    for lease, execution in (
        (fixture.node, fixture.execution), (fixture.lease, (fixture.attempt)),
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
    with pytest.raises(TypeError, match=('OutputValue')):
        OutputPublicationManifest.create(fixture.header, (fixture.result))
    with pytest.raises(TypeError, match=('OutputValue')):
        OutputPublicationManifest.create(fixture.header, b"result-bytes")
    with pytest.raises(TypeError, match="OutputPublicationManifest"):
        OutputPublicationCompleteWitness.for_manifest(fixture.envelope)
    with pytest.raises(TypeError, match="OutputPublicationCompleteWitness"):
        OutputPublicationEnvelope(fixture.manifest, fixture.header, (fixture.result))
    with pytest.raises(TypeError, match=('ResultDescriptor')):
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
        manifest = OutputPublicationManifest.create(replace(fixture.header, publication_id=publication_id), (fixture.value))
        assert manifest.manifest_digest != fixture.manifest.manifest_digest
    other_task = _id(TaskID, 41)
    other = OutputPublicationID(fixture.lease, (TaskExecution(AttemptID(other_task, fixture.attempt.attempt_number))))
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
        changed = OutputPublicationManifest.create(header, (plain.value))
        assert changed.manifest_digest != plain.manifest.manifest_digest
        with pytest.raises(OutputPublicationConflictError, match="manifest_digest"):
            replace(plain.manifest, header=header)
    first = (fixture.value)
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
        changed = OutputPublicationManifest.create(fixture.header, slot)
        assert changed.manifest_digest != fixture.manifest.manifest_digest
        with pytest.raises(OutputPublicationConflictError, match="manifest_digest"):
            replace(fixture.manifest, value=slot)


def test_borrowed_contained_source_fingerprint_binds_complete_typed_hold():
    fixture = _Fixture()
    transfer = (fixture.value).transfers[1]
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
        slot = replace((fixture.value), transfers=(changed_transfer,))
        manifest = OutputPublicationManifest.create(fixture.header, slot)
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
        replace((fixture.value), transfers=(forged,))


@pytest.mark.parametrize('field,value', (('tier', 'INLINE'), ('size_bytes', True), ('size_bytes', -1), ('size_bytes', 1 << 64), ('checksum', 'g' * 64), ('checksum', ' ' * 64), ('checksum', b'ab' * 32), ('transfers', b'not-transfers')))
def test_value_rejects_malformed_leaf_values(field, value):
    with pytest.raises((TypeError, ValueError)):
        replace(_Fixture().value, **{field: value})


def test_value_rejects_wrong_container_and_conflicting_duplicate_hold():
    fixture = _Fixture()
    first = fixture.value
    other_outer = ObjectID.for_task(_id(TaskID, 44))
    wrong = replace(first.transfers[0], provisional_hold=replace(first.transfers[0].provisional_hold, container_object_id=other_outer), final_hold=replace(first.transfers[0].final_hold, container_object_id=other_outer))
    with pytest.raises(OutputPublicationConflictError, match='custody'):
        OutputPublicationManifest.create(fixture.header, replace(first, transfers=(wrong,)))
    with pytest.raises(OutputPublicationConflictError, match='multiple transfers'):
        replace(first, transfers=(first.transfers[0], first.transfers[0]))
    same_hold_different_source = replace(first.transfers[1], source=replace(first.transfers[1].source, borrower_token='other-proof'))
    with pytest.raises(OutputPublicationConflictError, match='multiple transfers'):
        replace(first, transfers=(first.transfers[1], same_hold_different_source))


def test_header_rejects_source_custody_drift_and_shared_child_owner_conflict():
    fixture = _Fixture()
    for header in (
        replace(fixture.header, executor_worker_id=_id(WorkerID, 21)),
        replace(fixture.header, owner_worker_id=_id(WorkerID, 22)),
    ):
        with pytest.raises(OutputPublicationConflictError, match="custody"):
            OutputPublicationManifest.create(header, (fixture.value))
    borrowed = (fixture.value).transfers[1]
    wrong_source = replace(borrowed, source=replace(borrowed.source, borrower_worker_id=_id(WorkerID, 23)))
    wrong_slot = replace((fixture.value), transfers=(wrong_source,))
    with pytest.raises(OutputPublicationConflictError, match="source.*executor"):
        OutputPublicationManifest.create(fixture.header, wrong_slot)
    changed_owner = replace(borrowed, contained_owner_worker_id=_id(WorkerID, 24),
        provisional_hold=replace(borrowed.provisional_hold, transfer_token="other-hold"),
        final_hold=replace(borrowed.final_hold, transfer_token="other-hold"))
    other_slot = replace((fixture.value), transfers=(borrowed, changed_owner))
    with pytest.raises(OutputPublicationConflictError, match="conflicting owners"):
        OutputPublicationManifest.create(fixture.header, other_slot)


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
            OutputPublicationEnvelope(fixture.manifest, witness, (fixture.result))


@pytest.mark.parametrize("case", ("object", "missing", "duplicate", "owner", "node", "size", "checksum", "tier"))
def test_envelope_rejects_result_manifest_drift(case):
    fixture = _Fixture(stored=True)
    first = (fixture.result)
    result = {
        'object': replace(first, object_id=ObjectID.for_task(_id(TaskID, 32))),
        'missing': None,
        'duplicate': (first, first),
        'owner': replace(first, owner_worker_id=_id(WorkerID, 31)),
        'node': replace(first, node_id=_id(NodeID, 32)),
        'size': replace(first, size_bytes=first.size_bytes + 1),
        'checksum': replace(first, checksum='ab' * 32),
        'tier': replace(first, storage=protocol.ResultStorage.INLINE, inline_data=fixture.payload),
    }[case]
    error = TypeError if case in ('missing', 'duplicate') else OutputPublicationConflictError
    with pytest.raises(error):
        OutputPublicationEnvelope(fixture.manifest, fixture.witness, result)


@pytest.mark.parametrize("tamper", (
    "id-bytes", "task-id-type", "attempt-number", "attempt-task",
    ('missing-attempt'), ('nonzero-output-hold'), "node-id", "node-epoch",
    "transfer-owner", "transfer-port", "hold-id", "hold-owner",
    "source-id", "source-token", "task-source-kind", "task-source-origin",
))
def test_manifest_create_revalidates_deep_tampered_values(tamper):
    fixture = _Fixture()
    header = fixture.header
    transfer = (fixture.value).transfers[1]
    if tamper == "id-bytes":
        object.__setattr__(header.publication_id.lease_id, "value", b"short")
    elif tamper == "task-id-type":
        object.__setattr__((header.publication_id.execution.attempt_id), "task_id", fixture.job)
    elif tamper == "attempt-number":
        object.__setattr__(header.publication_id.execution.attempt_id, "attempt_number", True)
    elif tamper == "attempt-task":
        object.__setattr__(header.publication_id.execution.attempt_id, "task_id", _id(TaskID, 40))
    elif tamper == ('missing-attempt'):
        object.__setattr__((header.publication_id.execution), ('attempt_id'), (None))
    elif tamper == ('nonzero-output-hold'):
        for hold in (transfer.provisional_hold, transfer.final_hold):
            object.__setattr__(hold, 'container_object_id', ObjectID(fixture.task, 1))
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
        OutputPublicationManifest.create(header, (fixture.value))


def test_envelope_revalidates_descriptor_payload_checksum_and_nested_ids():
    for attribute, value in (
        ("inline_data", b"wrong-result"), ("size_bytes", True),
        ("storage", "INLINE"), ("owner_worker_id", _id(NodeID, 1)),
    ):
        fixture = _Fixture()
        object.__setattr__((fixture.result), attribute, value)
        with pytest.raises((TypeError, ValueError, ProtocolError)):
            OutputPublicationEnvelope(fixture.manifest, fixture.witness, (fixture.result))


def test_complete_witness_and_manifest_copy_revalidate_tampered_manifest():
    for construct in (
        OutputPublicationCompleteWitness.for_manifest,
        lambda value: replace(value),
    ):
        fixture = _Fixture()
        object.__setattr__((fixture.manifest.value), "checksum", "ff" * 32)
        with pytest.raises(OutputPublicationConflictError, match="manifest_digest"):
            construct(fixture.manifest)
    fixture = _Fixture()
    object.__setattr__(fixture.witness, "status", "SUCCEEDED")
    with pytest.raises(TypeError, match="TaskReplyStatus"):
        OutputPublicationEnvelope(fixture.manifest, fixture.witness, (fixture.result))


def test_constructed_manifest_detaches_mutable_aliases_from_input_values():
    fixture = _Fixture()
    original_digest = fixture.manifest.manifest_digest
    source = (fixture.value).transfers[1].source
    object.__setattr__(source, "borrower_token", "changed-after-construction")
    object.__setattr__(fixture.header.node_incarnation, "node_pid", 9999)
    assert fixture.manifest.header.node_incarnation.node_pid == 1701
    assert (fixture.manifest.value).transfers[1].source.borrower_token == "borrow-token"
    assert replace(fixture.manifest).manifest_digest == original_digest


def test_pickle_roundtrip_reenters_validation_without_using_pickle_as_digest():
    fixture = _Fixture()
    assert pickle.loads(pickle.dumps(fixture.envelope)) == fixture.envelope
    assert pickle.loads(pickle.dumps(fixture.manifest)) == fixture.manifest
    object.__setattr__((fixture.envelope.result), "inline_data", b"changed-bytes")
    with pytest.raises(ProtocolError, match="inline result"):
        pickle.loads(pickle.dumps(fixture.envelope))


def test_rebuilt_values_have_deterministic_digest_and_canonical_hex():
    first, second = _Fixture(), _Fixture()
    assert first.manifest == second.manifest
    assert first.manifest.manifest_digest == second.manifest.manifest_digest
    assert first.publication_id.transaction_id == second.publication_id.transaction_id
    value = (replace(first.value, checksum=first.value.checksum.upper()))
    rebuilt = OutputPublicationManifest.create(replace(first.header), value)
    assert rebuilt == first.manifest
    assert replace(first.manifest, manifest_digest=first.manifest.manifest_digest.upper()) == first.manifest
    # Known handoff framing uses canonical index zero, not a graph scope.
    framed = hashlib.sha256(b"miniray-output-publication-handoff-v1\0")
    for value in (bytes(first.lease), bytes(first.task), first.attempt.attempt_number.to_bytes(8, "big"),
                  bytes(first.task), (0).to_bytes(8, "big")):
        framed.update(len(value).to_bytes(8, "big"))
        framed.update(value)
    assert first.publication_id.transaction_id == "output-publication-handoff:" + framed.hexdigest()
