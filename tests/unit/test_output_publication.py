"""Small pure contracts for a unified selected-output value model."""

from __future__ import annotations

import hashlib
import pickle
from dataclasses import fields, is_dataclass, replace
from enum import Enum

import pytest

from miniray import protocol
from miniray.contained_cycle import (
    ContainedGraphManifestDisposition, ContainedReferenceGraphAuthority,
)
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
from miniray.task_outputs import (
    TargetExecutionKey, TargetOutputManifest, TaskExecutionKey, TaskOutputManifest,
)


pytestmark = pytest.mark.unit


def _id(kind, byte: int):
    return kind(bytes((byte,)) * 16)


class _Fixture:
    def __init__(self, *, target: bool = False, refs: bool = True) -> None:
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
        self.full = TaskOutputManifest.for_task(self.task, 4 if target else 2)
        self.execution = (
            TargetExecutionKey(
                TargetOutputManifest(self.full, (self.full.output_ids[1], self.full.output_ids[3])),
                self.attempt,
            ) if target else TaskExecutionKey(self.full, self.attempt)
        )
        self.publication_id = OutputPublicationID(self.lease, self.execution)
        self.header = OutputPublicationHeader(
            self.publication_id, self.job, self.executor, self.owner,
            OutputPublicationNodeIncarnation(self.node, 1701, 2),
        )
        self.payloads = (b"inline-result", b"stored-result")
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
                object_id, protocol.ResultStorage.INLINE if index == 0
                else protocol.ResultStorage.OBJECT_STORE, len(payload),
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


def test_mixed_slots_form_one_metadata_graph_and_one_data_plane_envelope():
    fixture = _Fixture()
    manifest = fixture.manifest
    graph = manifest.to_graph_manifest()
    assert manifest.execution == fixture.execution
    assert manifest.slots == fixture.slots
    assert graph.publication_id == fixture.publication_id
    assert graph.transaction_id == fixture.publication_id.graph_transaction_id
    assert graph.manifest_digest == manifest.manifest_digest
    assert graph.outer_owner_worker_id == fixture.owner
    assert graph.ordered_edges == tuple(edge for slot in fixture.slots for edge in slot.edges)
    assert len(graph.ordered_edges) == 4
    assert fixture.envelope.results == fixture.results
    assert fixture.envelope.results[0].inline_data == fixture.payloads[0]
    assert fixture.envelope.results[1].inline_data is None
    assert fixture.envelope.publication_id == fixture.publication_id
    assert OutputPublicationNodeIncarnation is PublicationNodeIncarnation
    for metadata in (fixture.publication_id, fixture.header, manifest, fixture.witness, graph):
        _assert_metadata(metadata)


def test_cross_slot_shared_children_have_distinct_holds_and_independent_gc():
    fixture = _Fixture()
    first, second = fixture.slots
    assert first.transfers[0].contained_object_id == second.transfers[0].contained_object_id
    assert first.transfers[0].final_hold.transfer_token == second.transfers[0].final_hold.transfer_token
    assert first.transfers[0].final_hold != second.transfers[0].final_hold
    graph = fixture.manifest.to_graph_manifest()
    authority = ContainedReferenceGraphAuthority()
    authority.prepare_manifest(graph)
    authority.commit_manifest(graph)
    released = authority.release_manifest_container(graph, first.object_id)
    assert released.released_edges == first.edges
    (snapshot,) = authority.snapshot().manifests
    assert snapshot.active_edges == second.edges
    replay = authority.release_manifest_container(graph, first.object_id)
    assert replay.disposition is ContainedGraphManifestDisposition.ALREADY_RELEASED
    assert authority.release_manifest_container(graph, second.object_id).released_edges == second.edges


def test_no_refs_is_a_real_nonempty_output_manifest_without_graph_effect():
    fixture = _Fixture(refs=False)
    assert fixture.manifest.to_graph_manifest() is None
    assert fixture.manifest.ordered_edges == ()
    assert len(fixture.manifest.slots) == 2
    assert len(fixture.envelope.results) == 2
    with pytest.raises(OutputPublicationConflictError, match="selected execution"):
        OutputPublicationManifest.create(fixture.header, ())


def test_single_slot_is_a_degenerate_batch_not_a_distinct_protocol():
    fixture = _Fixture()
    execution = TaskExecutionKey(TaskOutputManifest.for_task(fixture.task, 1), fixture.attempt)
    header = replace(fixture.header, publication_id=OutputPublicationID(fixture.lease, execution))
    manifest = OutputPublicationManifest.create(header, (fixture.slots[0],))
    envelope = OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest), fixture.results[:1]
    )
    assert len(manifest.slots) == len(envelope.results) == 1
    assert manifest.to_graph_manifest().ordered_edges == fixture.slots[0].edges


def test_targeted_identity_keeps_original_indices_and_full_canonical_manifest():
    fixture = _Fixture(target=True)
    publication = fixture.publication_id
    assert tuple(item.return_index for item in publication.output_ids) == (1, 3)
    assert publication.full_output_ids == fixture.full.output_ids
    assert tuple(slot.object_id for slot in fixture.manifest.slots) == publication.output_ids
    assert tuple(item.object_id for item in fixture.envelope.results) == publication.output_ids
    for slots in (fixture.slots[::-1], fixture.slots[:1], fixture.slots + fixture.slots[:1]):
        with pytest.raises(OutputPublicationConflictError, match="ordered selected"):
            OutputPublicationManifest.create(fixture.header, slots)
    renumbered = replace(fixture.slots[0], object_id=fixture.full.output_ids[0], transfers=())
    with pytest.raises(OutputPublicationConflictError, match="ordered selected"):
        OutputPublicationManifest.create(fixture.header, (renumbered, fixture.slots[1]))


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


def test_identity_digest_binds_scope_full_manifest_lease_attempt_and_domain():
    fixture = _Fixture(target=True)
    target = fixture.execution
    same_slots_wider_full = TargetExecutionKey(
        TargetOutputManifest(
            TaskOutputManifest.for_task(fixture.task, 5), target.target_output_ids
        ), fixture.attempt,
    )
    variants = (
        replace(fixture.publication_id, lease_id=_id(LeaseID, 40)),
        replace(fixture.publication_id, execution=target.for_attempt(fixture.attempt.next())),
        replace(fixture.publication_id, execution=same_slots_wider_full),
    )
    assert len({fixture.publication_id.graph_transaction_id, *(item.graph_transaction_id for item in variants)}) == 4
    for publication_id in variants:
        manifest = OutputPublicationManifest.create(replace(fixture.header, publication_id=publication_id), fixture.slots)
        assert manifest.manifest_digest != fixture.manifest.manifest_digest
    whole = TaskExecutionKey(fixture.full, fixture.attempt)
    full_target = TargetExecutionKey(TargetOutputManifest(fixture.full, fixture.full.output_ids), fixture.attempt)
    assert OutputPublicationID(fixture.lease, whole).graph_transaction_id != OutputPublicationID(fixture.lease, full_target).graph_transaction_id


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
    first, second = fixture.slots
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
        changed = OutputPublicationManifest.create(fixture.header, (slot, second))
        assert changed.manifest_digest != fixture.manifest.manifest_digest
        with pytest.raises(OutputPublicationConflictError, match="manifest_digest"):
            replace(fixture.manifest, slots=(slot, second))


def test_borrowed_contained_source_fingerprint_includes_original_typed_or_legacy_hold():
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
        protocol.ContainedTransferSource("source-pin"),
        protocol.ContainedTransferSource("another-source-pin"),
    )
    digests = []
    for original in originals:
        changed_transfer = replace(transfer, source=replace(source, original_source=original))
        slot = replace(fixture.slots[0], transfers=(changed_transfer,))
        manifest = OutputPublicationManifest.create(fixture.header, (slot, fixture.slots[1]))
        _assert_metadata(manifest)
        digests.append(manifest.manifest_digest)
    assert len(set(digests)) == len(originals)


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
    first, second = fixture.slots
    with pytest.raises(OutputPublicationConflictError, match="belong to its output slot"):
        replace(first, transfers=(second.transfers[0],))
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
        OutputPublicationManifest.create(fixture.header, (wrong_slot, fixture.slots[1]))
    changed_owner = replace(fixture.slots[1].transfers[1], contained_owner_worker_id=_id(WorkerID, 24))
    other_slot = replace(fixture.slots[1], transfers=(changed_owner,))
    with pytest.raises(OutputPublicationConflictError, match="conflicting owners"):
        OutputPublicationManifest.create(fixture.header, (fixture.slots[0], other_slot))


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


@pytest.mark.parametrize("case", ("order", "missing", "duplicate", "owner", "node", "size", "checksum", "tier"))
def test_envelope_rejects_result_manifest_drift(case):
    fixture = _Fixture()
    first, second = fixture.results
    results = {
        "order": (second, first), "missing": (first,),
        "duplicate": (first, second, first),
        "owner": (first, replace(second, owner_worker_id=_id(WorkerID, 31))),
        "node": (first, replace(second, node_id=_id(NodeID, 32))),
        "size": (first, replace(second, size_bytes=second.size_bytes + 1)),
        "checksum": (first, replace(second, checksum="ab" * 32)),
        "tier": (replace(first, storage=protocol.ResultStorage.OBJECT_STORE, inline_data=None), second),
    }[case]
    with pytest.raises(OutputPublicationConflictError):
        OutputPublicationEnvelope(fixture.manifest, fixture.witness, results)


@pytest.mark.parametrize("tamper", (
    "id-bytes", "task-id-type", "attempt-number", "attempt-task",
    "full-manifest", "selected-order", "node-id", "node-epoch",
    "transfer-owner", "transfer-port", "hold-id", "hold-owner",
    "source-id", "source-token", "task-source-kind", "task-source-origin",
))
def test_manifest_create_revalidates_deep_tampered_values(tamper):
    fixture = _Fixture(target=True)
    header = fixture.header
    transfer = fixture.slots[0].transfers[1]
    if tamper == "id-bytes":
        object.__setattr__(header.publication_id.lease_id, "value", b"short")
    elif tamper == "task-id-type":
        object.__setattr__(header.publication_id.execution.manifest.full_manifest, "task_id", fixture.job)
    elif tamper == "attempt-number":
        object.__setattr__(header.publication_id.execution.attempt_id, "attempt_number", True)
    elif tamper == "attempt-task":
        object.__setattr__(header.publication_id.execution.attempt_id, "task_id", _id(TaskID, 40))
    elif tamper == "full-manifest":
        full = header.publication_id.execution.manifest.full_manifest
        object.__setattr__(full, "output_ids", full.output_ids[::-1])
    elif tamper == "selected-order":
        target = header.publication_id.execution.manifest
        object.__setattr__(target, "target_output_ids", target.target_output_ids[::-1])
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


def test_graph_projection_and_complete_witness_revalidate_tampered_manifest():
    for construct in (
        lambda value: value.to_graph_manifest(),
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
    fixture = _Fixture(target=True)
    assert pickle.loads(pickle.dumps(fixture.envelope)) == fixture.envelope
    assert pickle.loads(pickle.dumps(fixture.manifest)) == fixture.manifest
    object.__setattr__(fixture.envelope.results[0], "inline_data", b"changed-bytes")
    with pytest.raises(ProtocolError, match="inline result"):
        pickle.loads(pickle.dumps(fixture.envelope))


def test_rebuilt_values_have_deterministic_digest_and_canonical_hex():
    first, second = _Fixture(target=True), _Fixture(target=True)
    assert first.manifest == second.manifest
    assert first.manifest.manifest_digest == second.manifest.manifest_digest
    assert first.publication_id.graph_transaction_id == second.publication_id.graph_transaction_id
    slots = tuple(replace(slot, checksum=slot.checksum.upper()) for slot in first.slots)
    rebuilt = OutputPublicationManifest.create(replace(first.header), list(slots))
    assert rebuilt == first.manifest
    assert replace(first.manifest, manifest_digest=first.manifest.manifest_digest.upper()) == first.manifest
    # Stable vectors freeze field order and domain framing independently of
    # object identity or a serialization library's representation.
    assert first.publication_id.graph_transaction_id == (
        "output-publication-graph:"
        "66602cb5728fe5750db4f7e41bacf660bd032e4fe3fcc81767258da66319d81a"
    )
    assert first.manifest.manifest_digest == (
        "8f392c12c27f22b862883cca82f76e2a00481b303153f07e5a5483cb8b4bd037"
    )
