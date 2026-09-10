"""Pure full-owner reachability after current single-output publication and GC.

One INLINE result exercises owner-held bytes; one child executes real
prepare/promote/release/collection transitions. Argument, keyword and function
bytes are installed before owner registration. No GCS graph, runtime, transport,
thread, process, user execution or payload deserialization is used.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
from enum import Enum
import hashlib

import pytest

from miniray import protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope, OutputPublicationManifest,
)
from miniray.ownership import (
    ObjectCollectionState, ObjectOwnerTable, ObjectState,
    OutputOwnerPublicationCollectionPlan, OutputOwnerPublicationConflictError,
    OutputOwnerPublicationDisposition, OutputOwnerPublicationMembership,
    OutputOwnerPublicationPlan, StoredContainedReferenceDisposition,
)
from tests.unit.test_output_owner_publication import _Fixture as _OwnerValues


pytestmark = pytest.mark.unit

_ARGUMENT = b"argument-stream-must-not-survive-full-owner-gc"
_KEYWORD = b"keyword-stream-must-not-survive-full-owner-gc"
_FUNCTION = b"serialized-function-must-not-survive-full-owner-gc"
_RESULT = b"single-inline-result-must-be-forgotten"


class _Fixture(_OwnerValues):
    def __init__(self):
        super().__init__(edges=True, all_stored=False)
        self.output = (self.publication_id.object_id)
        self.transfer = (self.manifest.value).transfers[0]
        # Set payload-bearing lineage before table() registers the TaskSpec.
        self.spec = replace(
            self.spec, args=(protocol.InlineArg(_ARGUMENT),),
            function_definition=protocol.FunctionDefinition.from_payload(self.spec.function, _FUNCTION),
            kwargs=(("payload", protocol.InlineArg(_KEYWORD)),),
        )
        self.plan = self.publication(_RESULT)

    def publication(self, payload):
        checksum = hashlib.sha256(payload).hexdigest()
        value = replace((self.manifest.value), size_bytes=len(payload), checksum=checksum)
        manifest = OutputPublicationManifest.create(self.header, value)
        result = replace(
            (self.envelope.result), size_bytes=len(payload), checksum=checksum, inline_data=payload,
        )
        envelope = OutputPublicationEnvelope(
            manifest, OutputPublicationCompleteWitness.for_manifest(manifest), result,
        )
        return OutputOwnerPublicationPlan(self.execution, envelope)

    def collect_all(self):
        owner, child_owner = self.table(), ObjectOwnerTable()
        child_attempt = AttemptID(self.child.task_id, 0)
        child_owner.register(self.child, current_attempt=child_attempt, local_token="child-handle")
        assert child_owner.publish_inline(self.child, child_attempt, b"child-value")
        assert child_owner.prepare_stored_contained_reference(
            self.transfer, authority_worker_id=self.executor,
        ) is StoredContainedReferenceDisposition.PREPARED
        assert child_owner.promote_stored_contained_reference(
            self.transfer, authority_worker_id=self.executor,
        ) is StoredContainedReferenceDisposition.PROMOTED
        assert child_owner.release_local_reference(self.child, "child-handle")
        assert child_owner.snapshot(self.child).contained_holds == frozenset((self.transfer.final_hold,))
        assert child_owner.contained_release_was_seen(self.child, self.transfer.provisional_hold)
        assert owner.commit_output_publication(self.plan).disposition is OutputOwnerPublicationDisposition.APPLIED
        snapshot = owner.snapshot(self.output)
        assert snapshot.state is ObjectState.READY_INLINE and snapshot.inline_data == _RESULT
        assert snapshot.producer_task_spec == self.spec
        assert snapshot.producer_task_spec.args[0].data == _ARGUMENT
        assert snapshot.producer_task_spec.kwargs[0][1].data == _KEYWORD
        assert snapshot.producer_task_spec.function_definition.payload == _FUNCTION

        collection_id = "full-owner-gc"
        assert owner.begin_output_publication_collection(self.output, collection_id=collection_id) is None
        self.release_handle(owner, self.output)
        plan = owner.begin_output_publication_collection(self.output, collection_id=collection_id)
        assert plan is not None and plan.metadata_plan.producer_task_spec == self.spec
        assert owner.collection_state(self.output) is ObjectCollectionState.COLLECTING
        assert owner.output_publication_collection_receipt(plan) is None
        assert plan.metadata_plan.locations == ()
        assert len(plan.metadata_plan.contained_releases) == 1
        edge = plan.metadata_plan.contained_releases[0]
        assert edge.incoming_hold(self.owner) == self.transfer.final_hold
        assert child_owner.release_contained_reference(self.child, edge.incoming_hold(self.owner))
        assert not child_owner.snapshot(self.child).contained_holds
        assert child_owner.contained_release_was_seen(self.child, self.transfer.final_hold)
        # The actual child effect precedes the local owner CAS. The current
        # owner API receives its frozen plan, not an invented GCS graph ACK.
        completed = owner.complete_output_publication_collection(plan)
        assert completed.disposition is OutputOwnerPublicationDisposition.APPLIED
        assert completed.collection.collected
        assert completed.collection.contained_releases == plan.metadata_plan.contained_releases
        assert not completed.collection.lineage_releases
        assert not owner.contains(self.output)
        assert owner.collection_state(self.output) is ObjectCollectionState.COLLECTED
        assert owner.output_owner_publication(self.output) is None
        assert owner.output_owner_result(self.output) is None

        child_gc = child_owner.begin_collection(self.child, collection_id="child-gc")
        assert child_gc is not None and child_owner.complete_collection(child_gc).collected
        assert not owner._entries and not owner._task_lineage
        assert not child_owner._entries
        return owner, child_owner, plan, completed


def _owner_state(owner):
    # Cover every root, including future caches. Only the synchronization lock
    # is excluded; never select just expected terminal-receipt dictionaries.
    assert "_lock" in vars(owner)
    return {name: value for name, value in vars(owner).items() if name != "_lock"}


def _assert_metadata_only(owner):
    seen = set()

    def visit(value, path):
        if type(value) in (JobID, TaskID, WorkerID, NodeID, LeaseID):
            # Only these exact opaque identity leaves may own bytes. A random
            # 16-byte payload or an extra identity cache receives no exemption.
            assert vars(value).keys() == {"value"}, path
            assert type(value.value) is bytes and len(value.value) == 16, path
            return
        assert not isinstance(value, (
            bytes, bytearray, memoryview, protocol.TaskSpec, protocol.FunctionDefinition,
            protocol.InlineArg, protocol.ResultDescriptor, OutputPublicationEnvelope,
            OutputOwnerPublicationPlan, OutputOwnerPublicationCollectionPlan,
        )), f"owner retained payload-bearing {type(value).__name__} at {path}"
        if value is None or type(value) in (str, bool, int):
            return
        if id(value) in seen:
            return
        seen.add(id(value))
        if isinstance(value, Enum):
            visit(value.value, f"{path}.value")
        elif is_dataclass(value) and not isinstance(value, type):
            members = {member.name for member in fields(value)}
            for name in sorted(members):
                visit(getattr(value, name), f"{path}.{name}")
            # Declared fields must not hide an extra instance cache.
            for name, item in vars(value).items():
                if name not in members:
                    visit(item, f"{path}.{name}")
        elif isinstance(value, dict):
            for index, (key, item) in enumerate(value.items()):
                visit(key, f"{path}.key[{index}]")
                visit(item, f"{path}.value[{index}]")
        elif isinstance(value, (tuple, list, set, frozenset)):
            for index, item in enumerate(value):
                visit(item, f"{path}[{index}]")
        else:
            raise AssertionError(f"unexpected owner history {type(value).__name__} at {path}")

    visit(_owner_state(owner), "owner")


def test_full_owner_gc_forgets_all_payloads_but_replays_exact_caller_plans():
    fixture = _Fixture()
    owner, child_owner, plan, completed = fixture.collect_all()
    _assert_metadata_only(owner)
    _assert_metadata_only(child_owner)
    terminal = deepcopy(_owner_state(owner))
    child_terminal = deepcopy(_owner_state(child_owner))
    replayed = replace(completed, disposition=OutputOwnerPublicationDisposition.ALREADY_APPLIED)
    # Caller-retained source bytes remain legal. Both original and structurally
    # rebuilt plans must recover the same exact ACK without owner retention.
    rebuilt = replace(plan, metadata_plan=replace(
        plan.metadata_plan, producer_task_spec=replace(fixture.spec),
    ))
    assert rebuilt == plan and rebuilt is not plan
    for retained in (plan, rebuilt):
        assert owner.output_publication_collection_receipt(retained) == replayed
        assert owner.complete_output_publication_collection(retained) == replayed
    assert owner.begin_output_publication_collection(plan.object_id, collection_id=plan.collection_id) is None
    assert owner.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED
    assert _owner_state(owner) == terminal
    assert _owner_state(child_owner) == child_terminal
    _assert_metadata_only(owner)
    _assert_metadata_only(child_owner)


@pytest.mark.parametrize("changed_field", ["argument", "keyword", "function", "result", "collection_id"])
def test_terminal_owner_rejects_changed_payload_or_collection_identity(changed_field):
    fixture = _Fixture()
    owner, child_owner, plan, completed = fixture.collect_all()
    terminal = deepcopy(_owner_state(owner))
    child_terminal = deepcopy(_owner_state(child_owner))
    metadata, membership = plan.metadata_plan, plan.membership
    if changed_field == "argument":
        spec = replace(fixture.spec, args=(protocol.InlineArg(b"altered-argument-payload"),))
        metadata = replace(metadata, producer_task_spec=spec)
    elif changed_field == "keyword":
        spec = replace(fixture.spec, kwargs=(("payload", protocol.InlineArg(b"altered-keyword-payload")),))
        metadata = replace(metadata, producer_task_spec=spec)
    elif changed_field == "function":
        spec = replace(fixture.spec, function_definition=protocol.FunctionDefinition.from_payload(
            fixture.spec.function, b"altered-function-payload",
        ))
        metadata = replace(metadata, producer_task_spec=spec)
    elif changed_field == "result":
        changed_publication = fixture.publication(b"altered-inline-result-payload")
        with pytest.raises(OutputOwnerPublicationConflictError, match="rebound"):
            owner.commit_output_publication(changed_publication)
        # Rebuild a self-consistent manifest/witness/descriptor. Rejection must
        # bind terminal identity rather than an unrelated malformed envelope.
        membership = OutputOwnerPublicationMembership(changed_publication.envelope.manifest, 0)
    else:
        metadata = replace(metadata, collection_id="different-gc")
        with pytest.raises(OutputOwnerPublicationConflictError, match="identity changed"):
            owner.begin_output_publication_collection(plan.object_id, collection_id=metadata.collection_id)
    changed = OutputOwnerPublicationCollectionPlan(membership, metadata)
    with pytest.raises(OutputOwnerPublicationConflictError, match="terminal identity"):
        owner.complete_output_publication_collection(changed)
    with pytest.raises(OutputOwnerPublicationConflictError, match="terminal identity"):
        owner.output_publication_collection_receipt(changed)
    assert owner.output_publication_collection_receipt(plan).collection == completed.collection
    assert _owner_state(owner) == terminal
    assert _owner_state(child_owner) == child_terminal
    _assert_metadata_only(owner)
    _assert_metadata_only(child_owner)
