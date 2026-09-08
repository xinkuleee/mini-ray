"""Pure full-owner reachability after two-slot output publication and GC.

Two INLINE results exercise real owner-held bytes; one shared child exercises
distinct final holds and per-container graph release.  Every authority is an
in-memory state machine.  No runtime, transport, thread, process or payload
deserialization is started.  Function/argument bytes are installed in TaskSpec
before registration, not retrofitted into an already-registered fixture.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import fields, is_dataclass, replace
from enum import Enum
import hashlib

import pytest

from miniray import protocol
from miniray.contained_cycle import ContainedReferenceGraphAuthority
from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputSlotManifest,
)
from miniray.ownership import (
    ObjectCollectionState, ObjectOwnerTable, ObjectState,
    OutputOwnerPublicationCollectionPlan, OutputOwnerPublicationConflictError,
    OutputOwnerPublicationDisposition, OutputOwnerPublicationMembership,
    OutputOwnerPublicationPlan, StoredContainedReferenceDisposition,
)
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecutionKey


pytestmark = pytest.mark.unit

_ARGUMENT = b"argument-stream-must-not-survive-full-owner-gc"
_KEYWORD = b"keyword-stream-must-not-survive-full-owner-gc"
_FUNCTION = b"serialized-function-must-not-survive-full-owner-gc"
_RESULTS = (b"slot-zero-inline-result-must-be-forgotten",
            b"slot-one-inline-result-must-be-forgotten")


class _Fixture:
    def __init__(self):
        self.job = JobID(b"J" * 16)
        self.owner_id, self.executor_id = WorkerID(b"O" * 16), WorkerID(b"E" * 16)
        self.node = NodeID(b"N" * 16)
        task = TaskID.derive(self.job, TaskID.for_driver(self.job), 73)
        self.attempt = AttemptID(task, 0)
        function = protocol.FunctionKey(self.job, __name__, "producer", "v1")
        self.spec = protocol.TaskSpec(
            self.job, task, self.attempt, function, (protocol.InlineArg(_ARGUMENT),),
            2, ResourceVector(), self.owner_id,
            function_definition=protocol.FunctionDefinition.from_payload(function, _FUNCTION),
            kwargs=(("payload", protocol.InlineArg(_KEYWORD)),),
        )
        self.execution = TaskExecutionKey.from_task_spec(self.spec)
        self.publication_id = OutputPublicationID(LeaseID(b"L" * 16), self.execution)
        self.header = OutputPublicationHeader(
            self.publication_id, self.job, self.executor_id, self.owner_id,
            OutputPublicationNodeIncarnation(self.node, 401, 1),
        )
        self.child_id = ObjectID.for_task(TaskID.for_put(self.job, self.executor_id, 0))
        self.transfers = tuple(
            PreparedContainedTransfer(
                self.child_id, self.executor_id, ("127.0.0.1", 31901),
                OwnedContainedSource(self.executor_id),
                ContainedReferenceHold(object_id, self.executor_id, "shared-child-token"),
                ContainedReferenceHold(object_id, self.owner_id, "shared-child-token"),
            )
            for object_id in self.publication_id.output_ids
        )
        self.plan = self.publication(_RESULTS)

    def publication(self, payloads):
        slots, results = [], []
        for object_id, payload, transfer in zip(
            self.publication_id.output_ids, payloads, self.transfers
        ):
            checksum = hashlib.sha256(payload).hexdigest()
            slots.append(OutputSlotManifest(
                object_id, protocol.ResultStorage.INLINE, len(payload), checksum, (transfer,),
            ))
            results.append(protocol.ResultDescriptor(
                object_id, protocol.ResultStorage.INLINE, len(payload),
                self.owner_id, self.node, checksum, payload,
            ))
        manifest = OutputPublicationManifest.create(self.header, tuple(slots))
        envelope = OutputPublicationEnvelope(
            manifest, OutputPublicationCompleteWitness.for_manifest(manifest), tuple(results),
        )
        return OutputOwnerPublicationPlan(self.execution, envelope)

    def collect_all(self):
        owner, child_owner = ObjectOwnerTable(), ObjectOwnerTable()
        owner.register_task_outputs(self.spec, local_tokens=("handle-0", "handle-1"))
        child_attempt = AttemptID(self.child_id.task_id, 0)
        child_owner.register(self.child_id, current_attempt=child_attempt, local_token="child-handle")
        assert child_owner.publish_inline(self.child_id, child_attempt, b"shared-child-value")
        for transfer in self.transfers:
            assert child_owner.prepare_stored_contained_reference(
                transfer, authority_worker_id=self.executor_id,
            ) is StoredContainedReferenceDisposition.PREPARED
            assert child_owner.promote_stored_contained_reference(
                transfer, authority_worker_id=self.executor_id,
            ) is StoredContainedReferenceDisposition.PROMOTED
        assert child_owner.release_local_reference(self.child_id, "child-handle")
        graph = ContainedReferenceGraphAuthority()
        graph_manifest = self.plan.envelope.manifest.to_graph_manifest()
        assert graph_manifest is not None
        graph.prepare_manifest(graph_manifest)
        graph.commit_manifest(graph_manifest)
        publication = owner.commit_output_publication(self.plan)
        assert publication.disposition is OutputOwnerPublicationDisposition.APPLIED
        for index, object_id in enumerate(self.publication_id.output_ids):
            snapshot = owner.snapshot(object_id)
            assert snapshot.state is ObjectState.READY_INLINE
            assert snapshot.inline_data == _RESULTS[index]
            assert snapshot.producer_task_spec == self.spec
            assert snapshot.producer_task_spec.args[0].data == _ARGUMENT
            assert snapshot.producer_task_spec.kwargs[0][1].data == _KEYWORD
            assert snapshot.producer_task_spec.function_definition.payload == _FUNCTION

        saved = []
        for index, object_id in enumerate(self.publication_id.output_ids):
            collection_id = f"full-owner-gc-{index}"
            assert owner.begin_output_publication_collection(object_id, collection_id=collection_id) is None
            assert owner.release_local_reference(object_id, f"handle-{index}")
            plan = owner.begin_output_publication_collection(object_id, collection_id=collection_id)
            assert plan is not None and plan.metadata_plan.producer_task_spec == self.spec
            assert owner.collection_state(object_id) is ObjectCollectionState.COLLECTING
            assert owner.output_publication_collection_receipt(plan) is None
            assert plan.metadata_plan.locations == ()
            assert len(plan.metadata_plan.contained_releases) == 1
            edge = plan.metadata_plan.contained_releases[0]
            assert edge.incoming_hold(self.owner_id) == self.transfers[index].final_hold
            assert child_owner.release_contained_reference(self.child_id, edge.incoming_hold(self.owner_id))
            graph_release = graph.release_manifest_container(graph_manifest, object_id)
            assert graph_release.released_edges == plan.membership.slot.edges
            completed = owner.complete_output_publication_collection(plan, graph_release)
            assert completed.disposition is OutputOwnerPublicationDisposition.APPLIED
            assert completed.collection.collected
            assert completed.collection.contained_releases == plan.metadata_plan.contained_releases
            assert not completed.collection.lineage_releases
            assert not owner.contains(object_id)
            assert owner.collection_state(object_id) is ObjectCollectionState.COLLECTED
            assert owner.output_owner_publication(object_id) is None
            assert owner.output_owner_result(object_id) is None
            saved.append((plan, graph_release, completed))
            remaining = self.transfers[index + 1:]
            assert child_owner.snapshot(self.child_id).contained_holds == frozenset(
                transfer.final_hold for transfer in remaining
            )
            (graph_state,) = graph.snapshot().manifests
            assert graph_state.active_edges == tuple(transfer.edge for transfer in remaining)
            for sibling_index in range(index + 1, 2):
                sibling = owner.snapshot(self.publication_id.output_ids[sibling_index])
                assert sibling.inline_data == _RESULTS[sibling_index]
                assert sibling.producer_task_spec == self.spec

        child_gc = child_owner.begin_collection(self.child_id, collection_id="shared-child-gc")
        assert child_gc is not None
        assert child_owner.complete_collection(child_gc).collected
        assert not owner._entries and not owner._task_lineage
        assert not child_owner._entries
        return owner, child_owner, tuple(saved)


def _owner_state(owner):
    # Cover every root, including future caches. Only the synchronization lock
    # is excluded; never select just the expected terminal-receipt dictionaries.
    assert "_lock" in vars(owner)
    return {name: value for name, value in vars(owner).items() if name != "_lock"}


def _assert_metadata_only(owner):
    seen = set()

    def visit(value, path):
        if type(value) in (JobID, TaskID, WorkerID, NodeID, LeaseID):
            # Only these typed opaque identity leaves may own bytes. A random
            # 16-byte payload elsewhere does not receive this exemption.
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
            # Dataclass fields alone must not hide an extra instance cache.
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
    owner, child_owner, saved = fixture.collect_all()
    _assert_metadata_only(owner)
    _assert_metadata_only(child_owner)
    terminal = deepcopy(_owner_state(owner))
    for plan, graph_release, completed in saved:
        replayed = replace(completed, disposition=OutputOwnerPublicationDisposition.ALREADY_APPLIED)
        # The caller may retain the source TaskSpec and its payloads. Both the
        # original and a structurally rebuilt plan must recover the exact ACK.
        rebuilt = replace(plan, metadata_plan=replace(
            plan.metadata_plan, producer_task_spec=replace(fixture.spec),
        ))
        assert rebuilt == plan and rebuilt is not plan
        for retained in (plan, rebuilt):
            assert owner.output_publication_collection_receipt(retained) == replayed
            assert owner.complete_output_publication_collection(retained, graph_release) == replayed
        assert owner.begin_output_publication_collection(plan.object_id, collection_id=plan.collection_id) is None
    assert owner.commit_output_publication(fixture.plan).disposition is OutputOwnerPublicationDisposition.FENCED
    assert _owner_state(owner) == terminal
    _assert_metadata_only(owner)


@pytest.mark.parametrize("changed_field", ["argument", "keyword", "function", "result", "collection_id"])
def test_terminal_owner_rejects_changed_payload_or_collection_identity(changed_field):
    fixture = _Fixture()
    owner, child_owner, saved = fixture.collect_all()
    terminal = deepcopy(_owner_state(owner))
    for index, (plan, graph_release, completed) in enumerate(saved):
        metadata, membership = plan.metadata_plan, plan.membership
        changed_graph_release = graph_release
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
            payloads = list(_RESULTS)
            payloads[index] = b"altered-inline-result-payload"
            changed_publication = fixture.publication(tuple(payloads))
            with pytest.raises(OutputOwnerPublicationConflictError, match="rebound"):
                owner.commit_output_publication(changed_publication)
            manifest = changed_publication.envelope.manifest
            membership = OutputOwnerPublicationMembership(manifest, index)
            # Even a self-consistent forged result/graph pair must fail the
            # terminal identity check, not merely an unrelated graph mismatch.
            changed_graph_release = replace(graph_release, manifest=manifest.to_graph_manifest())
        else:
            metadata = replace(metadata, collection_id=f"different-gc-{index}")
            with pytest.raises(OutputOwnerPublicationConflictError, match="identity changed"):
                owner.begin_output_publication_collection(plan.object_id, collection_id=metadata.collection_id)
        changed = OutputOwnerPublicationCollectionPlan(membership, metadata)
        with pytest.raises(OutputOwnerPublicationConflictError, match="terminal identity"):
            owner.complete_output_publication_collection(changed, changed_graph_release)
        with pytest.raises(OutputOwnerPublicationConflictError, match="terminal identity"):
            owner.output_publication_collection_receipt(changed)
        assert owner.output_publication_collection_receipt(plan).collection == completed.collection
    assert _owner_state(owner) == terminal
    _assert_metadata_only(owner)
    _assert_metadata_only(child_owner)
