"""Pure owner retirement with exact child death or real child-table release.

One published LOST output, at most two children, one local owner authority and
one child table. Death and byte-drop receipts are explicit model inputs; they
do not claim a process exited or a physical store was deleted. No runtime,
socket, thread, queue, user function, sleep or external fixture is started.
"""

from dataclasses import replace
import hashlib

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputSlotManifest,
)
from miniray.ownership import (
    ObjectOwnerTable, ObjectState, OutputOwnerPublicationDisposition,
    OutputOwnerPublicationPlan, OutputOwnerRetirementConflictError,
)
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecutionKey

pytestmark = pytest.mark.unit


def _id(kind, value):
    return kind(bytes((value,)) * 16)


def _installed_identity(death):
    # The test independently states the full proof consumed by the owner. It
    # deliberately does not use the production retirement identity helper.
    return "worker-death:v1:{}:{}:{}:{}:{}:{}:{}:{}:{}".format(
        death.death_epoch, death.worker_id.hex, death.node_id.hex,
        death.node_pid, death.node_registration_epoch, death.worker_pid,
        death.exit_code, death.reason.value, death.detection_id,
    )


class _Fixture:
    def __init__(self):
        job, task = _id(JobID, 1), _id(TaskID, 2)
        self.owner, self.child_owner, self.node = _id(WorkerID, 3), _id(WorkerID, 4), _id(NodeID, 5)
        self.output, self.attempt = ObjectID.for_task(task), AttemptID(task, 0)
        self.spec = protocol.TaskSpec(
            job, task, self.attempt, protocol.FunctionKey(job, __name__, "fixture", "1"),
            (), 1, ResourceVector(), self.owner, max_retries=1,
        )
        execution = TaskExecutionKey.from_task_spec(self.spec)
        identity = OutputPublicationID(_id(LeaseID, 6), execution)
        self.transfers = tuple(PreparedContainedTransfer(
            ObjectID.for_task(_id(TaskID, number)), self.child_owner, ("child.invalid", 1234),
            OwnedContainedSource(self.child_owner),
            ContainedReferenceHold(self.output, self.child_owner, "hold:" + str(number)),
            ContainedReferenceHold(self.output, self.owner, "hold:" + str(number)),
        ) for number in (7, 8))
        payload = b"published result"
        checksum = hashlib.sha256(payload).hexdigest()
        manifest = OutputPublicationManifest.create(
            OutputPublicationHeader(identity, job, self.child_owner, self.owner,
                                    OutputPublicationNodeIncarnation(self.node, 1001, 2)),
            (OutputSlotManifest(self.output, protocol.ResultStorage.OBJECT_STORE,
                                len(payload), checksum, self.transfers),),
        )
        descriptor = protocol.ResultDescriptor(self.output, protocol.ResultStorage.OBJECT_STORE,
            len(payload), self.owner, self.node, checksum)
        envelope = OutputPublicationEnvelope(manifest, OutputPublicationCompleteWitness.for_manifest(manifest), (descriptor,))
        self.owner_table = ObjectOwnerTable()
        self.owner_table.register_task_outputs(self.spec, local_tokens=("live-output",))
        self.owner_table.commit_output_publication(OutputOwnerPublicationPlan(execution, envelope))
        self.owner_table.mark_lost(self.output, self.attempt)
        member = self.owner_table.output_owner_publication(self.output)
        self.plan = self.owner_table.begin_output_publication_retirement(
            (member,), retirement_id="retire:old-result", replica_locations={self.output: (self.node,)},
        )
        request, = self.plan.replica_drops
        self.drops = (protocol.DropObjectReplicaReply(
            request.object_id, request.producer_attempt_id, request.owner_worker_id,
            request.node_id, request.checksum, protocol.DropObjectReplicaStatus.DROPPED,
        ),)
        self.death = protocol.WorkerDeathRecord(
            "child-exited", protocol.WorkerIncarnation(self.node, 1001, 2, self.child_owner, 1002),
            3, 7, protocol.WorkerDeathReason.PROCESS_EXIT,
        )
        self.child_table = ObjectOwnerTable()
        for transfer in self.transfers:
            self.child_table.register(transfer.contained_object_id, local_token="child-live")
            self.child_table.prepare_stored_contained_reference(transfer, authority_worker_id=self.child_owner)
            self.child_table.promote_stored_contained_reference(transfer, authority_worker_id=self.child_owner)

    def install(self, death=None, identity=None):
        death = self.death if death is None else death
        return self.owner_table.install_dead_worker(
            death.worker_id, _installed_identity(death) if identity is None else identity,
        )

    def release(self, index):
        request = self.plan.contained_releases[index]
        released = self.child_table.release_contained_reference(request.object_id, request.hold)
        return protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, released,
        )

    def retire(self, proofs):
        return self.owner_table.complete_output_publication_retirement(
            self.plan, released_edges=proofs, dropped_replicas=self.drops,
        )


def test_mixed_release_and_exact_installed_child_death_retire_without_fabricated_ack():
    f = _Fixture()
    released = f.release(0)
    f.install()
    before = f.owner_table.snapshot(f.output)
    receipt = f.retire((released, f.death))
    assert receipt.disposition is OutputOwnerPublicationDisposition.APPLIED
    assert receipt.released_edges == (released, f.death)
    assert type(receipt.released_edges[1]) is protocol.WorkerDeathRecord
    after = f.owner_table.snapshot(f.output)
    assert after.state is ObjectState.LOST and after.current_attempt == f.attempt
    assert after.local_tokens == before.local_tokens and after.producer_task_spec == f.spec
    assert after.output_publication is None and not after.outgoing_contained_edges
    assert not f.owner_table.has_active_output_retirements()
    # Death settlement never invokes Release against an absent remote owner.
    assert f.transfers[1].final_hold in f.child_table.snapshot(f.transfers[1].contained_object_id).contained_holds
    assert f.retire((released, f.death)).disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED
    assert f.owner_table.snapshot(f.output) == after


def test_one_actual_owner_death_can_cover_each_of_its_distinct_final_holds():
    f = _Fixture()
    f.install()
    receipt = f.retire((f.death, f.death))
    assert receipt.released_edges == (f.death, f.death)
    assert not f.owner_table.has_active_output_retirements()


@pytest.mark.parametrize("bad", ("missing", "wrong-installed", "other-owner", "expected-exit", "changed-incarnation"))
def test_unproven_or_wrong_child_death_never_partially_retires(bad):
    f = _Fixture()
    proof = f.death
    if bad == "wrong-installed":
        f.install(identity="some-other-death")
    elif bad != "missing":
        f.install()
    if bad == "other-owner":
        proof = replace(proof, incarnation=replace(proof.incarnation, worker_id=_id(WorkerID, 9)))
    elif bad == "expected-exit":
        proof = replace(proof, reason=protocol.WorkerDeathReason.EXPECTED)
    elif bad == "changed-incarnation":
        proof = replace(proof, incarnation=replace(proof.incarnation, worker_pid=1003))
    before = f.owner_table.snapshot(f.output)
    with pytest.raises(OutputOwnerRetirementConflictError):
        f.retire((f.release(0), proof))
    assert f.owner_table.snapshot(f.output) == before
    assert f.owner_table.has_active_output_retirements()
    assert f.owner_table.output_publication_retirement_receipt(f.plan) is None


def test_death_coverage_is_not_a_reason_to_skip_an_unsettled_final_hold():
    f = _Fixture()
    f.install()
    before = f.owner_table.snapshot(f.output)
    with pytest.raises(OutputOwnerRetirementConflictError, match="every child"):
        f.retire((f.death,))
    assert f.owner_table.snapshot(f.output) == before


def test_death_receipt_replay_cannot_change_a_proof_after_retirement():
    f = _Fixture()
    f.install()
    receipt = f.retire((f.death, f.death))
    before = f.owner_table.snapshot(f.output)
    with pytest.raises(OutputOwnerRetirementConflictError):
        f.retire((f.death, replace(f.death, detection_id="changed-late-proof")))
    assert f.owner_table.output_publication_retirement_receipt(f.plan) == replace(
        receipt, disposition=OutputOwnerPublicationDisposition.ALREADY_APPLIED,
    )
    assert f.owner_table.snapshot(f.output) == before


def test_mutating_nested_death_input_cannot_change_retained_receipt():
    f = _Fixture()
    f.install()
    original = f.death
    supplied = replace(original, incarnation=replace(original.incarnation))
    receipt = f.retire((supplied, supplied))
    object.__setattr__(supplied.incarnation, "worker_pid", 9999)
    assert receipt.released_edges == (original, original)
    assert f.owner_table.output_publication_retirement_receipt(f.plan) == replace(
        receipt, disposition=OutputOwnerPublicationDisposition.ALREADY_APPLIED,
    )
