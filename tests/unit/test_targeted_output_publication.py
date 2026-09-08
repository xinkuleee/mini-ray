"""Focused, resource-reviewed coordinator regressions for unified publication.

Every case uses one three-output task, at most two reconstruction STARTs, and
five-byte payloads. All callbacks are synchronous in-memory fakes; the only
locks are uncontended model RLocks. No runtime, object store, filesystem I/O,
producer execution, network, thread, process, timer, or polling loop is needed.
The autouse guard fails immediately if a test attempts blocking/runtime work.
"""

from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputSlotManifest,
)
from miniray.output_recovery import OutputRecoveryResolution
from miniray.ownership import (
    ObjectOwnerTable, ObjectState, OutputOwnerPublicationDisposition,
)
from miniray.recovery import RecoveryManager, TaskState
from miniray.resources import ResourceVector
from miniray.targeted_reconstruction import (
    TargetedReconstructionCoordinator, TargetedReconstructionError,
    TargetedRequestDisposition, TargetedSessionPhase,
)
from miniray.task_outputs import TaskExecutionKey


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure targeted publication test attempted runtime work")

    for kind, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (socket, "socket"),
        (socket, "create_connection"), (subprocess, "Popen"),
        (time, "sleep"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    if hasattr(os, "fork"):
        monkeypatch.setattr(os, "fork", forbidden)


def _id(kind, byte: int):
    return kind(bytes((byte,)) * 16)


class _Fixture:
    def __init__(self) -> None:
        job = _id(JobID, 0xd1)
        task = TaskID.derive(job, TaskID.for_driver(job), 19)
        self.spec = protocol.TaskSpec(
            job, task, AttemptID(task, 0),
            protocol.FunctionKey(job, __name__, "producer", "v1"),
            (), 3, ResourceVector(), _id(WorkerID, 0xd2), max_retries=4,
        )
        self.outputs = self.spec.return_ids()
        self.owner = ObjectOwnerTable()
        self.recovery = RecoveryManager()
        self.owner.register_task_outputs(self.spec)
        self.recovery.register_task(self.spec, max_retries=4)
        results = tuple(
            protocol.ResultDescriptor(
                output, protocol.ResultStorage.OBJECT_STORE, len(payload),
                self.spec.owner_worker_id, _id(NodeID, 0xe1 + index),
                hashlib.sha256(payload).hexdigest(),
            )
            for index, (output, payload) in enumerate(
                zip(self.outputs, (b"old-0", b"old-1", b"old-2"))
            )
        )
        assert self.owner.publish_task_outputs(
            TaskExecutionKey.from_task_spec(self.spec), results
        )
        self.recovery.record_task_success(task, self.spec.attempt_id)
        self.coordinator = TargetedReconstructionCoordinator(
            self.recovery, self.owner
        )

    def start(self):
        for output in (self.outputs[0], self.outputs[2]):
            assert self.owner.mark_lost(output, self.spec.attempt_id)
            self.coordinator.request(output, self.spec.attempt_id)
        return self.coordinator.start(self.spec.task_id)

    def owner_snapshot(self):
        return tuple(self.owner.snapshot(output) for output in self.outputs)

    def recovery_snapshot(self):
        record = self.recovery.task_record(self.spec.task_id)
        return (
            record.current_attempt, record.retries_started,
            record.retries_remaining, record.state, record.last_error,
            self.recovery.active_recovery(self.spec.task_id),
        )


def _envelope(values, session) -> OutputPublicationEnvelope:
    identity = OutputPublicationID(_id(LeaseID, 0xd3), session.execution)
    header = OutputPublicationHeader(
        identity, values.spec.job_id, _id(WorkerID, 0xd4),
        values.spec.owner_worker_id,
        OutputPublicationNodeIncarnation(_id(NodeID, 0xf1), 1701, 2),
    )
    results = tuple(
        protocol.ResultDescriptor(
            output, protocol.ResultStorage.INLINE if index == 0
            else protocol.ResultStorage.OBJECT_STORE, len(payload),
            values.spec.owner_worker_id, header.node_incarnation.node_id,
            hashlib.sha256(payload).hexdigest(), payload if index == 0 else None,
        )
        for index, (output, payload) in enumerate(
            zip(session.target_output_ids, (b"new-0", b"new-2"))
        )
    )
    manifest = OutputPublicationManifest.create(header, tuple(
        OutputSlotManifest(
            result.object_id, result.storage, result.size_bytes, result.checksum
        )
        for result in results
    ))
    return OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest), results
    )


def _resolve_known_loss(values, envelope) -> None:
    node = envelope.manifest.header.node_incarnation
    # This is a typed, already-acknowledged cleanup fact, not a real Node exit
    # or a call into the GCS/runtime cleanup driver. Only slot zero is retained.
    resolution = OutputRecoveryResolution(
        envelope.publication_id, envelope.manifest.manifest_digest,
        protocol.NodeDeathRecord(
            "targeted-publisher-exit", node.node_id, node.node_pid,
            node.registration_epoch, 1, 1, protocol.NodeDeathReason.PROCESS_EXIT,
            "metadata-only observed exit",
        ),
        values.spec.owner_worker_id, "targeted-cleanup-ack",
        kept_slots=(0,), complete=envelope.complete,
    )
    assert values.owner.resolve_output_node_loss(
        envelope.manifest, resolution, envelope
    )


def test_unified_targeted_success_preflight_and_commit_preserve_sibling() -> None:
    values = _Fixture()
    healthy = values.owner.snapshot(values.outputs[1])
    session = values.start()
    envelope = _envelope(values, session)
    before_owner = values.owner_snapshot()
    before_recovery = values.recovery_snapshot()

    plan = values.coordinator.validate_output_publication_success(envelope)

    assert plan.session == session
    assert values.owner_snapshot() == before_owner
    assert values.recovery_snapshot() == before_recovery
    assert values.coordinator.commit_output_publication_success(plan) is None
    assert values.owner.snapshot(values.outputs[1]) == healthy
    assert values.owner.snapshot(values.outputs[0]).state is ObjectState.READY_INLINE
    assert values.owner.snapshot(values.outputs[0]).inline_data == b"new-0"
    stored = values.owner.snapshot(values.outputs[2])
    assert stored.state is ObjectState.READY_STORED
    assert stored.canonical_stored_result == envelope.results[1]
    assert stored.current_attempt == session.execution.attempt_id
    assert values.recovery.task_record(values.spec.task_id).state is TaskState.SUCCEEDED
    assert values.recovery.task_record(values.spec.task_id).retries_started == 1
    assert values.recovery.active_recovery(values.spec.task_id) is None
    assert values.coordinator.current_session(values.spec.task_id) is None


@pytest.mark.parametrize(
    "success_recorded", (False, True),
    ids=("repair-success", "success-recorded"),
)
def test_known_lost_complete_closes_session_without_republishing(
    monkeypatch: pytest.MonkeyPatch, success_recorded: bool,
) -> None:
    values = _Fixture()
    session = values.start()
    envelope = _envelope(values, session)
    attempt = session.execution.attempt_id
    _resolve_known_loss(values, envelope)
    if success_recorded:
        values.recovery.record_task_success(values.spec.task_id, attempt)
    before_owner = values.owner_snapshot()

    def forbidden_publication(*_args, **_kwargs):
        pytest.fail("known Complete must retain the exact owner KEEP/LOST vector")

    monkeypatch.setattr(values.owner, "commit_output_publication", forbidden_publication)
    monkeypatch.setattr(values.owner, "commit_publish_target_outputs", forbidden_publication)

    assert values.coordinator.complete_lost_output_publication(
        values.spec.task_id, attempt
    ) is None
    assert values.coordinator.current_session(values.spec.task_id) is None
    assert values.coordinator.active_task_ids() == ()
    record = values.recovery.task_record(values.spec.task_id)
    assert record.current_attempt == attempt
    assert record.state is TaskState.SUCCEEDED
    assert record.retries_started == 1
    assert values.recovery.active_recovery(values.spec.task_id) is None
    assert values.owner_snapshot() == before_owner
    assert before_owner[0].state is ObjectState.READY_INLINE
    assert before_owner[0].inline_data == b"new-0"
    assert before_owner[2].state is ObjectState.LOST
    assert before_owner[2].locations == frozenset()
    assert before_owner[2].canonical_stored_result is None
    before_replay = values.recovery_snapshot()
    assert values.coordinator.complete_lost_output_publication(
        values.spec.task_id, attempt
    ) is None
    assert values.owner_snapshot() == before_owner
    assert values.recovery_snapshot() == before_replay


def test_known_lost_complete_rewakes_open_successor_but_fences_started_successor() -> None:
    values = _Fixture()
    session = values.start()
    envelope = _envelope(values, session)
    attempt = session.execution.attempt_id
    _resolve_known_loss(values, envelope)
    assert values.owner.mark_lost(values.outputs[1], values.spec.attempt_id)
    for output, expected in (
        (values.outputs[2], attempt),
        (values.outputs[1], values.spec.attempt_id),
    ):
        request = values.coordinator.request(output, expected)
        assert request.disposition is TargetedRequestDisposition.QUEUED_NEXT

    promoted = values.coordinator.complete_lost_output_publication(
        values.spec.task_id, attempt
    )

    assert promoted is not None
    assert promoted.phase is TargetedSessionPhase.OPEN
    assert promoted.target_output_ids == (values.outputs[1], values.outputs[2])
    assert promoted.expected_attempts == {
        values.outputs[1]: values.spec.attempt_id, values.outputs[2]: attempt,
    }
    before_owner = values.owner_snapshot()
    before_recovery = values.recovery_snapshot()
    for _ in range(2):
        assert values.coordinator.complete_lost_output_publication(
            values.spec.task_id, attempt
        ) is promoted
        assert values.owner_snapshot() == before_owner
        assert values.recovery_snapshot() == before_recovery
        assert values.coordinator.queued_losses(values.spec.task_id) == ()

    successor = values.coordinator.start(values.spec.task_id)
    assert successor.execution.attempt_id == AttemptID(values.spec.task_id, 2)
    assert values.recovery.task_record(values.spec.task_id).retries_started == 2
    assert values.owner.snapshot(values.outputs[0]) == before_owner[0]
    before_owner = values.owner_snapshot()
    before_recovery = values.recovery_snapshot()
    for _ in range(2):
        assert values.coordinator.complete_lost_output_publication(
            values.spec.task_id, attempt
        ) is None
        assert values.coordinator.current_session(values.spec.task_id) is successor
        assert values.owner_snapshot() == before_owner
        assert values.recovery_snapshot() == before_recovery
    assert values.recovery.active_recovery(values.spec.task_id) == successor.execution.attempt_id


@pytest.mark.parametrize(
    "repair", ("same-plan", "revalidated-plan", "receipt-completion"),
)
def test_owner_cas_effect_then_error_replay_preserves_new_session(
    monkeypatch: pytest.MonkeyPatch, repair: str,
) -> None:
    values = _Fixture()
    session = values.start()
    envelope = _envelope(values, session)
    attempt = session.execution.attempt_id
    assert values.owner.mark_lost(values.outputs[1], values.spec.attempt_id)
    assert values.coordinator.request(
        values.outputs[1], values.spec.attempt_id
    ).disposition is TargetedRequestDisposition.QUEUED_NEXT
    plan = values.coordinator.validate_output_publication_success(envelope)
    before_recovery = values.recovery_snapshot()
    commit = values.owner.commit_output_publication
    commits = []

    def effect_then_error(owner_plan):
        receipt = commit(owner_plan)
        commits.append(receipt.disposition)
        if len(commits) == 1:
            raise RuntimeError("owner CAS reply lost after effect")
        return receipt

    monkeypatch.setattr(values.owner, "commit_output_publication", effect_then_error)
    with pytest.raises(RuntimeError, match="reply lost after effect"):
        values.coordinator.commit_output_publication_success(plan)

    assert commits == [OutputOwnerPublicationDisposition.APPLIED]
    assert values.recovery_snapshot() == before_recovery
    assert values.coordinator.current_session(values.spec.task_id) is session
    published = values.owner_snapshot()
    assert published[0].state is ObjectState.READY_INLINE
    assert published[2].state is ObjectState.READY_STORED
    receipt = values.owner.output_owner_publication_receipt(plan.owner_plan)
    assert receipt is not None and receipt.committed
    assert receipt.disposition is OutputOwnerPublicationDisposition.ALREADY_APPLIED

    if repair == "receipt-completion":
        promoted = values.coordinator.complete_lost_output_publication(
            values.spec.task_id, attempt
        )
        assert commits == [OutputOwnerPublicationDisposition.APPLIED]
    else:
        replay = (plan if repair == "same-plan" else
                  values.coordinator.validate_output_publication_success(envelope))
        promoted = values.coordinator.commit_output_publication_success(replay)
        assert commits == [
            OutputOwnerPublicationDisposition.APPLIED,
            OutputOwnerPublicationDisposition.ALREADY_APPLIED,
        ]
    assert values.owner_snapshot() == published
    assert promoted is not None
    assert promoted.phase is TargetedSessionPhase.OPEN
    assert promoted.target_output_ids == (values.outputs[1],)
    assert values.recovery.task_record(values.spec.task_id).state is TaskState.SUCCEEDED
    assert values.recovery.task_record(values.spec.task_id).retries_started == 1
    assert values.coordinator.complete_lost_output_publication(
        values.spec.task_id, attempt
    ) is promoted

    successor = values.coordinator.start(values.spec.task_id)
    assert successor.execution.attempt_id == AttemptID(values.spec.task_id, 2)
    assert values.owner.snapshot(values.outputs[0]) == published[0]
    assert values.owner.snapshot(values.outputs[2]) == published[2]
    before_owner = values.owner_snapshot()
    before_recovery = values.recovery_snapshot()
    before_commits = tuple(commits)
    # Old selected slots still prove a committed CAS. That receipt cannot
    # authorize task-level success or close the now-active sibling execution.
    assert values.owner.output_owner_publication_receipt(plan.owner_plan).committed
    for _ in range(2):
        with pytest.raises(TargetedReconstructionError, match="no matching"):
            values.coordinator.validate_output_publication_success(envelope)
        with pytest.raises(TargetedReconstructionError, match="no matching"):
            values.coordinator.commit_output_publication_success(plan)
        assert values.coordinator.complete_lost_output_publication(
            values.spec.task_id, attempt
        ) is None
        assert values.coordinator.current_session(values.spec.task_id) is successor
        assert values.coordinator.queued_losses(values.spec.task_id) == ()
        assert values.owner_snapshot() == before_owner
        assert values.recovery_snapshot() == before_recovery
        assert tuple(commits) == before_commits
    assert values.recovery.task_record(values.spec.task_id).state is TaskState.RETRY_PENDING
    assert values.recovery.task_record(values.spec.task_id).retries_started == 2
    assert values.recovery.active_recovery(values.spec.task_id) == successor.execution.attempt_id
