"""Unified recovery contracts plus the two retained opt-in race selectors.

The filename is kept for the existing bounded-runner L1 exact IDs.  It does
not instantiate an INLINE-only registry: every case uses one two-slot unified
metadata manifest.  Historical source is preserved verbatim under
docs/history/retired-protocol-family/test_inline_recovery.py.txt.

Pure tests have no threads or waits.  Each separately marked L1 starts exactly
two daemon requester threads, a one-second timed barrier, a shared two-second
join deadline and a shared one-second finally cleanup deadline.  No Core/Node
runtime, transport, timers, user code, large payload or subprocess is involved.
"""

from __future__ import annotations

import hashlib
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from dataclasses import fields, is_dataclass, replace
from enum import Enum
from types import SimpleNamespace

import pytest

from miniray import protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputSlotManifest,
)
from miniray.output_recovery import (
    OutputPublicationRecoveryAuthority, OutputRecoveryAction, OutputRecoveryConflictError,
    OutputRecoveryDisposition, OutputRecoveryOwnerDecision, OutputRecoveryStage,
    OutputRecoveryStateError, OutputSlotDecision, UnknownOutputRecoveryError,
)
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest


# Do not add a module-level unit marker: it would make real L1 races part of
# the default run.  Every test declares exactly its own safety classification.


@pytest.fixture(autouse=True)
def _runtime_tripwires(monkeypatch, request):
    def forbidden(*_args, **_kwargs):
        pytest.fail("recovery metadata test attempted runtime infrastructure")

    for owner, method in (
        (threading.Timer, "start"), (socket, "socket"),
        (socket, "create_connection"), (subprocess, "Popen"),
        (multiprocessing.process.BaseProcess, "start"), (time, "sleep"),
    ):
        monkeypatch.setattr(owner, method, forbidden)
    if request.node.get_closest_marker("unit") is not None:
        for owner, method in (
            (threading.Thread, "__init__"), (threading.Thread, "start"),
            (threading.Thread, "join"), (threading.Event, "wait"),
            (threading.Condition, "wait"),
        ):
            monkeypatch.setattr(owner, method, forbidden)


def _values():
    def identity(kind, byte):
        return kind(bytes((byte,)) * 16)

    task = identity(TaskID, 2)
    execution = TaskExecutionKey(TaskOutputManifest.for_task(task, 2), AttemptID(task, 0))
    publication_id = OutputPublicationID(identity(LeaseID, 3), execution)
    owner = identity(WorkerID, 4)
    node = OutputPublicationNodeIncarnation(identity(NodeID, 6), 601, 1)
    header = OutputPublicationHeader(
        publication_id, identity(JobID, 1), identity(WorkerID, 5), owner, node,
    )
    checksum = hashlib.sha256(b"tiny").hexdigest()
    manifest = OutputPublicationManifest.create(header, tuple(
        OutputSlotManifest(object_id, protocol.ResultStorage.INLINE, 4, checksum)
        for object_id in publication_id.output_ids
    ))
    witness = OutputPublicationCompleteWitness.for_manifest(manifest)
    node_death = protocol.NodeDeathRecord(
        "recovery-race-node-exit", node.node_id, node.node_pid, node.registration_epoch,
        9, 1, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed publisher exit",
    )
    owner_death = protocol.WorkerDeathRecord(
        "recovery-race-owner-exit", protocol.WorkerIncarnation(
            identity(NodeID, 7), 701, 2, owner, 702,
        ), 10, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    return SimpleNamespace(manifest=manifest, witness=witness, publication_id=publication_id,
                           owner=owner, node_death=node_death, owner_death=owner_death)


def _metadata(value):
    if type(value) in (JobID, TaskID, LeaseID, NodeID, WorkerID):
        assert type(value.value) is bytes and len(value.value) == 16
        return
    assert not isinstance(value, (bytes, bytearray, memoryview, protocol.ResultDescriptor, protocol.TaskSpec))
    if value is None or isinstance(value, (str, int, bool, Enum)):
        return
    if is_dataclass(value) and not isinstance(value, type):
        for member in fields(value):
            _metadata(getattr(value, member.name))
    elif isinstance(value, dict):
        for key, item in value.items():
            _metadata(key)
            _metadata(item)
    elif isinstance(value, (tuple, list, set, frozenset)):
        for item in value:
            _metadata(item)
    else:
        raise AssertionError("unexpected metadata value: " + type(value).__name__)


def _authority_metadata(authority):
    for name, value in vars(authority).items():
        if name != "_lock":
            _metadata(value)


def _race_two(first, second):
    """Exactly two genuine races; all construction/start paths own cleanup."""
    barrier = threading.Barrier(3, timeout=1.0)
    outcomes = queue.Queue()
    threads = []

    def run(index, operation):
        try:
            barrier.wait(timeout=1.0)
            outcomes.put_nowait((index, operation(), None))
        except BaseException as exc:
            outcomes.put_nowait((index, None, exc))

    try:
        for index, operation in enumerate((first, second)):
            thread = threading.Thread(
                target=run, args=(index, operation), daemon=True,
                name="miniray-unified-recovery-race-{}".format(index),
            )
            # Ledger insertion precedes attempted start, including a custom
            # Thread.start that could start successfully and then raise.
            threads.append(thread)
        deadline = time.monotonic() + 2.0
        for thread in threads:
            thread.start()
        barrier.wait(timeout=max(0.0, min(1.0, deadline - time.monotonic())))
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        assert all(not thread.is_alive() for thread in threads), "recovery race exceeded its join deadline"
        reports = [outcomes.get_nowait() for _ in range(2)]
        assert outcomes.empty()
        assert {index for index, _value, _error in reports} == {0, 1}
        results = [None, None]
        for index, value, error in reports:
            if error is not None:
                raise error
            results[index] = value
        return tuple(results)
    finally:
        barrier.abort()
        deadline = time.monotonic() + 1.0
        for thread in threads:
            if thread.ident is not None:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
        assert all(not thread.is_alive() for thread in threads), "recovery requester leaked after bounded cleanup"


@pytest.mark.unit
def test_intent_is_complete_pre_effect_fact_and_exact_replay():
    values = _values()
    authority = OutputPublicationRecoveryAuthority()
    first = authority.report_intent(values.manifest)
    replay = authority.report_intent(values.manifest)
    assert first.stage is OutputRecoveryStage.INTENT
    assert first.disposition is OutputRecoveryDisposition.APPLIED
    assert replay.disposition is OutputRecoveryDisposition.ALREADY_RECORDED
    assert replay.snapshot == first.snapshot
    assert first.snapshot.manifest == values.manifest
    assert first.snapshot.forward_allowed
    assert not first.snapshot.armed and first.snapshot.complete is None
    assert first.snapshot.recovery_action is OutputRecoveryAction.PRECOMPLETE_ROLLBACK
    assert first.snapshot.manifest.header.node_incarnation == values.manifest.header.node_incarnation
    _metadata(first)
    _authority_metadata(authority)


@pytest.mark.unit
@pytest.mark.parametrize("owner_first", (False, True))
def test_owner_and_publishing_node_freezes_are_orthogonal_and_immutable(owner_first):
    values = _values()
    authority = OutputPublicationRecoveryAuthority()
    authority.report_intent(values.manifest)
    authority.arm_complete(values.publication_id, values.manifest.manifest_digest)
    if owner_first:
        owner_work = authority.freeze_owner_death(values.owner_death)
        assert owner_work[0].snapshot.frozen_node_death is None
        node_work = authority.freeze_node_death(values.node_death)
    else:
        node_work = authority.freeze_node_death(values.node_death)
        assert node_work[0].snapshot.owner_death is None
        owner_work = authority.freeze_owner_death(values.owner_death)
    assert len(owner_work) == len(node_work) == 1
    current = authority.snapshot(values.publication_id)
    assert current.owner_death == values.owner_death
    assert current.frozen_node_death == values.node_death
    assert current.armed and current.complete is None
    assert node_work[0].action is OutputRecoveryAction.COMPLETION_UNKNOWN
    assert authority.freeze_owner_death(values.owner_death) == owner_work
    assert authority.frozen_owner_workset(values.owner_death) == owner_work
    assert authority.freeze_node_death(values.node_death) == node_work
    assert authority.frozen_workset(values.node_death) == node_work
    assert authority.report_intent(values.manifest).disposition is OutputRecoveryDisposition.FENCED
    assert authority.arm_complete(values.publication_id, values.manifest.manifest_digest).disposition is OutputRecoveryDisposition.FENCED
    assert authority.snapshot(values.publication_id) == current
    _authority_metadata(authority)


@pytest.mark.loopback_smoke
def test_owner_death_and_intent_admission_linearize_atomically():
    values = _values()
    authority = OutputPublicationRecoveryAuthority()

    def report():
        try:
            return authority.report_intent(values.manifest)
        except OutputRecoveryStateError:
            return None

    acknowledgement, frozen = _race_two(
        report, lambda: authority.freeze_owner_death(values.owner_death),
    )
    if acknowledgement is None:
        assert frozen == ()
        assert authority.publication_ids() == ()
        with pytest.raises(UnknownOutputRecoveryError):
            authority.snapshot(values.publication_id)
        with pytest.raises(OutputRecoveryStateError, match="death-frozen"):
            authority.report_intent(values.manifest)
    else:
        assert acknowledgement.disposition is OutputRecoveryDisposition.APPLIED
        assert acknowledgement.snapshot.owner_death is None
        assert len(frozen) == 1 and frozen[0].publication_id == values.publication_id
        assert frozen[0].death == values.owner_death
        assert frozen[0].snapshot == replace(acknowledgement.snapshot, owner_death=values.owner_death)
        current = authority.snapshot(values.publication_id)
        assert current == frozen[0].snapshot
        # Exact intent replay is observable but cannot authorize fresh work
        # after the owner-death fence has already won.
        assert authority.report_intent(values.manifest).disposition is OutputRecoveryDisposition.FENCED
        assert authority.arm_complete(values.publication_id, values.manifest.manifest_digest).disposition is OutputRecoveryDisposition.FENCED
    assert authority.frozen_owner_workset(values.owner_death) == frozen
    assert authority.freeze_owner_death(values.owner_death) == frozen
    _metadata((acknowledgement, frozen))
    _authority_metadata(authority)


@pytest.mark.loopback_smoke
def test_owner_keep_drop_race_has_one_winner_and_no_payload_in_authority():
    values = _values()
    authority = OutputPublicationRecoveryAuthority()
    authority.report_intent(values.manifest)
    authority.arm_complete(values.publication_id, values.manifest.manifest_digest)
    (work,) = authority.freeze_node_death(values.node_death)
    assert work.action is OutputRecoveryAction.COMPLETION_UNKNOWN
    vectors = (
        tuple(OutputSlotDecision(index, slot.object_id, OutputRecoveryOwnerDecision.KEEP)
              for index, slot in enumerate(values.manifest.slots)),
        tuple(OutputSlotDecision(index, slot.object_id, OutputRecoveryOwnerDecision.DROP)
              for index, slot in enumerate(values.manifest.slots)),
    )

    def choose(index):
        try:
            return authority.decide_owner(
                work, values.owner, vectors[index], decision_id="owner-choice",
                complete=values.witness if index == 0 else None,
            )
        except OutputRecoveryConflictError:
            return None

    results = _race_two(lambda: choose(0), lambda: choose(1))
    assert sum(result is not None for result in results) == 1
    winner = next(index for index, result in enumerate(results) if result is not None)
    accepted = results[winner]
    assert accepted.stage is OutputRecoveryStage.OWNER_DECIDED
    assert accepted.disposition is OutputRecoveryDisposition.APPLIED
    snapshot = authority.snapshot(values.publication_id)
    assert snapshot.owner_decision.slots == vectors[winner]
    assert snapshot.owner_decision.complete == (values.witness if winner == 0 else None)
    # The owner witness is not a late Node report or a payload.  Frozen UNKNOWN
    # history remains intact even when the KEEP proposal wins.
    assert snapshot.complete is None and work.snapshot.owner_decision is None
    assert authority.frozen_workset(values.node_death) == (work,)
    replay = choose(winner)
    assert replay.disposition is OutputRecoveryDisposition.ALREADY_RECORDED
    assert replay.snapshot == snapshot
    assert choose(1 - winner) is None
    assert authority.snapshot(values.publication_id) == snapshot
    _metadata((results, work, snapshot))
    _authority_metadata(authority)
