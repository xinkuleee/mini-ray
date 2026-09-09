"""Pure owner preflight for an unreceived output after publisher Node loss.

One output, one exact child identity, and one task-lineage edge. No
Core/Node/Worker, transport, thread, timer, process or actual wait is started.
The owner reducer consumes an exact metadata resolution; it performs no cleanup.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold, LineageReferenceEdge
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.output_handoff import NodeLostOutputResolution
from miniray.ownership import ObjectState, OutputOwnerPublicationConflictError
from tests.unit.test_output_owner_publication import _Fixture, _assert_metadata_only


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure owner resolution attempted runtime infrastructure")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait"),
                         (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _case(*, known):
    # One current output; explicit cleanup receipts are boundary input facts,
    # not claims that this local owner reducer performed remote releases.
    values = _Fixture()
    owner = values.table()
    for index, output in enumerate(values.publication_id.output_ids):
        incoming = ContainedReferenceHold(
            ObjectID.for_task(TaskID(bytes((90 + index,)) * 16)),
            WorkerID(bytes((80 + index,)) * 16), "incoming-live",
        )
        assert owner.add_contained_reference(output, incoming)
        assert owner.add_lineage_reference(output, "incoming-lineage-{}".format(index))
    first = values.publication_id.output_ids[0]
    owner.add_outgoing_lineage_edge(first, LineageReferenceEdge(first, values.child, "producer-input"))
    node = values.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "owner-preflight-publisher-exit", node.node_id, node.node_pid, node.registration_epoch,
        1, -9, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed publisher exit",
    )
    cleanup = tuple(protocol.ReleaseContainedReferenceReply(
        transfer.contained_object_id, transfer.contained_owner_worker_id, hold, True, False,
    ) for transfer in values.manifest.slots[0].transfers
      for hold in (transfer.final_hold, transfer.provisional_hold))
    resolution = NodeLostOutputResolution(
        values.publication_id, values.manifest.manifest_digest, values.owner, death,
        complete=values.envelope.complete if known else None, keep=False, cleanup=cleanup,
    )
    return values, owner, resolution


def _snapshots(owner, values):
    return tuple(owner.snapshot(output) for output in values.publication_id.output_ids)


def _metadata_history(owner):
    return deepcopy((
        owner._output_publication_receipts, owner._output_loss_receipts,
        owner._retired_output_slots, owner._retired_output_attempts, owner._task_lineage,
    ))


@pytest.mark.parametrize("known", (False, True), ids=("unknown", "known-complete"))
def test_pristine_unreceived_output_preserves_incoming_holds_and_canonical_lineage(known):
    values, owner, resolution = _case(known=known)
    before = _snapshots(owner, values)
    lineage = deepcopy(owner._task_lineage)
    assert all(snapshot.state is ObjectState.PENDING and snapshot.output_publication is None
               for snapshot in before)
    assert owner.resolve_output_node_loss(values.manifest, resolution)
    after = _snapshots(owner, values)
    state = ObjectState.LOST if known else ObjectState.PENDING
    assert after == tuple(replace(snapshot, state=state) for snapshot in before)
    assert owner._task_lineage == lineage
    assert all(snapshot.producer_task_spec == values.spec and snapshot.current_attempt == values.attempt
               and snapshot.is_live and snapshot.inline_data is None
               and snapshot.canonical_stored_result is None and not snapshot.locations
               and not snapshot.outgoing_contained_edges for snapshot in after)
    expected_slots = {(values.publication_id, output) for output in values.publication_id.output_ids}
    expected_attempts = {(output, values.attempt) for output in values.publication_id.output_ids} if known else set()
    assert owner._retired_output_slots == expected_slots
    assert owner._retired_output_attempts == expected_attempts
    # UNKNOWN retains its exact loss/cleanup history without asserting that
    # publication succeeded. Only an actual Complete may retain that receipt.
    assert owner._output_publication_receipts == (
        {values.publication_id: values.manifest} if known else {}
    )
    assert owner._output_loss_receipts == {values.publication_id: resolution}
    history = _metadata_history(owner)
    assert not owner.resolve_output_node_loss(values.manifest, resolution)
    assert _snapshots(owner, values) == after and _metadata_history(owner) == history
    _assert_metadata_only(owner._output_publication_receipts)
    _assert_metadata_only(owner._output_loss_receipts)


@pytest.mark.parametrize("known", (False, True), ids=("unknown", "known-complete"))
@pytest.mark.parametrize("corruption", (
    "inline_data", "error", "canonical_stored_result", "location_attempts",
    "outgoing_contained_edges", "missing-lineage", "foreign-owner-lineage",
    "foreign-job-lineage", "foreign-task-lineage",
    "lost-state", "ready-state", "next-attempt",
))
def test_bad_unreceived_output_cannot_erase_metadata_or_install_resolution(known, corruption):
    values, owner, resolution = _case(known=known)
    second = values.publication_id.output_ids[0]
    entry = owner._entries[second]
    assert entry.output_publication is None and entry.state is ObjectState.PENDING
    if corruption == "inline_data":
        entry.inline_data = b"already-received"
    elif corruption == "error":
        entry.error = "already-recorded-error"
    elif corruption == "canonical_stored_result":
        entry.canonical_stored_result = replace(values.envelope.results[0], storage=protocol.ResultStorage.OBJECT_STORE, inline_data=None)
    elif corruption == "location_attempts":
        entry.location_attempts[values.node] = values.attempt
    elif corruption == "outgoing_contained_edges":
        entry.outgoing_contained_edges.add(values.manifest.slots[0].edges[0])
    elif corruption == "missing-lineage":
        entry.producer_task_spec = None
    elif corruption == "foreign-owner-lineage":
        entry.producer_task_spec = replace(values.spec, owner_worker_id=WorkerID.random())
    elif corruption == "foreign-job-lineage":
        other_job = JobID.random()
        entry.producer_task_spec = replace(values.spec, job_id=other_job, function=replace(values.spec.function, job_id=other_job))
    elif corruption == "foreign-task-lineage":
        task = TaskID.random()
        entry.producer_task_spec = replace(values.spec, task_id=task, attempt_id=AttemptID(task, 0))
    elif corruption == "lost-state":
        entry.state = ObjectState.LOST
    elif corruption == "ready-state":
        entry.state = ObjectState.READY_INLINE
        entry.inline_data = b"another-completed-result"
    else:
        entry.current_attempt = values.attempt.next()
    before = _snapshots(owner, values)
    history = _metadata_history(owner)
    assert before[0].output_publication is None
    with pytest.raises(OutputOwnerPublicationConflictError):
        owner.resolve_output_node_loss(values.manifest, resolution)
    assert _snapshots(owner, values) == before
    assert _metadata_history(owner) == history
    assert owner._output_publication_receipts == owner._output_loss_receipts == {}
    assert not owner._retired_output_slots and not owner._retired_output_attempts
