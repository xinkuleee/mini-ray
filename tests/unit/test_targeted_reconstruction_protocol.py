"""Pure wire and Node outcome contracts for targeted multi-return execution.

Successful outcomes use real unified Prepare/ARM/Complete with a synchronous
in-memory recovery authority. Each case has at most three tiny local replicas
in a 1 KiB store; the Worker liveness observer is a plain fake object. No Core
or Node constructor, socket, background thread, process, or real wait runs.
Descriptor-only success remains an explicitly rejected historical fixture;
SYSTEM_ERROR partial-seal evidence still uses the ordinary orphan projection.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer, _LeaseOutcome, _LeaseRecord, _WorkerSlot
from miniray.object_store import ObjectStore
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope, OutputPublicationID,
    OutputPublicationManifest, OutputSlotManifest,
)
from miniray.resources import AllocationToken, ResourceLedger, ResourceVector
from miniray.task_outputs import TargetExecutionKey
from tests.unit.test_output_publication_node_server import _node as _publication_node


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("targeted wire contract attempted runtime infrastructure")

    for kind, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _Process:
    def __init__(self, alive: bool = True) -> None:
        self.alive = alive

    def is_alive(self) -> bool:
        return self.alive


def _identity():
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 5)
    attempt = AttemptID(task, 1)
    spec = protocol.TaskSpec(
        job, task, attempt, protocol.FunctionKey(job, __name__, "f", "v1"),
        (), 3, ResourceVector({"CPU": 1}), WorkerID.random(), max_retries=2,
    )
    outputs = spec.return_ids()
    target = TargetExecutionKey.from_task_spec(
        spec, (outputs[0], outputs[2])
    )
    return spec, target


def _lease(spec, target):
    node = NodeID.random()
    request = protocol.RequestWorkerLease(
        LeaseID.random(), spec.task_id, spec.attempt_id, spec.resources, node,
        spec.owner_worker_id, target_node_id=node,
        return_ids=target.target_output_ids, target_execution=target,
    )
    grant = protocol.GrantWorkerLease(
        request.lease_id, spec.task_id, spec.attempt_id, node, WorkerID.random(),
        ("127.0.0.1", 21001), AllocationToken("target"),
        target_execution=target,
    )
    return request, grant


def _terminal_outcome_fixture(
    status: protocol.TaskReplyStatus,
    sealed_indices: tuple[int, ...],
):
    spec, target = _identity()
    request, grant = _lease(spec, target)
    process = _Process(False)
    completion = protocol.CompleteWorkerLease(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        status, target_execution=target,
    )
    node = object.__new__(NodeServer)
    node.node_id = grant.node_id
    node.worker_id = grant.worker_id
    node._worker_order = (grant.worker_id,)
    node._workers = {grant.worker_id: _WorkerSlot(
        grant.worker_id, process=process, address=grant.worker_address,
        active_lease_id=None,
    )}
    node._legacy_worker_compat = False
    node._state_lock = threading.RLock()
    node._leases = {request.lease_id: _LeaseRecord(
        request, grant.allocation_token, grant,
        state=protocol.LeaseExecutionState.COMPLETED, completion=completion,
    )}
    node._sealed_metadata = {}
    node._object_store = ObjectStore(1024)
    sealed_ids = tuple(spec.return_ids()[index] for index in sealed_indices)
    for index, object_id in enumerate(sealed_ids):
        payload = b"sealed-" + bytes([index])
        node._object_store.put(object_id, payload)
        node._sealed_metadata[object_id] = (
            spec.attempt_id, spec.owner_worker_id, len(payload),
            hashlib.sha256(payload).hexdigest(),
        )
    query = protocol.GetWorkerLeaseOutcome(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        spec.owner_worker_id, target.target_output_ids, target_execution=target,
    )
    return spec, target, node, query


def _successful_outcome_fixture(stored_indices: tuple[int, ...]):
    """Complete only selected slots 0/2; retain an unrelated healthy slot 1."""
    fixture, node, record, complete = _publication_node(refs=False)
    header = fixture.manifest.header
    old_identity = header.publication_id
    spec = protocol.TaskSpec(
        header.job_id, old_identity.task_id, old_identity.attempt_id,
        protocol.FunctionKey(header.job_id, __name__, "f", "v1"), (), 3,
        ResourceVector({"CPU": 1}), header.owner_worker_id, max_retries=2,
    )
    outputs = spec.return_ids()
    target = TargetExecutionKey.from_task_spec(spec, (outputs[0], outputs[2]))
    assert set(stored_indices).issubset((0, 2))
    identity = OutputPublicationID(record.request.lease_id, target)
    header = replace(header, publication_id=identity)
    payloads = (b"selected-zero", b"selected-two")
    slots = tuple(OutputSlotManifest(
        output, protocol.ResultStorage.OBJECT_STORE if output.return_index in stored_indices
        else protocol.ResultStorage.INLINE, len(payload), hashlib.sha256(payload).hexdigest(),
    ) for output, payload in zip(target.target_output_ids, payloads))
    manifest = OutputPublicationManifest.create(header, slots)
    record.request = replace(record.request, return_ids=target.target_output_ids, target_execution=target)
    record.grant = replace(record.grant, target_execution=target)
    complete = replace(complete, target_execution=target)
    request, grant = record.request, record.grant
    node._lease_outcomes = {request.lease_id: _LeaseOutcome(request, grant)}
    process = _Process()
    node._workers[grant.worker_id].process = process
    healthy = outputs[1]
    healthy_data = b"healthy-one"
    healthy_seal = protocol.SealObject.from_data(
        healthy, spec.attempt_id, spec.owner_worker_id, healthy_data,
    )
    assert node._handle_seal_object(healthy_seal).sealed
    healthy_before = (node._object_store.snapshot(healthy), node._object_store.get(healthy), node._sealed_metadata[healthy])
    prepared = node._handle_prepare_output_publication(wire.PrepareOutputPublication(manifest, payloads))
    assert prepared.accepted and record.output_publication_id == identity
    assert fixture.recovery.snapshot(identity).armed
    assert fixture.journal.snapshot(identity).ready_to_complete
    assert fixture.ledger.available == ResourceVector.empty()
    completed = node._handle_complete_worker_lease(complete)
    assert completed.accepted and completed.released
    assert record.state is protocol.LeaseExecutionState.COMPLETED and record.completion == complete
    assert fixture.ledger.available == fixture.ledger.total
    expected = OutputPublicationEnvelope(manifest, OutputPublicationCompleteWitness.for_manifest(manifest), tuple(
        protocol.ResultDescriptor(slot.object_id, slot.tier, slot.size_bytes, spec.owner_worker_id,
            node.node_id, slot.checksum, payload if slot.tier is protocol.ResultStorage.INLINE else None)
        for slot, payload in zip(slots, payloads)
    ))
    assert completed.output_publication == expected
    assert fixture.journal.snapshot(identity).complete == expected.complete
    assert fixture.recovery.snapshot(identity).complete is None  # Local Complete is not terminal delivery.
    assert (node._object_store.snapshot(healthy), node._object_store.get(healthy), node._sealed_metadata[healthy]) == healthy_before
    process.alive = False  # A later Worker exit cannot erase the retained envelope.
    query = protocol.GetWorkerLeaseOutcome(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id, spec.owner_worker_id,
        target.target_output_ids, target_execution=target,
    )
    return spec, target, node, query, expected, healthy_before


def _assert_successful_target_outcome(spec, target, node, query, expected, healthy_before):
    outcome = node._handle_get_worker_lease_outcome(query)
    assert outcome.found and not outcome.worker_alive and not outcome.cleanup_pending
    assert outcome.state is protocol.LeaseExecutionState.COMPLETED
    assert outcome.completion_status is protocol.TaskReplyStatus.SUCCEEDED
    assert outcome.target_execution == target and outcome.object_ids == target.target_output_ids
    assert outcome.output_publication == expected and outcome.output_completion is None
    assert tuple(result.object_id.return_index for result in outcome.output_publication.results) == (0, 2)
    assert outcome.output_publication.publication_id.full_output_ids == spec.return_ids()
    assert outcome.orphan_descriptors == ()
    assert tuple(value.object_id for value in outcome.descriptors) == tuple(
        value.object_id for value in expected.results if value.storage is protocol.ResultStorage.OBJECT_STORE
    )
    healthy = spec.return_ids()[1]
    assert (node._object_store.snapshot(healthy), node._object_store.get(healthy), node._sealed_metadata[healthy]) == healthy_before
    assert node._handle_get_worker_lease_outcome(query) == outcome
    return outcome


def test_noncontiguous_target_identity_crosses_every_task_lease_message() -> None:
    spec, target = _identity()
    request, grant = _lease(spec, target)
    push = protocol.PushTask(
        request.lease_id, grant.worker_id, spec, target_execution=target
    )
    start = protocol.StartWorkerLease(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        target_execution=target,
    )
    complete = protocol.CompleteWorkerLease(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        protocol.TaskReplyStatus.SUCCEEDED, target_execution=target,
    )
    query = protocol.GetWorkerLeaseOutcome(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        spec.owner_worker_id, target.target_output_ids, target_execution=target,
    )
    payloads = (b"zero", b"two")
    results = tuple(
        protocol.ResultDescriptor(
            object_id, protocol.ResultStorage.INLINE, len(payload),
            spec.owner_worker_id, request.requester_node_id,
            hashlib.sha256(payload).hexdigest(), payload,
        )
        for object_id, payload in zip(target.target_output_ids, payloads)
    )
    reply = protocol.TaskReply(
        spec.task_id, spec.attempt_id, grant.worker_id,
        protocol.TaskReplyStatus.SUCCEEDED, results,
        target_execution=target,
    )

    assert request.return_ids == target.target_output_ids
    assert all(
        value.target_execution == target
        for value in (request, grant, push, start, complete, query, reply)
    )


def test_target_identity_is_echoed_by_routing_and_terminal_ack_messages() -> None:
    spec, target = _identity()
    request, grant = _lease(spec, target)
    spillback = protocol.SpillbackWorkerLease(
        request.lease_id, spec.task_id, spec.attempt_id, NodeID.random(),
        target_execution=target,
    )
    rejected = protocol.RejectWorkerLease(
        request.lease_id, spec.task_id, spec.attempt_id,
        protocol.LeaseRejectReason.PENDING_CAPACITY,
        target_execution=target,
    )
    start_reply = protocol.StartWorkerLeaseReply(
        request.lease_id, protocol.LeaseExecutionState.RUNNING, True,
        target_execution=target,
    )
    completion_reply = protocol.CompleteWorkerLeaseReply(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        protocol.TaskReplyStatus.SUCCEEDED,
        protocol.LeaseExecutionState.COMPLETED, True, True,
        target_execution=target,
    )
    missing_outcome = protocol.GetWorkerLeaseOutcomeReply(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        spec.owner_worker_id, target.target_output_ids, grant.node_id,
        False, False, error="missing", target_execution=target,
    )

    assert all(
        value.target_execution == target
        for value in (
            spillback, rejected, start_reply, completion_reply, missing_outcome,
        )
    )


def test_target_mask_drift_reorder_and_attempt_mismatch_are_rejected() -> None:
    spec, target = _identity()
    outputs = spec.return_ids()
    with pytest.raises(ProtocolError, match="target outputs"):
        protocol.RequestWorkerLease(
            LeaseID.random(), spec.task_id, spec.attempt_id, spec.resources,
            NodeID.random(), spec.owner_worker_id,
            return_ids=(outputs[2], outputs[0]), target_execution=target,
        )
    with pytest.raises(ProtocolError, match="task and attempt"):
        protocol.PushTask(
            LeaseID.random(), WorkerID.random(),
            protocol.TaskSpec(
                spec.job_id, spec.task_id, spec.attempt_id.next(), spec.function,
                (), 3, spec.resources, spec.owner_worker_id,
            ),
            target_execution=target,
        )
    one = (
        protocol.ResultDescriptor(
            outputs[0], protocol.ResultStorage.INLINE, 1,
            spec.owner_worker_id, NodeID.random(),
            hashlib.sha256(b"x").hexdigest(), b"x",
        ),
    )
    with pytest.raises(ProtocolError, match="exactly publish"):
        protocol.TaskReply(
            spec.task_id, spec.attempt_id, WorkerID.random(),
            protocol.TaskReplyStatus.SUCCEEDED, one, target_execution=target,
        )


def test_node_fences_start_completion_and_outcome_target_drift() -> None:
    spec, target = _identity()
    request, grant = _lease(spec, target)
    other = TargetExecutionKey.from_task_spec(
        spec, (spec.return_ids()[1],), attempt_id=spec.attempt_id
    )
    node = object.__new__(NodeServer)
    process = _Process()
    node.node_id = grant.node_id
    node.worker_id = grant.worker_id
    node.num_workers_per_node = 1
    node._worker_order = (grant.worker_id,)
    node._workers = {grant.worker_id: _WorkerSlot(
        grant.worker_id, process=process, address=grant.worker_address,
        active_lease_id=request.lease_id,
    )}
    node._legacy_worker_compat = False
    node._ledger = ResourceLedger(spec.resources)
    node._ledger.allocate(spec.resources, grant.allocation_token)
    node._dependency_pin_cleanups = {}
    node._leases = {request.lease_id: _LeaseRecord(
        request, grant.allocation_token, grant
    )}
    node._lease_outcomes = {request.lease_id: _LeaseOutcome(request, grant)}
    node._sealed_metadata = {}
    node._object_store = ObjectStore(1024)
    node._state_lock = threading.RLock()

    wrong_start = protocol.StartWorkerLease(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        target_execution=other,
    )
    assert not node._handle_start_worker_lease(wrong_start).accepted
    start = protocol.StartWorkerLease(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        target_execution=target,
    )
    assert node._handle_start_worker_lease(start).accepted
    wrong_complete = protocol.CompleteWorkerLease(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        protocol.TaskReplyStatus.SUCCEEDED, target_execution=other,
    )
    assert not node._handle_complete_worker_lease(wrong_complete).accepted
    query = protocol.GetWorkerLeaseOutcome(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        spec.owner_worker_id, other.target_output_ids, target_execution=other,
    )
    outcome = node._handle_get_worker_lease_outcome(query)
    assert not outcome.found
    assert outcome.target_execution == other


def test_node_outcome_scans_only_target_slots_not_healthy_sibling() -> None:
    spec, target = _identity()
    request, grant = _lease(spec, target)
    process = _Process(False)
    node = object.__new__(NodeServer)
    node.node_id = grant.node_id
    node.worker_id = grant.worker_id
    node._worker_order = (grant.worker_id,)
    node._workers = {grant.worker_id: _WorkerSlot(
        grant.worker_id, process=process, address=grant.worker_address,
        active_lease_id=None,
    )}
    node._legacy_worker_compat = False
    node._state_lock = threading.RLock()
    completion = protocol.CompleteWorkerLease(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        protocol.TaskReplyStatus.SYSTEM_ERROR, target_execution=target,
    )
    record = _LeaseRecord(
        request, grant.allocation_token, grant,
        state=protocol.LeaseExecutionState.COMPLETED, completion=completion,
    )
    node._leases = {request.lease_id: record}
    node._sealed_metadata = {}
    node._object_store = ObjectStore(1024)
    for index, object_id in enumerate(spec.return_ids()):
        payload = bytes([index])
        node._object_store.put(object_id, payload)
        node._sealed_metadata[object_id] = (
            spec.attempt_id, spec.owner_worker_id, len(payload),
            hashlib.sha256(payload).hexdigest(),
        )
    query = protocol.GetWorkerLeaseOutcome(
        request.lease_id, spec.task_id, spec.attempt_id, grant.worker_id,
        spec.owner_worker_id, target.target_output_ids, target_execution=target,
    )

    outcome = node._handle_get_worker_lease_outcome(query)

    assert tuple(item.object_id for item in outcome.orphan_descriptors) == (
        spec.return_ids()[0], spec.return_ids()[2],
    )
    assert spec.return_ids()[1] not in {
        item.object_id for item in outcome.orphan_descriptors
    }
    assert outcome.target_execution == target


def test_node_recovers_stored_success_from_complete_target_publication() -> None:
    spec, target, node, query, envelope, healthy = _successful_outcome_fixture((0, 2))
    outcome = _assert_successful_target_outcome(spec, target, node, query, envelope, healthy)

    assert tuple(item.object_id for item in outcome.descriptors) == (
        spec.return_ids()[0], spec.return_ids()[2],
    )
    assert outcome.orphan_descriptors == ()
    assert spec.return_ids()[1] not in {
        item.object_id for item in outcome.descriptors
    }


def test_node_mixed_success_recovers_inline_and_stored_targets_from_one_envelope() -> None:
    spec, target, node, query, envelope, healthy = _successful_outcome_fixture((0,))
    stored_target = target.target_output_ids[0]
    outcome = _assert_successful_target_outcome(spec, target, node, query, envelope, healthy)
    assert tuple(item.object_id for item in outcome.descriptors) == (
        stored_target,
    )
    stored, inline = outcome.output_publication.results
    assert stored.inline_data is None and stored.storage is protocol.ResultStorage.OBJECT_STORE
    assert node._object_store.get(stored_target) == b"selected-zero"
    assert inline.object_id == target.target_output_ids[1] and inline.inline_data == b"selected-two"
    assert not node._object_store.contains(inline.object_id)


def test_node_inline_only_success_exposes_no_fake_descriptor() -> None:
    spec, target, node, query, envelope, healthy = _successful_outcome_fixture(())
    outcome = _assert_successful_target_outcome(spec, target, node, query, envelope, healthy)

    assert outcome.descriptors == ()
    assert outcome.orphan_descriptors == ()
    assert outcome.completion_status is protocol.TaskReplyStatus.SUCCEEDED
    assert tuple(result.inline_data for result in outcome.output_publication.results) == (b"selected-zero", b"selected-two")
    assert all(not node._object_store.contains(output) for output in target.target_output_ids)


def test_node_rejects_descriptor_only_success_without_prepared_publication() -> None:
    _spec, target, node, query = _terminal_outcome_fixture(protocol.TaskReplyStatus.SUCCEEDED, (0, 1, 2))
    before = dict(node._sealed_metadata)
    outcome = node._handle_get_worker_lease_outcome(query)
    assert not outcome.found and "prepared publication" in outcome.error
    assert outcome.target_execution == target and outcome.completion_status is None
    assert outcome.descriptors == outcome.orphan_descriptors == ()
    assert outcome.output_publication is outcome.output_completion is None
    assert node._sealed_metadata == before


def test_node_system_error_reports_only_partial_target_seals_as_orphans() -> None:
    spec, target, node, query = _terminal_outcome_fixture(
        protocol.TaskReplyStatus.SYSTEM_ERROR, (0, 1),
    )
    first_target = target.target_output_ids[0]
    healthy = spec.return_ids()[1]

    outcome = node._handle_get_worker_lease_outcome(query)

    assert outcome.descriptors == ()
    assert tuple(item.object_id for item in outcome.orphan_descriptors) == (
        first_target,
    )
    assert healthy not in {
        item.object_id for item in outcome.orphan_descriptors
    }
