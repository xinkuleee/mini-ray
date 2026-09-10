"""Pure owner custody handoff without granting an inactive task execution.

One threadless Core, one one-byte stored result and one foreign report. Owner
publication/location/retirement reducers are real; locations are grant-proven
input facts, not a claim that a physical store runs in these tests. GC enqueue
is observed without fabricated Drop ACKs. No thread, socket, process or wait.
"""

from copy import deepcopy
from dataclasses import replace
import hashlib
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.core import CoreWorker, _ObjectWaiter, _RetryInlineGc, _RetryReplicaCleanup
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope, OutputPublicationHeader,
    OutputPublicationID, OutputPublicationManifest, OutputPublicationNodeIncarnation,
    OutputValue,
)
from miniray.output_handoff import NodeLostOutputResolution
from miniray.ownership import ObjectState, OutputOwnerPublicationPlan
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecution
from tests.unit._pure_core import close_pure_core, make_pure_core


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("owner custody test attempted runtime infrastructure")

    for kind, method in (
        (CoreWorker, "__init__"), (threading.Thread, "start"),
        (threading.Thread, "join"), (threading.Timer, "start"),
        (threading.Event, "wait"), (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


class _Fixture:
    def __init__(self, *, active=False, unified=True):
        core = self.core = make_pure_core()
        task = TaskID(bytes((61,)) * 16)
        self.attempt, self.output = AttemptID(task, 0), ObjectID.for_task(task)
        spec = protocol.TaskSpec(
            core.job_id, task, self.attempt,
            protocol.FunctionKey(core.job_id, __name__, "metadata-producer", "v1"),
            (), 1, ResourceVector(), core.worker_id,
        )
        core.owner_table.register_task_outputs(spec, local_tokens=("owner-result",))
        core._recovery.register_task(spec)
        core._objects[self.output] = _ObjectWaiter(threading.Event())
        self.source, self.target = NodeID(bytes((62,)) * 16), NodeID(bytes((63,)) * 16)
        execution = TaskExecution.from_task_spec(spec)
        self.identity = OutputPublicationID(LeaseID(bytes((64,)) * 16), execution)
        header = OutputPublicationHeader(
            self.identity, core.job_id, WorkerID(bytes((65,)) * 16), core.worker_id,
            OutputPublicationNodeIncarnation(self.source, 1601, 1),
        )
        checksum = hashlib.sha256(b"x").hexdigest()
        self.manifest = OutputPublicationManifest.create(header, (OutputValue(protocol.ResultStorage.OBJECT_STORE, 1, checksum)))
        self.complete = OutputPublicationCompleteWitness.for_manifest(self.manifest)
        self.canonical = protocol.ResultDescriptor(
            self.output, protocol.ResultStorage.OBJECT_STORE, 1, core.worker_id, self.source, checksum,
        )
        self.envelope = OutputPublicationEnvelope(self.manifest, self.complete, (self.canonical))
        if unified:
            assert core.owner_table.commit_output_publication(OutputOwnerPublicationPlan(execution, self.envelope)).committed
        else:
            assert core.owner_table.publish_stored(self.output, self.attempt, self.source, descriptor=self.canonical)
        core._recovery.record_task_success(task, self.attempt)
        core._stored_descriptors[self.output] = self.canonical
        borrower, consumer_task = WorkerID(bytes((66,)) * 16), TaskID(bytes((67,)) * 16)
        self.hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, borrower, consumer_task, AttemptID(consumer_task, 0),
        )
        token = (borrower, "initial-reader")
        assert core.owner_table.add_borrowed_reference(self.output, token)
        assert core.owner_table.retain_borrowed_reference_for_task(self.output, token, self.hold)
        assert core.owner_table.release_borrowed_reference(self.output, token)
        if not active:
            assert core.owner_table.release_retained_reference_for_task(self.output, self.hold)
        self.report = protocol.ReportRetainedObjectLocation(
            self.output, core.worker_id, borrower, self.hold,
            protocol.ObjectStoreDescriptor(self.output, core.worker_id, self.attempt, self.target, 1, checksum),
        )

    def close(self):
        for output in self.core._objects:
            for token in self.core.owner_table.snapshot(output).local_tokens:
                self.core.owner_table.release_local_reference(output, token)
        close_pure_core(self.core)

    def events(self):
        return tuple(self.core._reference_mailbox.pending.queue)

    def lose_source(self):
        death = protocol.NodeDeathRecord(
            "custody-source-exit", self.source, 1601, 1, 1, 1,
            protocol.NodeDeathReason.PROCESS_EXIT, "installed source loss",
        )
        self.core.handle_node_death(death, protocol.InstallClusterSnapshot(1, "source-removed", ()))
        assert self.core.owner_table.snapshot(self.output).state is ObjectState.LOST
        assert self.output not in self.core._stored_descriptors
        return death


@pytest.mark.parametrize("active", (False, True), ids=("inactive-custody", "active-execution"))
def test_exact_current_replica_separates_owner_custody_from_consumer_permission(active):
    f = _Fixture(active=active)
    core = f.core
    try:
        before = core.owner_table.snapshot(f.output)
        core.close_owner_retain_admission()
        first = core.report_retained_object_location(f.report)
        second = core.report_retained_object_location(f.report)
        expected = protocol.RetainedLocationReportStatus.ADDED if active else protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert first.status is expected and first.accepted is active
        assert first.custody_transferred and second.custody_transferred
        assert second.status is (protocol.RetainedLocationReportStatus.ALREADY_RECORDED if active else expected)
        assert second.accepted is active
        assert first.descriptor == second.descriptor == f.report.descriptor
        after = core.owner_table.snapshot(f.output)
        assert after == replace(before, locations=frozenset({f.source, f.target}))
        assert after.canonical_stored_result == f.canonical
        assert core._stored_descriptors[f.output] == f.canonical
        assert core._inflight_borrow_ops == 0
        assert not getattr(core, "_late_replica_cleanup", None)
        if active:
            assert first.error is None and f.events() == ()
        else:
            assert "task hold is not active" in first.error
            assert f.events() == (_RetryInlineGc(f.output),) * 2
            queried = core.get_retained_owned_object(protocol.GetRetainedOwnedObject(
                f.output, core.worker_id, f.report.borrower_worker_id, f.hold,
            ))
            assert not queried.accepted and queried.descriptor is None
    finally:
        f.close()


def test_custody_only_current_epoch_rediscovery_restores_route_not_execution():
    f = _Fixture()
    try:
        f.lose_source()
        before = f.core.owner_table.snapshot(f.output)
        reply = f.core.report_retained_object_location(f.report)
        assert reply.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert reply.custody_transferred and not reply.accepted
        after = f.core.owner_table.snapshot(f.output)
        assert after == replace(before, state=ObjectState.READY_STORED, locations=frozenset({f.target}))
        assert f.core._stored_descriptors[f.output] == replace(f.canonical, node_id=f.target)
        assert after.canonical_stored_result.node_id == f.source
        assert f.events() == (_RetryInlineGc(f.output),)
    finally:
        f.close()


def test_custody_without_any_live_reference_enqueues_normal_gc_for_the_replica():
    f = _Fixture()
    try:
        assert f.core.owner_table.release_local_reference(f.output, "owner-result")
        assert not f.core.owner_table.snapshot(f.output).is_live
        reply = f.core.report_retained_object_location(f.report)
        assert reply.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert reply.custody_transferred and not reply.accepted
        after = f.core.owner_table.snapshot(f.output)
        assert not after.is_live and after.locations == frozenset({f.source, f.target})
        assert f.events() == (_RetryInlineGc(f.output),)
        assert not f.core._object_gc_obligations
    finally:
        f.close()


def test_lost_gc_wakeup_after_custody_commit_requires_exact_report_replay(monkeypatch):
    f = _Fixture()
    core = f.core
    original = core._enqueue_inline_gc_check
    attempts = []

    def enqueue(output):
        attempts.append(output)
        if len(attempts) == 1:
            raise RuntimeError("GC notification failed after location commit")
        original(output)

    monkeypatch.setattr(core, "_enqueue_inline_gc_check", enqueue)
    try:
        with pytest.raises(RuntimeError, match="after location commit"):
            core.report_retained_object_location(f.report)
        assert core.owner_table.snapshot(f.output).locations == frozenset((f.source, f.target))
        assert core._inflight_borrow_ops == 0
        reply = core.report_retained_object_location(f.report)
        assert reply.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert reply.custody_transferred and not reply.accepted
        assert attempts == [f.output, f.output] and f.events() == (_RetryInlineGc(f.output),)
    finally:
        f.close()


def test_custody_location_and_reply_do_not_share_caller_mutable_id_objects():
    f = _Fixture()
    try:
        replay = deepcopy(f.report)
        reply = f.core.report_retained_object_location(f.report)
        assert reply.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        before = f.core.owner_table.snapshot(f.output)
        route = deepcopy(f.core._stored_descriptors[f.output])
        object.__setattr__(f.report.descriptor.node_id, "value", b"z" * 16)
        object.__setattr__(reply.descriptor.node_id, "value", b"y" * 16)
        object.__setattr__(reply.descriptor.producer_attempt_id, "attempt_number", 8)
        assert f.core.owner_table.snapshot(f.output) == before
        assert f.core._stored_descriptors[f.output] == route
        repeated = f.core.report_retained_object_location(replay)
        assert repeated.status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY
        assert repeated.descriptor == replay.descriptor
    finally:
        f.close()


@pytest.mark.parametrize("corruption", (
    "checksum", "size", "next-attempt", "unknown-object", "wrong-owner", "closed-owner", "dead-target",
))
def test_unproven_replica_neither_mutates_locations_nor_claims_cleanup(corruption):
    f = _Fixture(unified=False)
    core, request = f.core, f.report
    try:
        if corruption == "checksum":
            request = replace(request, descriptor=replace(request.descriptor, checksum="0" * 64))
        elif corruption == "size":
            request = replace(request, descriptor=replace(request.descriptor, size_bytes=2))
        elif corruption == "next-attempt":
            request = replace(request, descriptor=replace(request.descriptor, producer_attempt_id=f.attempt.next()))
        elif corruption == "unknown-object":
            task = TaskID(bytes((68,)) * 16)
            unknown = ObjectID.for_task(task)
            request = replace(request, object_id=unknown, descriptor=replace(request.descriptor, object_id=unknown, producer_attempt_id=AttemptID(task, 0)))
        elif corruption == "wrong-owner":
            other = WorkerID(bytes((69,)) * 16)
            request = replace(request, owner_worker_id=other, descriptor=replace(request.descriptor, owner_worker_id=other))
        elif corruption == "closed-owner":
            assert core.shutdown(timeout=0.25, preserve_owner_protocol=True)
            assert core.finalize_shutdown(require_distributed_clean=True)
        elif corruption == "dead-target":
            death = protocol.NodeDeathRecord(
                "custody-target-exit", f.target, 1602, 2, 2, 1,
                protocol.NodeDeathReason.PROCESS_EXIT, "installed target loss",
            )
            core.handle_node_death(death, protocol.InstallClusterSnapshot(2, "target-removed", ()))
        before = core.owner_table.snapshot(f.output)
        routes = dict(core._stored_descriptors)
        events = f.events()
        reply = core.report_retained_object_location(request)
        assert reply.status is (protocol.RetainedLocationReportStatus.STALE_PRODUCER
                                if corruption == "next-attempt" else protocol.RetainedLocationReportStatus.REJECTED)
        assert not reply.accepted and not reply.custody_transferred
        assert core.owner_table.snapshot(f.output) == before
        assert core._stored_descriptors == routes and f.events() == events
        assert not core._has_late_replica_cleanup_locked() and core._inflight_borrow_ops == 0
    finally:
        f.close()


@pytest.mark.parametrize("fault", ("route-write", "owner-exception", "owner-fenced"))
def test_failed_custody_transaction_restores_exact_previous_route(monkeypatch, fault):
    f = _Fixture()
    core = f.core
    try:
        f.lose_source()
        before = core.owner_table.snapshot(f.output)
        if fault == "route-write":
            class RejectingRoutes(dict):
                def __setitem__(self, key, value):
                    raise RuntimeError("custody route write failed")
            core._stored_descriptors = RejectingRoutes()
        elif fault == "owner-exception":
            def reject(*_args, **_kwargs):
                raise RuntimeError("custody owner rejected location")
            monkeypatch.setattr(core.owner_table, "add_location", reject)
        else:
            monkeypatch.setattr(core.owner_table, "add_location", lambda *_args, **_kwargs: False)
        if fault == "owner-fenced":
            reply = core.report_retained_object_location(f.report)
            assert not reply.accepted and not reply.custody_transferred
            assert reply.status is protocol.RetainedLocationReportStatus.STALE_PRODUCER
        else:
            with pytest.raises(RuntimeError, match="custody"):
                core.report_retained_object_location(f.report)
        assert core.owner_table.snapshot(f.output) == before
        assert not core._stored_descriptors and f.events() == ()
        assert core._inflight_borrow_ops == 0
    finally:
        f.close()


def test_location_commit_then_exception_keeps_real_custody_and_replays_exactly(monkeypatch):
    f = _Fixture(active=True)
    core = f.core
    original = core.owner_table.add_location
    calls = []

    def apply_then_error(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append((args, kwargs))
        if len(calls) == 1:
            raise RuntimeError("location committed before callback failed")
        return result

    monkeypatch.setattr(core.owner_table, "add_location", apply_then_error)
    try:
        f.lose_source()
        with pytest.raises(RuntimeError, match="location committed"):
            core.report_retained_object_location(f.report)
        snapshot = core.owner_table.snapshot(f.output)
        route = core._stored_descriptors[f.output]
        assert snapshot.state is ObjectState.READY_STORED and snapshot.locations == frozenset((f.target,))
        assert route == replace(f.canonical, node_id=f.target)
        reply = core.report_retained_object_location(f.report)
        assert reply.status is protocol.RetainedLocationReportStatus.ALREADY_RECORDED
        assert reply.accepted and reply.custody_transferred
        assert calls[0] == calls[1]
        assert core.owner_table.snapshot(f.output) == snapshot and core._stored_descriptors[f.output] == route
    finally:
        f.close()


@pytest.mark.parametrize("phase", ("latched-drop", "resolved-drop", "collection", "retirement"))
def test_retired_or_frozen_publication_keeps_exact_cleanup_not_custody_location(phase):
    f = _Fixture()
    core = f.core
    try:
        if phase == "latched-drop":
            # This exact local decision is an input to the custody reducer,
            # not a GCS publication decision or proof of physical cleanup.
            core._output_loss_choices = {f.identity: False}
        elif phase == "resolved-drop":
            death = f.lose_source()
            resolution = NodeLostOutputResolution(
                f.identity, f.manifest.manifest_digest, core.worker_id, death,
                complete=f.complete, keep=False,
            )
            assert core.owner_table.resolve_output_node_loss(f.manifest, resolution)
        elif phase == "collection":
            core.owner_table.release_local_reference(f.output, "owner-result")
            assert core.owner_table.begin_output_publication_collection(f.output, collection_id="collect-before-report") is not None
        else:
            f.lose_source()
            membership = core.owner_table.output_owner_publication(f.output)
            core.owner_table.begin_output_publication_retirement(
                membership, retirement_id="retire-before-report", replica_locations=(f.source,),
            )
        before = core.owner_table.snapshot(f.output)
        routes = dict(core._stored_descriptors)
        reply = core.report_retained_object_location(f.report)
        assert reply.status is protocol.RetainedLocationReportStatus.RETIRED
        assert reply.custody_transferred and not reply.accepted
        assert core.owner_table.snapshot(f.output) == before and core._stored_descriptors == routes
        (drop,) = core._late_replica_cleanup.pending()
        assert drop == protocol.DropObjectReplica(f.output, f.attempt, core.worker_id, f.target, f.canonical.checksum)
        assert f.events() == (_RetryReplicaCleanup(),)
    finally:
        f.close()


@pytest.mark.parametrize("status", tuple(protocol.RetainedLocationReportStatus))
def test_report_wire_distinguishes_execution_acceptance_from_custody(status):
    f = _Fixture()
    try:
        accepted = status in (protocol.RetainedLocationReportStatus.ADDED, protocol.RetainedLocationReportStatus.ALREADY_RECORDED)
        request = f.report
        reply = protocol.ReportRetainedObjectLocationReply(
            request.object_id, request.owner_worker_id, request.borrower_worker_id,
            request.hold, request.descriptor, status, None if accepted else "execution rejected",
        )
        assert reply.accepted is accepted
        assert reply.custody_transferred is (accepted or status in (
            protocol.RetainedLocationReportStatus.CUSTODY_ONLY, protocol.RetainedLocationReportStatus.RETIRED,
        ))
        assert pickle.loads(pickle.dumps(reply)) == reply
        if status is protocol.RetainedLocationReportStatus.CUSTODY_ONLY:
            with pytest.raises(ProtocolError, match="must contain an error"):
                replace(reply, error=None)
    finally:
        f.close()
