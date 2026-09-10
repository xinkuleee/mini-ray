"""Pure frozen-owner-route and abandoned-dependency wire/adapter contracts.

Each case has two tiny metadata descriptors, one immutable lease request and
one Worker death input. No Node/Core construction, object stores, network,
processes, threads, waits, schedulers or user code run. Adapter tests use a
passive owner and a handler-recording server; they never create an embedded
Core or claim that a typed death input alone proves a real process died.
"""

from copy import deepcopy
from dataclasses import replace
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time

import pytest

from miniray import owner_service, protocol, worker as worker_module
from miniray.core import CoreWorker
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.resources import ResourceVector
from miniray.worker import WorkerServer


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("abandoned dependency protocol test attempted runtime infrastructure")

    for kind, method in ((CoreWorker, "__init__"), (WorkerServer, "__init__"),
                         (threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Event, "wait"), (threading.Condition, "wait"),
                         (multiprocessing.process.BaseProcess, "start"),
                         (multiprocessing.process.BaseProcess, "join")):
        monkeypatch.setattr(kind, method, forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(owner_service, "TCPServer", forbidden)
    monkeypatch.setattr(worker_module, "TCPServer", forbidden)


def _fixture():
    submitter, foreign = WorkerID(b"s" * 16), WorkerID(b"f" * 16)
    task = TaskID(b"t" * 16)
    source, target = NodeID(b"a" * 16), NodeID(b"b" * 16)
    descriptors, routes = [], []
    for index, owner in enumerate((submitter, foreign)):
        producer = TaskID(bytes((120 + index,)) * 16)
        object_id = ObjectID.for_task(producer)
        descriptors.append(protocol.ObjectStoreDescriptor(object_id, owner, AttemptID(producer, 1), source, 8, "a" * 64))
        hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED if index == 0 else protocol.TaskReferenceHoldKind.RETAINED,
            submitter, task, AttemptID(task, 0),
        )
        routes.append(protocol.DependencyOwnerRoute(object_id, owner, ("owner-{}.invalid".format(index), 2100 + index), hold))
    request = protocol.RequestWorkerLease(
        LeaseID(b"l" * 16), task, AttemptID(task, 2), ResourceVector({"CPU": 1}),
        source, submitter, target_node_id=target, dependencies=tuple(descriptors),
        return_ids=(ObjectID.for_task(task),), dependency_owner_routes=tuple(routes),
    )
    inventory = protocol.LeaseDependencyInventory(
        request, target, tuple(replace(item, node_id=target) for item in descriptors),
    )
    death = protocol.WorkerDeathRecord(
        "submitter-exit", protocol.WorkerIncarnation(source, 3101, 1, submitter, 3201),
        3, -9, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    report = protocol.ReportAbandonedDependencyReplica(inventory, inventory.descriptors[1], death)
    return request, inventory, death, report


def test_routes_cover_ordered_dependencies_with_real_hold_kinds_and_older_origins():
    request, inventory, _death, report = _fixture()
    assert tuple(route.hold.kind for route in request.dependency_owner_routes) == (
        protocol.TaskReferenceHoldKind.SUBMITTED, protocol.TaskReferenceHoldKind.RETAINED,
    )
    assert all(route.hold.origin_attempt_id.attempt_number == 0 for route in request.dependency_owner_routes)
    assert request.attempt_id.attempt_number == 2
    assert protocol.revalidate_worker_lease_request(request) == request
    assert pickle.loads(pickle.dumps(inventory)) == inventory
    assert report.owner_route == request.dependency_owner_routes[1]
    legacy = replace(request, dependency_owner_routes=())
    assert protocol.revalidate_worker_lease_request(legacy).dependency_owner_routes == ()
    with pytest.raises(protocol.ProtocolError, match="owner routes"):
        protocol.ReportAbandonedDependencyReplica(
            protocol.LeaseDependencyInventory(legacy, inventory.node_id, inventory.descriptors),
            report.descriptor, report.submitter_death,
        )


@pytest.mark.parametrize("defect", ("partial", "reverse", "duplicate", "wrong-owner", "wrong-kind", "wrong-submitter", "wrong-task", "future-origin"))
def test_nonempty_owner_routes_reject_changed_manifest_or_submission_identity(defect):
    request, *_rest = _fixture()
    first, second = request.dependency_owner_routes
    other = TaskID(b"q" * 16)
    if defect == "partial":
        routes = (first,)
    elif defect == "reverse":
        routes = (second, first)
    elif defect == "duplicate":
        routes = (first, first)
    elif defect == "wrong-owner":
        routes = (first, replace(second, owner_worker_id=WorkerID(b"z" * 16)))
    elif defect == "wrong-kind":
        routes = (first, replace(second, hold=replace(second.hold, kind=protocol.TaskReferenceHoldKind.SUBMITTED)))
    elif defect == "wrong-submitter":
        routes = (first, replace(second, hold=replace(second.hold, submitting_worker_id=WorkerID(b"z" * 16))))
    elif defect == "wrong-task":
        routes = (first, replace(second, hold=replace(second.hold, task_id=other, origin_attempt_id=AttemptID(other, 0))))
    else:
        routes = (first, replace(second, hold=replace(second.hold, origin_attempt_id=AttemptID(request.task_id, 3))))
    with pytest.raises(protocol.ProtocolError):
        replace(request, dependency_owner_routes=routes)


def test_route_report_reply_and_revalidation_do_not_alias_caller_nested_identities():
    request, inventory, death, report = _fixture()
    expected = deepcopy(report)
    checked = protocol.revalidate_worker_lease_request(request)
    reply = protocol.ReportAbandonedDependencyReplicaReply(report, protocol.RetainedLocationReportStatus.CUSTODY_ONLY)
    assert pickle.loads(pickle.dumps(reply)) == reply
    object.__setattr__(request.dependency_owner_routes[1].hold.origin_attempt_id, "attempt_number", 8)
    object.__setattr__(request.dependency_owner_routes[1].owner_worker_id, "value", b"z" * 16)
    object.__setattr__(inventory.descriptors[1], "checksum", "b" * 64)
    object.__setattr__(death.incarnation.worker_id, "value", b"y" * 16)
    assert report == expected and reply.request == expected
    assert checked == expected.inventory.lease_request
    object.__setattr__(report.submitter_death.incarnation, "worker_pid", 9999)
    assert reply.request == expected


@pytest.mark.parametrize("defect", ("descriptor-checksum", "descriptor-node", "not-witnessed", "other-submitters-death", "expected-exit", "nested-pid", "nested-hold"))
def test_abandoned_report_rejects_unwitnessed_replica_or_invalid_submitter_death(defect):
    _request, inventory, death, report = _fixture()
    if defect == "descriptor-checksum":
        object.__setattr__(report.descriptor, "checksum", "b" * 64)
    elif defect == "descriptor-node":
        object.__setattr__(report.descriptor.node_id, "value", b"z" * 16)
    elif defect == "not-witnessed":
        object.__setattr__(report, "inventory", protocol.LeaseDependencyInventory(inventory.lease_request, inventory.node_id, inventory.descriptors[:1]))
    elif defect == "other-submitters-death":
        object.__setattr__(report.submitter_death.incarnation.worker_id, "value", b"z" * 16)
    elif defect == "expected-exit":
        object.__setattr__(report.submitter_death, "reason", protocol.WorkerDeathReason.EXPECTED)
    elif defect == "nested-pid":
        object.__setattr__(report.submitter_death.incarnation, "worker_pid", True)
    else:
        object.__setattr__(report.inventory.lease_request.dependency_owner_routes[1].hold.origin_attempt_id, "attempt_number", True)
    with pytest.raises(protocol.ProtocolError):
        replace(report)
    with pytest.raises(protocol.ProtocolError):
        pickle.loads(pickle.dumps(report))


def test_abandoned_reply_only_acknowledges_custody_never_execution():
    *_rest, report = _fixture()
    for status in (protocol.RetainedLocationReportStatus.CUSTODY_ONLY, protocol.RetainedLocationReportStatus.RETIRED):
        reply = protocol.ReportAbandonedDependencyReplicaReply(report, status)
        assert reply.accepted and reply.custody_transferred
    for status in (protocol.RetainedLocationReportStatus.REJECTED, protocol.RetainedLocationReportStatus.STALE_PRODUCER):
        reply = protocol.ReportAbandonedDependencyReplicaReply(report, status, "owner rejected this exact replica")
        assert not reply.accepted and not reply.custody_transferred
        with pytest.raises(protocol.ProtocolError):
            protocol.ReportAbandonedDependencyReplicaReply(report, status)
    for status in (protocol.RetainedLocationReportStatus.ADDED, protocol.RetainedLocationReportStatus.ALREADY_RECORDED):
        with pytest.raises(protocol.ProtocolError, match="authorize execution"):
            protocol.ReportAbandonedDependencyReplicaReply(report, status)


class _PassiveOwner:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.calls = []
        self.reply = None

    def report_abandoned_dependency_replica(self, request):
        self.calls.append(request)
        self.reply = protocol.ReportAbandonedDependencyReplicaReply(request, protocol.RetainedLocationReportStatus.CUSTODY_ONLY)
        return self.reply

    event_sink = None

    def acquire_exported_reference(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def release_borrowed_reference(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def get_owned_object(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def request_owned_object_reconstruction(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def request_drop_owned_object(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def retain_owned_object_for_task(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def get_retained_owned_object(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def report_retained_object_location(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def release_owned_object_for_task(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def replace_retained_object_for_task(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def release_contained_reference(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def install_actor_state(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def prepare_stored_contained_pin(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def promote_stored_contained_pin(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def register_output_handoff(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def report_output_handoff_complete(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def report_output_handoff_rollback(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")

    def get_output_handoff(self, request):
        pytest.fail("adapter invoked an unrelated owner operation")


class _Server:
    def __init__(self, handlers, **_kwargs):
        self.handlers = handlers
        self.address = ("not-started.invalid", 2001)
        self.is_running = False


def test_owner_service_advertises_and_forwards_only_to_matching_existing_owner(monkeypatch):
    *_rest, report = _fixture()
    owner = _PassiveOwner(report.descriptor.owner_worker_id)
    monkeypatch.setattr(owner_service, "TCPServer", _Server)
    service = owner_service.OwnerService(owner)
    handler = service._server.handlers[protocol.REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER]
    assert handler(report) is owner.reply and owner.calls == [report]
    owner.worker_id = WorkerID(b"z" * 16)
    wrong = handler(report)
    assert not wrong.custody_transferred and wrong.status is protocol.RetainedLocationReportStatus.REJECTED
    assert len(owner.calls) == 1
    service._core = None
    unavailable = handler(report)
    assert not unavailable.custody_transferred and "not available" in unavailable.error
    with pytest.raises(TypeError):
        handler(object())


def test_worker_adapter_never_creates_core_and_rejects_cross_owner_reports():
    *_rest, report = _fixture()
    worker = object.__new__(WorkerServer)
    worker.worker_id = report.descriptor.owner_worker_id
    worker._embedded_core = None
    worker._embedded_core_for = lambda *_a, **_k: pytest.fail("abandoned report created an embedded Core")
    missing = worker._handle_report_abandoned_dependency_replica(report)
    assert not missing.custody_transferred and worker._embedded_core is None
    owner = _PassiveOwner(report.descriptor.owner_worker_id)
    worker._embedded_core = owner
    assert worker._handle_report_abandoned_dependency_replica(report) is owner.reply
    assert owner.calls == [report]
    owner.worker_id = WorkerID(b"z" * 16)
    mismatched_core = worker._handle_report_abandoned_dependency_replica(report)
    assert not mismatched_core.custody_transferred and len(owner.calls) == 1
    worker.worker_id = WorkerID(b"q" * 16)
    wrong_worker = worker._handle_report_abandoned_dependency_replica(report)
    assert not wrong_worker.custody_transferred and len(owner.calls) == 1
    with pytest.raises(TypeError):
        worker._handle_report_abandoned_dependency_replica(object())
