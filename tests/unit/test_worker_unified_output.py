"""Pure Worker publication replay contracts; one output per execution.

Every RPC is an in-memory value.  No process, socket, timer, sleep, or runtime
Core is created; reducers and method calls expose every interleaving.
"""

from dataclasses import replace
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import enhanced_publication as ep, output_protocol as wire, protocol
from miniray.core import CoreWorker, ObjectRef
from miniray.enhanced_publication_client import PublicationClient
from miniray.dependency import ContainedRef, NestedReferenceImportSession, encode_task_argument
from miniray.ids import LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationManifest, OutputPublicationNodeIncarnation,
)
from miniray.output_handoff import OutputHandoffTable, OutputHandoffPhase
from miniray.transport import TransportError
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER,
    START_WORKER_LEASE_HANDLER,
)
from tests.unit.test_worker_completion_paths import _push, _worker


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    _install_no_runtime(monkeypatch)


def _install_no_runtime(monkeypatch):
    """Reusable tripwires for exact pure cases inside otherwise mixed files."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("unified Worker contract attempted runtime work")

    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Thread, "join", forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(threading.Event, "wait", forbidden)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr("miniray.worker.CoreWorker.__init__", forbidden)
    monkeypatch.setattr("miniray.worker.TCPServer.__init__", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _envelope(request):
    manifest = request.manifest
    (header, value) = ((manifest.header, manifest.value))
    result = (protocol.ResultDescriptor(manifest.publication_id.object_id, value.tier, value.size_bytes, header.owner_worker_id, header.node_incarnation.node_id, value.checksum, request.payload if value.tier is protocol.ResultStorage.INLINE else None))
    return OutputPublicationEnvelope(
        manifest, OutputPublicationCompleteWitness.for_manifest(manifest), result,
    )


def _completion(request, envelope=None, *, accepted=True, witness=None):
    return protocol.CompleteWorkerLeaseReply(
        request.lease_id, request.task_id, request.attempt_id, request.worker_id,
        request.status, protocol.LeaseExecutionState.COMPLETED, accepted, accepted,
        error=None if accepted else "completion not yet accepted",
        scheduling_key=request.scheduling_key,
        output_publication=envelope, output_completion=witness,
    )


class _Fixture:
    def __init__(self, monkeypatch, function, *, count=1, threshold=1024):
        self.worker = _worker(WorkerID.random(), inline_threshold=threshold)
        self.worker._server = SimpleNamespace(address=("127.0.0.1", 32301))
        self.push = _push(self.worker.worker_id, b"unified-output-function", num_returns=count)
        assert count == 1
        self.worker._lifecycle = threading.Condition(threading.RLock())
        self.worker._accepted_pushes = {}
        self.worker._push_obligations = set()
        self.worker._accepting_tasks = True
        self.worker._active_tasks = 0
        self.incarnation = OutputPublicationNodeIncarnation(self.worker.node_id, 23001, 7)
        self.calls = []
        self.executions = []
        self.prepares = []
        self.on_prepare = self.on_complete = self.on_outcome = None
        self.prepared_request = None
        self.complete_envelope = None
        real_loads = cloudpickle.loads

        def invoke(*args, **kwargs):
            self.executions.append(True)
            return function(*args, **kwargs)

        monkeypatch.setattr("miniray.worker.cloudpickle.loads", lambda payload:
                            invoke if payload == b"unified-output-function" else real_loads(payload))
        monkeypatch.setattr("miniray.worker.rpc_request", self.rpc)
        for name in ("_encode_result", "_encode_serialized_result", "_reference_export_session"):
            assert not hasattr(self.worker, name), name

    @property
    def key(self):
        return self.push.spec.attempt_id, self.push.lease_id

    @property
    def pending(self):
        return self.worker._prepared_output_replies[self.key]

    @property
    def handlers(self):
        return [handler for handler, _request in self.calls]

    def rpc(self, _address, handler, request, **_options):
        self.calls.append((handler, request))
        if handler == START_WORKER_LEASE_HANDLER:
            return protocol.StartWorkerLeaseReply(
                request.lease_id, protocol.LeaseExecutionState.RUNNING, True,
                scheduling_key=request.scheduling_key,
                node_incarnation=self.incarnation,
            )
        if handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER:
            assert type(request) is wire.PrepareOutputPublication
            self.prepares.append(request)
            self.prepared_request = request
            return self.on_prepare(request) if self.on_prepare else wire.PreparedOutputPublicationReply(request.request_identity, True)
        if handler == COMPLETE_WORKER_LEASE_HANDLER:
            if self.on_complete:
                return self.on_complete(request)
            return self.complete(request)
        if handler == GET_WORKER_LEASE_OUTCOME_HANDLER and self.on_outcome:
            return self.on_outcome(request)
        pytest.fail(f"unexpected RPC: {handler}")

    def complete(self, request):
        if request.status is protocol.TaskReplyStatus.SUCCEEDED:
            assert self.key not in self.worker._replies
            self.complete_envelope = _envelope(self.prepared_request)
        return _completion(request, self.complete_envelope)


class _ActualNodePublication:
    """Bounded adapter/store composition for precise Worker migration tests.

    One output and one 128 KiB store; no Node constructor,
    transport or background worker. Complete comes from the real journal.
    """

    def __init__(self, fixture, *, child_tables=None):
        from miniray.node import NodeServer
        from miniray.object_manager import ObjectManager
        from miniray.object_store import ObjectStore
        from miniray.output_publication_journal import OutputPublicationJournal
        from miniray.output_publication_node import OutputPublicationNodeAdapter
        from miniray.resources import AllocationToken, ResourceLedger, ResourceVector

        self.fixture = fixture
        self.children = child_tables or {}
        self.journal = OutputPublicationJournal()
        self.handoffs = OutputHandoffTable()
        self.authority = ep.PublicationAuthority()
        self.publication_calls = []
        self.owner_address = ("owner.invalid", 32302)
        owner = self.owner = object.__new__(CoreWorker)
        owner.worker_id = fixture.push.spec.owner_worker_id
        owner.owner_address = self.owner_address
        owner._state_lock = threading.RLock()
        owner._completion = threading.Condition(owner._state_lock)
        owner._owner_protocol_open = True
        owner._output_handoffs = self.handoffs
        owner._enhanced_publication_client = PublicationClient(self.owner_publication_rpc)
        self.store = ObjectStore(128 * 1024)
        self.ledger = ResourceLedger(ResourceVector({"CPU": 1}))
        self.token = AllocationToken("exact-worker-output")
        self.ledger.allocate(ResourceVector({"CPU": 1}), self.token)
        self.completions = []
        node = object.__new__(NodeServer)
        self.node = node
        node.node_id = fixture.incarnation.node_id
        node._node_pid = fixture.incarnation.node_pid
        node._registration_epoch = fixture.incarnation.registration_epoch
        node._state_lock = threading.RLock()
        node._object_store = self.store
        node._object_manager = ObjectManager(node.node_id, self.store)
        node._sealed_metadata, node._dropped_metadata = {}, {}
        node._local_replica_write_claims, node._object_localization_locks = {}, {}
        node._owner_death_fences = {}
        node._output_publication_journal = self.journal
        self.adapter = OutputPublicationNodeAdapter(
            self.journal, register_owner=self.register_owner,
            report_complete=self.report_complete, report_rollback=self.report_rollback,
            publication_value=lambda manifest: ep.TaskPublication(manifest, self.owner_address),
            publication_rpc=self.publication_rpc, abort_owner=self.abort_owner,
            prepare_child=self.prepare_child, promote_child=self.promote_child,
            release_child=self.release_child,
            seal_replica=node._seal_output_publication_replica,
            drop_replica=node._drop_output_publication_replica,
        )
        fixture.on_prepare, fixture.on_complete = self.prepare, self.complete

    def publication_rpc(self, request):
        assert len(self.publication_calls) < 24
        reply = self.authority.apply(request)
        assert type(reply) is ep.PublicationReply and reply.request == request
        self.publication_calls.append((request, reply))
        return reply

    def owner_publication_rpc(self, handler, request):
        assert handler == ep.PUBLICATION_HANDLER
        return self.publication_rpc(request)

    def abort_owner(self, publication, scope):
        assert scope == self.journal.rollback_scope(publication.reference.key)
        request = ep.AbortOwnerPublication(publication, scope)
        reply = self.owner.abort_owner_publication(request)
        assert type(reply) is ep.AbortOwnerPublicationReply and reply.request == request
        assert reply.accepted and reply.receipt is not None, reply.error
        return reply.receipt

    def register_owner(self, manifest):
        snapshot = self.handoffs.register(manifest, manifest.publication_id.attempt_id)
        request = wire.RegisterOutputHandoff(manifest)
        reply = wire.OutputHandoffReply(request, True, snapshot)
        assert reply.request == request and reply.snapshot.manifest == manifest

    def report_complete(self, witness):
        assert self.journal.snapshot(witness.publication_id).complete == witness
        reply = self.owner.report_output_handoff_complete(wire.ReportOutputHandoffComplete(witness))
        assert type(reply) is wire.OutputHandoffCompleteAck
        assert reply.accepted and reply.witness == witness

    def report_rollback(self, tombstone, *, manifest):
        assert self.journal.snapshot(manifest.publication_id).rollback_tombstone == tombstone
        request = wire.ReportOutputHandoffRollback(manifest, tombstone)
        reply = self.owner.report_output_handoff_rollback(request)
        assert type(reply) is wire.OutputHandoffReply and reply.request == request and reply.accepted, reply.error
        assert reply.snapshot.phase is OutputHandoffPhase.ABORTED
        assert reply.snapshot.abort_reason == "node rollback:" + tombstone.plan.rollback_id

    def prepare_child(self, address, request):
        assert address == request.transfer.contained_owner_address
        result = self.children[request.authority_worker_id].prepare_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id,
        )
        return protocol.StoredContainedPinReply(request, result)

    def promote_child(self, address, request):
        assert address == request.transfer.contained_owner_address
        result = self.children[request.authority_worker_id].promote_stored_contained_reference(
            request.transfer, authority_worker_id=request.authority_worker_id,
        )
        return protocol.StoredContainedPinReply(request, result)

    def release_child(self, address, request):
        released = self.children[request.owner_worker_id].release_contained_reference(
            request.object_id, request.hold,
        )
        return protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, released,
        )

    def prepare(self, request):
        assert len(((request.manifest.value,))) == 1
        assert request.manifest == self.fixture.pending.outputs.manifest
        self.adapter.prepare(request.manifest, (request.payload))
        return wire.PreparedOutputPublicationReply(request.request_identity, True)

    def complete(self, request):
        identity = self.fixture.prepared_request.manifest.publication_id
        assert (request.lease_id, request.task_id, request.attempt_id) == (
            identity.lease_id, identity.task_id, identity.attempt_id,
        )
        assert request.status is protocol.TaskReplyStatus.SUCCEEDED

        def commit(witness):
            assert self.journal.snapshot(identity).complete == witness
            assert not self.completions and self.ledger.release(self.token)
            self.completions.append(witness)

        self.fixture.complete_envelope = self.adapter.complete(identity, commit_lease=commit)
        return _completion(request, self.fixture.complete_envelope)
@pytest.mark.parametrize("case", ("inline", "stored", "tuple-value", "inline-contained", "stored-contained"))
def test_every_success_uses_one_output_and_one_actual_complete(monkeypatch, case):
    values = []
    stored = case in ("stored", "stored-contained")
    f = _Fixture(monkeypatch, lambda: values[0], threshold=0 if stored else 1024)
    children = {}
    if "contained" in case:
        from miniray.ownership import ObjectOwnerTable
        child = ObjectRef(ObjectID.for_task(TaskID.random()), f.worker.worker_id, f.worker.address)
        table = ObjectOwnerTable()
        table.register(child.object_id, local_token="source-live")
        children[f.worker.worker_id] = table
        values.append({"same-child": (child, child)})
    else:
        values.append((7, b"tuple-value") if case == "tuple-value" else 7)
    actual = _ActualNodePublication(f, child_tables=children)
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert reply.output_publication == f.complete_envelope
    assert reply.results == ((f.complete_envelope.result,))
    assert len(actual.completions) == 1 and actual.completions[0] == reply.output_publication.complete
    assert not hasattr(reply, "stored_publication") and not hasattr(reply, "inline_publication")
    assert not hasattr(reply, "contained_edges")
    assert f.handlers == [START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER, COMPLETE_WORKER_LEASE_HANDLER]
    assert not f.worker._prepared_output_replies and f.key in f.worker._completion_acked
    assert f.worker._handle_push_task(f.push) is reply and f.executions == [True]
    manifest = f.prepares[0].manifest
    assert manifest.header.node_incarnation == f.incarnation
    slot = manifest.value
    assert manifest.publication_id.object_id == f.push.spec.return_ids()[0]
    assert (manifest.publication_id.object_id.return_index == 0)
    assert slot.tier is (protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE)
    if "contained" in case:
        transfer, = slot.transfers
        assert transfer.contained_object_id == child.object_id
        assert transfer.final_hold in table.snapshot(child.object_id).contained_holds
        assert transfer.provisional_hold not in table.snapshot(child.object_id).contained_holds
    if case == "tuple-value":
        assert cloudpickle.loads(reply.results[0].inline_data) == (7, b"tuple-value")


class _TrackedRef(ObjectRef):
    __slots__ = ("closes",)

    def __init__(self, object_id, owner, address):
        super().__init__(object_id, owner, address)
        self.closes = 0

    def close(self):
        self.closes += 1
        self._closed = True


def _borrowed_fixture(monkeypatch, *, threshold=1024):
    reductions = []

    class _Returned:
        def __init__(self, child):
            self.child = child

        def __reduce__(self):
            reductions.append(True)
            return dict, ((("child", self.child),),)

    def function(container):
        returned = _Returned(container["child"])
        return returned

    f = _Fixture(monkeypatch, function, threshold=threshold)
    owner = WorkerID.random()
    child_id = ObjectID.for_task(TaskID.random())
    hold = protocol.TaskReferenceHold(protocol.TaskReferenceHoldKind.RETAINED,
                                      f.push.spec.owner_worker_id, f.push.spec.task_id, f.push.spec.attempt_id)
    transfer = protocol.NestedReferenceTransfer(child_id, owner, ("127.0.0.1", 32302), hold)
    argument = encode_task_argument({"child": ContainedRef(child_id, owner)}, serializer="cloudpickle",
                                    export_nested_ref=lambda _ref: transfer)
    f.push = replace(f.push, spec=replace(f.push.spec, args=(argument,)))
    child = _TrackedRef(child_id, owner, transfer.owner_address)
    child._borrower_token = "live-attempt-borrower"
    child._borrow_source = protocol.TaskHoldSource(hold)
    imports = NestedReferenceImportSession(lambda exact: child if exact == transfer else pytest.fail("wrong transfer"))
    monkeypatch.setattr(f.worker, "_nested_argument_import_session", lambda _push: imports)
    return f, child, imports, reductions


@pytest.mark.parametrize("mode", ("transport", "malformed", "rebound"))
def test_prepare_ambiguity_keeps_once_bytes_and_borrower_custody_for_exact_replay(monkeypatch, mode):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch)
    attempts = []

    def prepare(request):
        attempts.append(request)
        assert not child.closed and f.pending.nested_imports is imports
        assert f.pending.discovery.source_references == (child,)
        if mode == "transport":
            raise TransportError("prepare ACK lost")
        if mode == "malformed":
            reply = wire.PreparedOutputPublicationReply(request.request_identity, True)
            object.__setattr__(reply, "accepted", "yes")
            return reply
        identity = replace(request.request_identity, manifest_digest="0" * 64)
        return wire.PreparedOutputPublicationReply(identity, True)

    f.on_prepare = prepare
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    cached_outputs = f.pending.outputs
    assert not f.pending.prepare_acked and f.pending.failure_reply is None
    assert not child.closed and child.closes == 0 and f.key not in f.worker._replies
    assert COMPLETE_WORKER_LEASE_HANDLER not in f.handlers
    f.on_prepare = None
    reply = f.worker._handle_push_task(f.push)
    assert f.executions == [True] and reductions == [True]
    assert all(request == attempts[0] for request in f.prepares)
    assert (f.prepared_request.payload) == (cached_outputs.payload)
    assert child.closed and child.closes == 1
    assert reply.output_publication.manifest == cached_outputs.manifest


@pytest.mark.parametrize("phase", ("source", "imports"))
def test_prepare_ack_survives_cleanup_effect_then_error_and_retries_only_local_drain(monkeypatch, phase):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=0)
    calls = []
    real_release = OutputDiscoverySession.release_sources_after_promotions
    real_close = imports.close

    def release(discovery):
        real_release(discovery)
        if phase == "source":
            calls.append(True)
            if len(calls) == 1:
                raise RuntimeError("source release took effect before error")

    def close():
        real_close()
        if phase == "imports":
            calls.append(True)
            if len(calls) == 1:
                raise RuntimeError("import close took effect before error")

    monkeypatch.setattr(OutputDiscoverySession, "release_sources_after_promotions", release)
    monkeypatch.setattr(imports, "close", close)
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    assert pending.prepare_acked and pending.nested_imports is imports
    assert pending.complete_envelope is None and f.key not in f.worker._replies
    assert COMPLETE_WORKER_LEASE_HANDLER not in f.handlers
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert len(f.prepares) == 1 and len(calls) == 2
    assert child.closes == 1 and pending.nested_imports is None
    assert f.executions == [True] and reductions == [True]


@pytest.mark.parametrize("mode", ("transport", "missing", "manifest", "malformed"))
def test_complete_ambiguity_retains_prepared_success_without_republication_or_reserialization(monkeypatch, mode):
    f, child, _imports, reductions = _borrowed_fixture(monkeypatch)
    completions = []

    def complete(request):
        completions.append(request)
        envelope = _envelope(f.prepared_request)
        assert child.closed and f.pending.prepare_acked and f.key not in f.worker._replies
        if mode == "transport":
            raise TransportError("Complete took effect but ACK was lost")
        if mode == "missing":
            return _completion(request)
        if mode == "manifest":
            header = replace(envelope.manifest.header, node_incarnation=replace(f.incarnation, registration_epoch=8))
            manifest = OutputPublicationManifest.create(header, (envelope.manifest.value))
            envelope = OutputPublicationEnvelope(manifest, OutputPublicationCompleteWitness.for_manifest(manifest), (envelope.result))
            return _completion(request, envelope)
        reply = _completion(request, envelope)
        object.__setattr__((reply.output_publication.result), "inline_data", b"changed")
        return reply

    f.on_complete = complete
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    assert pending.complete_envelope is None and pending.failure_reply is None
    assert f.key not in f.worker._completion_acked and f.key not in f.worker._replies
    f.on_complete = None
    reply = f.worker._handle_push_task(f.push)
    assert reply.output_publication.manifest == pending.outputs.manifest
    assert len(f.prepares) == 1 and child.closes == 1
    assert f.executions == [True] and reductions == [True]
    assert all(request == completions[0] for handler, request in f.calls if handler == COMPLETE_WORKER_LEASE_HANDLER)


def _outcome(f, request, envelope=None, *, witness=None):
    if witness is not None:
        return protocol.GetWorkerLeaseOutcomeReply(
            request.lease_id, request.task_id, request.attempt_id, request.executor_worker_id,
            request.owner_worker_id, request.object_ids, f.worker.node_id, True, True,
            protocol.LeaseExecutionState.COMPLETED, protocol.TaskReplyStatus.SUCCEEDED,
            scheduling_key=request.scheduling_key,
            output_completion=witness,
        )
    if envelope is None:
        return protocol.GetWorkerLeaseOutcomeReply(
            request.lease_id, request.task_id, request.attempt_id, request.executor_worker_id,
            request.owner_worker_id, request.object_ids, f.worker.node_id,
            True, True, protocol.LeaseExecutionState.RUNNING,
            scheduling_key=request.scheduling_key,
        )
    descriptors = tuple(protocol.ObjectStoreDescriptor(
        result.object_id, result.owner_worker_id, request.attempt_id, result.node_id,
        result.size_bytes, result.checksum,
    ) for result in ((envelope.result,)) if result.storage is protocol.ResultStorage.OBJECT_STORE)
    return protocol.GetWorkerLeaseOutcomeReply(
        request.lease_id, request.task_id, request.attempt_id, request.executor_worker_id,
        request.owner_worker_id, request.object_ids, f.worker.node_id, True, True,
        protocol.LeaseExecutionState.COMPLETED, protocol.TaskReplyStatus.SUCCEEDED, descriptors,
        scheduling_key=request.scheduling_key,
        output_publication=envelope,
    )


@pytest.mark.parametrize("complete_failure", ("transport", "rejected"))
def test_rejected_prepare_freezes_abort_and_keeps_custody_until_failed_complete_ack(monkeypatch, complete_failure):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch)
    f.on_prepare = lambda request: wire.PreparedOutputPublicationReply(
        request.request_identity, False, wire.OutputPublicationRPCErrorKind.INVALID_STATE, "promotion rejected",
    )

    def incomplete(request):
        assert request.status is protocol.TaskReplyStatus.SYSTEM_ERROR
        assert not child.closed and f.pending.nested_imports is imports
        if complete_failure == "transport":
            raise TransportError("compensation ACK lost")
        return _completion(request, accepted=False)

    f.on_complete = incomplete
    f.on_outcome = lambda request: _outcome(f, request)
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    failure = pending.failure_reply
    assert failure.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert not child.closed and pending.nested_imports is imports
    assert not pending.prepare_acked and pending.complete_envelope is None
    assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
    f.on_complete = None
    reply = f.worker._handle_push_task(f.push)
    assert reply is failure and reply.output_publication is None
    assert len(f.prepares) == 1 and f.executions == [True] and reductions == [True]
    assert child.closes == 1 and not f.worker._prepared_output_replies
    assert f.worker._handle_push_task(f.push) is failure
    assert all(request.status is protocol.TaskReplyStatus.SYSTEM_ERROR
               for handler, request in f.calls if handler == COMPLETE_WORKER_LEASE_HANDLER)


@pytest.mark.parametrize("metadata_only", (False, True))
def test_positive_exact_outcome_replaces_abort_and_survives_local_cleanup_error(monkeypatch, metadata_only):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch)
    f.on_prepare = lambda request: wire.PreparedOutputPublicationReply(
        request.request_identity, False, wire.OutputPublicationRPCErrorKind.CONFLICT, "success may already exist",
    )
    f.on_complete = lambda request: _completion(request, accepted=False)
    queries = []

    def outcome(request):
        queries.append(request)
        assert request.object_ids == f.push.spec.return_ids()
        assert not child.closed
        if metadata_only:
            return _outcome(f, request, witness=OutputPublicationCompleteWitness.for_manifest(f.prepared_request.manifest))
        return _outcome(f, request, _envelope(f.prepared_request))

    f.on_outcome = outcome
    release_calls = []
    real_release = OutputDiscoverySession.release_sources_after_promotions

    def release(discovery):
        real_release(discovery)
        release_calls.append(True)
        if len(release_calls) == 1:
            raise RuntimeError("local drain acknowledgement lost")

    monkeypatch.setattr(OutputDiscoverySession, "release_sources_after_promotions", release)
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    assert pending.failure_reply is None and pending.complete_envelope is not None
    assert pending.prepare_acked and pending.nested_imports is imports
    assert f.key in f.worker._completion_acked and f.key not in f.worker._replies
    actual = pending.complete_envelope
    rpc_count = len(f.calls)
    reply = f.worker._handle_push_task(f.push)
    assert reply.output_publication == actual and len(f.calls) == rpc_count
    assert len(queries) == 1 and len(release_calls) == 2
    assert child.closes == 1 and pending.nested_imports is None
    assert f.executions == [True] and reductions == [True]


def test_success_reply_creation_failure_reuses_actual_node_envelope_not_new_complete(monkeypatch):
    f = _Fixture(monkeypatch, lambda: 7)
    actual_cache = f.worker._cache_complete_and_return

    def fail_cache(*_args):
        raise RuntimeError("post-Complete local reply cache failed")

    monkeypatch.setattr(f.worker, "_cache_complete_and_return", fail_cache)
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    assert f.pending.complete_envelope is not None and f.key in f.worker._completion_acked
    assert f.key not in f.worker._replies
    actual = f.pending.complete_envelope
    rpc_count = len(f.calls)
    monkeypatch.setattr(f.worker, "_cache_complete_and_return", actual_cache)
    reply = f.worker._handle_push_task(f.push)
    assert reply.output_publication == actual and len(f.calls) == rpc_count
    assert f.executions == [True]


def test_discovery_failure_has_no_prepare_effect_and_keeps_ordinary_failed_complete(monkeypatch):
    reductions = []

    class _Good:
        def __reduce__(self):
            reductions.append("good")
            return int, (1,)

    class _Bad:
        def __reduce__(self):
            reductions.append("bad")
            raise ValueError("result reduction failed")

    f = _Fixture(monkeypatch, lambda: (_Good(), _Bad()))
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert reply.output_publication is None and reply.results == ()
    assert f.handlers == [START_WORKER_LEASE_HANDLER, COMPLETE_WORKER_LEASE_HANDLER]
    assert not getattr(f.worker, "_prepared_output_replies", {})
    assert reductions == ["good", "bad"] and f.executions == [True]
    assert f.worker._handle_push_task(f.push) is reply


def test_pending_complete_keeps_lifecycle_obligation_until_success_reply_is_cached(monkeypatch):
    f = _Fixture(monkeypatch, lambda: 7)
    worker = f.worker
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._accepted_pushes = {}
    worker._push_obligations = set()
    worker._accepting_tasks = True
    worker._active_tasks = 0

    def complete(_request):
        raise TransportError("Complete reply lost")

    f.on_complete = complete
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        worker._handle_push_task(f.push)
    assert worker._push_obligations == {f.key} and worker._active_tasks == 0
    worker._accepting_tasks = False
    f.on_complete = None
    reply = worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert worker._push_obligations == set() and worker._active_tasks == 0
    assert f.executions == [True]


@pytest.mark.parametrize("stored", (False, True))
def test_owner_retires_payload_after_lost_complete_ack_then_worker_drain_uses_local_bytes(monkeypatch, stored):
    f, child, _imports, reductions = _borrowed_fixture(monkeypatch, threshold=0 if stored else 1024)
    worker = f.worker
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._accepted_pushes = {}
    worker._push_obligations = set()
    worker._accepting_tasks = True
    worker._active_tasks = 0
    worker._drain_request_id = "owner-adopted-before-worker-ack"
    node_metadata = []

    def lost_complete(request):
        node_metadata.append(OutputPublicationCompleteWitness.for_manifest(f.prepared_request.manifest))
        raise TransportError("owner recovered and retired Node payload before Worker ACK")

    f.on_complete = lost_complete
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        worker._handle_push_task(f.push)
    local_outputs = f.pending.outputs
    assert child.closed and worker._push_obligations == {f.key}
    assert f.pending.complete_envelope is None and f.key not in worker._replies
    deadlines = []

    def retired_rpc(_address, handler, request, **options):
        assert handler == COMPLETE_WORKER_LEASE_HANDLER
        deadlines.append(options)
        reply = _completion(request, witness=node_metadata[0])
        assert reply.output_publication is None
        assert (local_outputs.payload not in pickle.dumps(reply))
        return reply

    monkeypatch.setattr("miniray.worker.rpc_request", retired_rpc)
    worker._accepting_tasks = False
    status = worker._drain_status(worker._drain_request_id, timeout=0.5)
    assert status.clean and worker._push_obligations == set()
    assert not worker._prepared_output_replies and worker._active_tasks == 0
    reply = worker._replies[f.key]
    assert reply.output_publication.complete == node_metadata[0]
    assert reply.output_publication.manifest == local_outputs.manifest
    result, = reply.results
    assert result.inline_data == (None if stored else (local_outputs.payload))
    assert result.storage is (protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE)
    assert len(deadlines) == 1
    assert 0 < deadlines[0]["request_timeout"] <= 0.5
    assert deadlines[0]["connect_timeout"] == deadlines[0]["request_timeout"]
    assert f.executions == [True] and reductions == [True]


@pytest.mark.parametrize("mode", ("missing", "digest", "malformed", "no-local-bytes"))
def test_metadata_completion_without_exact_witness_and_local_data_stays_pending(monkeypatch, mode):
    f, _child, _imports, reductions = _borrowed_fixture(monkeypatch)

    def complete(request):
        witness = OutputPublicationCompleteWitness.for_manifest(f.prepared_request.manifest)
        if mode == "missing":
            return _completion(request)
        if mode == "digest":
            return _completion(request, witness=replace(witness, manifest_digest="0" * 64))
        if mode == "malformed":
            reply = _completion(request, witness=witness)
            object.__setattr__(reply.output_completion, "manifest_digest", "invalid")
            return reply
        object.__setattr__(f.pending.outputs, ('payload'), (b'corrupt'))
        return _completion(request, witness=witness)

    f.on_complete = complete
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    assert f.pending.complete_envelope is None and f.pending.failure_reply is None
    assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
    assert f.executions == [True] and reductions == [True]


def test_drain_replays_at_most_one_pending_publication_and_never_waits_for_execution_lock(monkeypatch):
    f = _Fixture(monkeypatch, lambda: 7)
    f.on_prepare = lambda _request: (_ for _ in ()).throw(TransportError("prepare pending"))
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    worker = f.worker
    first_key, first_pending = f.key, f.pending
    second = _push(worker.worker_id, b"unified-output-function")
    f.push = second
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        worker._handle_push_task(f.push)
    assert len(worker._prepared_output_replies) == 2
    worker._drain_request_id = "one-publication-per-poll"
    requests = []

    def unavailable(_address, handler, request, **options):
        requests.append((handler, request, options))
        raise TransportError("one bounded drain attempt")

    monkeypatch.setattr("miniray.worker.rpc_request", unavailable)
    assert worker._execution_lock.acquire(blocking=False)
    try:
        assert not worker._drain_status(worker._drain_request_id, timeout=0.25).clean
        assert requests == []
    finally:
        worker._execution_lock.release()
    assert not worker._drain_status(worker._drain_request_id, timeout=0.25).clean
    assert len(requests) == 1 and requests[0][0] == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER
    assert requests[0][1].manifest == first_pending.outputs.manifest
    assert 0 < requests[0][2]["request_timeout"] <= 0.25
    assert first_key in worker._prepared_output_replies and len(worker._prepared_output_replies) == 2
    assert f.executions == [True, True]


def test_drain_deadline_expiring_after_prepare_does_not_start_complete(monkeypatch):
    f = _Fixture(monkeypatch, lambda: 7)
    f.on_prepare = lambda _request: (_ for _ in ()).throw(TransportError("prepare pending"))
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    worker = f.worker
    worker._drain_request_id = "bounded-deadline"
    now = [10.0]
    monkeypatch.setattr("miniray.worker.time.monotonic", lambda: now[0])
    requests = []

    def prepare_at_deadline(_address, handler, request, **options):
        requests.append((handler, options))
        assert handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER
        now[0] = options["deadline"]
        return wire.PreparedOutputPublicationReply(request.request_identity, True)

    monkeypatch.setattr("miniray.worker.rpc_request", prepare_at_deadline)
    status = worker._drain_status(worker._drain_request_id, timeout=0.1)
    assert not status.clean and len(requests) == 1
    assert f.pending.prepare_acked and f.pending.complete_envelope is None
    assert f.key not in worker._replies and f.executions == [True]


def test_invalid_metadata_outcome_does_not_override_frozen_abort_or_release_custody(monkeypatch):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch)
    f.on_prepare = lambda request: wire.PreparedOutputPublicationReply(
        request.request_identity, False, wire.OutputPublicationRPCErrorKind.CONFLICT, "prepare rejected",
    )
    f.on_complete = lambda request: _completion(request, accepted=False)

    def wrong_outcome(request):
        witness = OutputPublicationCompleteWitness.for_manifest(f.prepared_request.manifest)
        return _outcome(f, request, witness=replace(witness, manifest_digest="0" * 64))

    f.on_outcome = wrong_outcome
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    assert f.pending.failure_reply is not None and f.pending.complete_envelope is None
    assert f.pending.nested_imports is imports and not child.closed
    assert f.key not in f.worker._completion_acked and f.key not in f.worker._replies
    assert f.executions == [True] and reductions == [True]


def _owner_cleanup_request(f, manifest=None):
    manifest = f.prepared_request.manifest if manifest is None else manifest
    death = protocol.WorkerDeathRecord(
        "exact-output-owner-exit",
        protocol.WorkerIncarnation(
            f.worker.node_id, f.incarnation.node_pid, f.incarnation.registration_epoch,
            f.push.spec.owner_worker_id, 32305,
        ),
        11, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    return wire.FinalizeOutputOwnerDeath(manifest, death)


def _owner_cleanup_lifecycle(worker):
    # Locks/notifications only; _no_runtime forbids every wait/start and the
    # constructors that could otherwise hide a Core or TCP service in a helper.
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._accepted_pushes = {}
    worker._push_obligations = set()
    worker._accepting_tasks = True
    worker._active_tasks = 0


def _owner_cleanup_fixture(monkeypatch, *, stage="pending", stored=False):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=0 if stored else 1024)
    _owner_cleanup_lifecycle(f.worker)

    def lost_ack(_request):
        raise TransportError("publication ACK lost before owner cleanup")

    if stage == "pending":
        f.on_prepare = lost_ack
    elif stage == "complete-unknown":
        f.on_complete = lost_ack
    elif stage == "aborted":
        f.on_prepare = lambda request: wire.PreparedOutputPublicationReply(
            request.request_identity, False, wire.OutputPublicationRPCErrorKind.INVALID_STATE,
            "publication compensated before owner death",
        )
    else:
        assert stage == "cached"
    if stage in ("pending", "complete-unknown"):
        with pytest.raises(RuntimeError, match="exact PushTask replay"):
            f.worker._handle_push_task(f.push)
    else:
        f.worker._handle_push_task(f.push)
    return f, child, imports, reductions, _owner_cleanup_request(f)


def _owner_cleanup_state(worker):
    mappings = (
        "_prepared_output_replies", "_replies", "_cached_pushes",
        "_cached_output_manifests", "_accepted_pushes", "_owner_abandoned_outputs",
        "_lease_bindings", "_attempt_leases",
    )
    custody = tuple(
        (key, pending.discovery.source_references, pending.discovery.discovered, pending.nested_imports)
        for key, pending in getattr(worker, "_prepared_output_replies", {}).items()
    )
    return (tuple(dict(getattr(worker, name, {})) for name in mappings), custody,
            set(worker._completion_acked), set(getattr(worker, "_push_obligations", ())))


def _assert_owner_cleanup_finished(f, request):
    worker = f.worker
    assert f.key not in getattr(worker, "_prepared_output_replies", {})
    assert f.key not in worker._replies and f.key not in worker._cached_pushes
    assert f.key not in getattr(worker, "_cached_output_manifests", {})
    assert f.key not in worker._accepted_pushes and f.key not in worker._push_obligations
    assert f.key not in worker._completion_acked
    assert worker._owner_abandoned_outputs[f.key] == request
    assert worker._lease_bindings[f.push.lease_id] == f.push.spec.attempt_id
    assert worker._attempt_leases[f.push.spec.attempt_id] == f.push.lease_id
    calls = tuple(f.calls)
    with pytest.raises(RuntimeError, match="output owner died"):
        worker._handle_push_task(f.push)
    assert worker._active_tasks == 0 and tuple(f.calls) == calls


@pytest.mark.parametrize("stage", ("pending", "complete-unknown", "cached", "aborted"))
@pytest.mark.parametrize("stored", (False, True))
def test_output_owner_death_retires_exact_custody_and_replays_lost_cleanup_ack(monkeypatch, stage, stored):
    f, child, imports, reductions, request = _owner_cleanup_fixture(monkeypatch, stage=stage, stored=stored)
    worker = f.worker
    pending = getattr(worker, "_prepared_output_replies", {}).get(f.key)
    if stage == "pending":
        assert pending.discovery.source_references == (child,)
        assert pending.nested_imports is imports and imports.acquired == (child,)
    if stage == "aborted":
        assert worker._replies[f.key].output_publication is None
        assert worker._cached_output_manifests[f.key] == request.manifest
    rpc_count = len(f.calls)
    acknowledged = []

    def lose_reply():
        acknowledged.append(worker._handle_finalize_output_owner_death(request))
        raise TransportError("owner cleanup took effect but its ACK was lost")

    with pytest.raises(TransportError, match="ACK was lost"):
        lose_reply()
    assert acknowledged[0].cleaned and acknowledged[0].request == request
    assert child.closed and child.closes == 1 and imports.acquired == ()
    if pending is not None:
        assert pending.discovery.source_references == () and pending.nested_imports is None
    _assert_owner_cleanup_finished(f, request)
    before = _owner_cleanup_state(worker)
    replay = worker._handle_finalize_output_owner_death(pickle.loads(pickle.dumps(request)))
    assert replay == acknowledged[0] and _owner_cleanup_state(worker) == before
    assert child.closes == 1 and len(f.calls) == rpc_count
    assert f.executions == [True] and reductions == [True]
    assert (f.prepared_request.payload not in pickle.dumps(worker._owner_abandoned_outputs[f.key]))


def _rebound_owner_cleanup(request, field):
    manifest, death = request.manifest, request.owner_death
    header, slots = manifest.header, ((manifest.value,))
    if field == "owner":
        owner = WorkerID.random()
        header = replace(header, owner_worker_id=owner)
        slots = tuple(replace(slot, transfers=tuple(
            replace(transfer, final_hold=replace(transfer.final_hold, container_owner_worker_id=owner))
            for transfer in slot.transfers
        )) for slot in slots)
        death = replace(death, incarnation=replace(death.incarnation, worker_id=owner))
    elif field == "lease":
        header = replace(header, publication_id=replace(manifest.publication_id, lease_id=LeaseID.random()))
    else:
        node = header.node_incarnation
        if field == "node":
            node = replace(node, node_id=NodeID.random())
        elif field == "node-pid":
            node = replace(node, node_pid=node.node_pid + 1)
        else:
            assert field == "node-epoch"
            node = replace(node, registration_epoch=node.registration_epoch + 1)
        header = replace(header, node_incarnation=node)
    return wire.FinalizeOutputOwnerDeath(OutputPublicationManifest.create(header, (slots[0])), death)


@pytest.mark.parametrize("stage", ("pending", "cached", "aborted", "retired"))
@pytest.mark.parametrize("field", ("owner", "lease", "node", "node-pid", "node-epoch"))
def test_output_owner_death_rejects_rebound_identity_without_touching_local_custody(monkeypatch, stage, field):
    f, child, _imports, _reductions, request = _owner_cleanup_fixture(
        monkeypatch, stage="pending" if stage == "retired" else stage,
    )
    if stage == "retired":
        assert f.worker._handle_finalize_output_owner_death(request).cleaned
    before, closes, calls = _owner_cleanup_state(f.worker), child.closes, tuple(f.calls)
    with pytest.raises(RuntimeError, match="output cleanup"):
        f.worker._handle_finalize_output_owner_death(_rebound_owner_cleanup(request, field))
    assert _owner_cleanup_state(f.worker) == before
    assert child.closes == closes and tuple(f.calls) == calls
    assert f.worker._handle_finalize_output_owner_death(request).cleaned
    _assert_owner_cleanup_finished(f, request)


@pytest.mark.parametrize("field", ("worker-pid", "node-epoch", "death-epoch", "detection"))
def test_output_owner_death_rejects_changed_frozen_death_after_payload_retirement(monkeypatch, field):
    f, child, _imports, _reductions, request = _owner_cleanup_fixture(monkeypatch)
    assert f.worker._handle_finalize_output_owner_death(request).cleaned
    death = request.owner_death
    if field == "worker-pid":
        death = replace(death, incarnation=replace(death.incarnation, worker_pid=death.worker_pid + 1))
    elif field == "node-epoch":
        death = replace(death, incarnation=replace(
            death.incarnation, node_registration_epoch=death.node_registration_epoch + 1,
        ))
    elif field == "death-epoch":
        death = replace(death, death_epoch=death.death_epoch + 1)
    else:
        death = replace(death, detection_id="another-owner-exit")
    before = _owner_cleanup_state(f.worker)
    with pytest.raises(RuntimeError, match="frozen owner-death"):
        f.worker._handle_finalize_output_owner_death(replace(request, owner_death=death))
    assert _owner_cleanup_state(f.worker) == before and child.closes == 1
    assert f.worker._handle_finalize_output_owner_death(request).cleaned


@pytest.mark.parametrize("phase", ("source", "imports"))
@pytest.mark.parametrize("after_effect", (False, True))
def test_output_owner_death_cleanup_error_preserves_exact_retry_and_fences_push_and_drain(
    monkeypatch, phase, after_effect,
):
    f, child, imports, reductions, request = _owner_cleanup_fixture(monkeypatch)
    worker, pending = f.worker, f.pending
    abort, close = OutputDiscoverySession.abort, imports.close
    failures = []

    def invoke(label, callback):
        assert worker._owner_abandoned_outputs[f.key] == request
        if label == phase and not failures:
            failures.append(label)
            if after_effect:
                callback()
            raise RuntimeError("local owner cleanup failed")
        callback()

    monkeypatch.setattr(OutputDiscoverySession, "abort", lambda discovery: invoke("source", lambda: abort(discovery)))
    monkeypatch.setattr(imports, "close", lambda: invoke("imports", close))
    rpc_count = len(f.calls)
    with pytest.raises(RuntimeError, match="local owner cleanup failed"):
        worker._handle_finalize_output_owner_death(request)
    assert f.pending is pending and pending.nested_imports is imports
    assert f.key in worker._push_obligations and f.key in worker._accepted_pushes
    assert child.closes == int(phase == "imports" and after_effect)
    assert pending.discovery.source_references == ((child,) if phase == "source" and not after_effect else ())
    with pytest.raises(RuntimeError, match="output owner died"):
        worker._handle_push_task(f.push)
    with pytest.raises(RuntimeError, match="frozen owner-death"):
        worker._handle_finalize_output_owner_death(replace(
            request, owner_death=replace(request.owner_death, death_epoch=12),
        ))
    worker._drain_request_id = "retry-owner-cleanup-only"
    worker._accepting_tasks = False
    assert not worker._drain_status(worker._drain_request_id, timeout=0.1).clean
    assert f.pending is pending and len(f.calls) == rpc_count
    assert worker._handle_finalize_output_owner_death(request).cleaned
    _assert_owner_cleanup_finished(f, request)
    assert child.closes == 1 and pending.nested_imports is None
    assert worker._handle_finalize_output_owner_death(request).cleaned
    assert f.executions == [True] and reductions == [True] and len(f.calls) == rpc_count


@pytest.mark.parametrize("conflict", ("cached-reply", "pending-complete"))
def test_output_owner_death_preflights_all_local_manifests_before_source_release(monkeypatch, conflict):
    f, child, imports, _reductions, request = _owner_cleanup_fixture(monkeypatch)
    pending = f.pending
    actual = _envelope(f.prepared_request)
    changed = _rebound_owner_cleanup(request, "node-epoch").manifest
    wrong = OutputPublicationEnvelope(changed, OutputPublicationCompleteWitness.for_manifest(changed), (actual.result))
    if conflict == "pending-complete":
        pending.complete_envelope = wrong
    else:
        f.worker._replies[f.key] = protocol.TaskReply(
            f.push.spec.task_id, f.push.spec.attempt_id, f.worker.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED, results=((wrong.result,)), output_publication=wrong,
        )
        f.worker._cached_pushes[f.key] = f.push
    before = _owner_cleanup_state(f.worker)
    with pytest.raises(RuntimeError, match="retained publication manifest"):
        f.worker._handle_finalize_output_owner_death(request)
    assert _owner_cleanup_state(f.worker) == before and child.closes == 0
    assert pending.discovery.source_references == (child,) and pending.nested_imports is imports
    if conflict == "pending-complete":
        pending.complete_envelope = actual
    else:
        f.worker._replies[f.key] = replace(f.worker._replies[f.key], output_publication=actual)
    assert f.worker._handle_finalize_output_owner_death(request).cleaned
    _assert_owner_cleanup_finished(f, request)


def test_output_owner_death_busy_execution_lock_is_nonblocking_and_does_not_retire_custody(monkeypatch):
    f, child, _imports, _reductions, request = _owner_cleanup_fixture(monkeypatch)
    before = _owner_cleanup_state(f.worker)
    assert f.worker._execution_lock.acquire(blocking=False)
    try:
        reply = f.worker._handle_finalize_output_owner_death(request)
        assert reply == wire.FinalizeOutputOwnerDeathReply(request, False)
        assert _owner_cleanup_state(f.worker) == before and child.closes == 0
    finally:
        f.worker._execution_lock.release()
    assert f.worker._handle_finalize_output_owner_death(request).cleaned
    _assert_owner_cleanup_finished(f, request)


@pytest.mark.parametrize("wrong_executor", (False, True))
def test_output_owner_death_rejects_unretained_or_wrong_executor_without_poisoning_push(monkeypatch, wrong_executor):
    f = _Fixture(monkeypatch, lambda: 7)
    _owner_cleanup_lifecycle(f.worker)
    outputs = f.worker._output_discovery_session(f.push, f.incarnation).discover((7))
    manifest = outputs.manifest
    if wrong_executor:
        manifest = OutputPublicationManifest.create(
            replace(manifest.header, executor_worker_id=WorkerID.random()), (manifest.value),
        )
    before = _owner_cleanup_state(f.worker)
    with pytest.raises(RuntimeError, match="another executor" if wrong_executor else "no retained publication"):
        f.worker._handle_finalize_output_owner_death(_owner_cleanup_request(f, manifest))
    assert _owner_cleanup_state(f.worker) == before and f.calls == []
    assert f.worker._handle_push_task(f.push).status is protocol.TaskReplyStatus.SUCCEEDED
    assert f.executions == [True]


@pytest.mark.parametrize("stage", ("pending", "cached"))
def test_output_owner_death_fences_push_admitted_before_cleanup_acquires_replay_lock(monkeypatch, stage):
    f, child, _imports, reductions, request = _owner_cleanup_fixture(monkeypatch, stage=stage)
    begin = f.worker._begin_task
    rpc_count = len(f.calls)

    def admit_then_finalize(push):
        assert begin(push)
        assert f.worker._active_tasks == 1
        assert f.worker._handle_finalize_output_owner_death(request).cleaned
        return True

    monkeypatch.setattr(f.worker, "_begin_task", admit_then_finalize)
    with pytest.raises(RuntimeError, match="output owner died"):
        f.worker._handle_push_task(f.push)
    monkeypatch.setattr(f.worker, "_begin_task", begin)
    _assert_owner_cleanup_finished(f, request)
    assert child.closes == 1 and len(f.calls) == rpc_count
    assert f.executions == [True] and reductions == [True]


def test_output_owner_death_fences_execution_after_empty_cache_probe_without_threads(monkeypatch):
    f, child, _imports, reductions = _borrowed_fixture(monkeypatch)
    _owner_cleanup_lifecycle(f.worker)
    admitted = f.worker._handle_admitted_push_task
    cleanups = []

    def finish_other_admission_then_finalize(push):
        # Deterministically model another admitted handler completing after this
        # Push's empty-cache probe, followed by cleanup before its execution lock.
        assert admitted(push).status is protocol.TaskReplyStatus.SUCCEEDED
        request = _owner_cleanup_request(f)
        cleanups.append(request)
        assert f.worker._handle_finalize_output_owner_death(request).cleaned
        return admitted(push)

    monkeypatch.setattr(f.worker, "_handle_admitted_push_task", finish_other_admission_then_finalize)
    with pytest.raises(RuntimeError, match="output owner died"):
        f.worker._handle_push_task(f.push)
    _assert_owner_cleanup_finished(f, cleanups[0])
    assert f.handlers == [START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER, COMPLETE_WORKER_LEASE_HANDLER]
    assert child.closes == 1 and f.executions == [True] and reductions == [True]


def test_output_owner_death_only_retires_its_exact_publication_and_leaves_other_owner_pending(monkeypatch):
    f = _Fixture(monkeypatch, lambda: 7)
    _owner_cleanup_lifecycle(f.worker)

    def unavailable(_request):
        raise TransportError("both owners retain their own pending batch")

    f.on_prepare = unavailable
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    first_push, first_key = f.push, f.key
    request = _owner_cleanup_request(f)
    f.push = _push(f.worker.worker_id, b"unified-output-function")
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    second_push, second_key, second_pending = f.push, f.key, f.pending
    assert f.worker._push_obligations == {first_key, second_key}
    assert f.worker._handle_finalize_output_owner_death(request).cleaned
    f.push = first_push
    _assert_owner_cleanup_finished(f, request)
    assert f.worker._prepared_output_replies == {second_key: second_pending}
    assert f.worker._accepted_pushes == {second_key: second_push}
    assert f.worker._push_obligations == {second_key}
    assert f.worker._owner_abandoned_outputs == {first_key: request}
    f.push = second_push
    f.on_prepare = None
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert reply.output_publication.manifest == second_pending.outputs.manifest
    assert f.executions == [True, True] and f.worker._push_obligations == set()
