"""Pure Actor result regression checks, separate from task publication.

Resource review: one threadless Core, one ActorClientTable, at most two method
calls/output handles per case, and three-byte payloads. Actor endpoints and
death proofs are inert metadata; one synchronous callback replaces Actor RPC.
No submit_actor_call, Core constructor, actor code, network, process, thread,
timer, sleep, polling, or blocking wait runs. Real local finalizers release
handles, and is_set (never wait) verifies their synchronous completion.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
import hashlib
import multiprocessing as mp
import os
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.actor_client import ActorClientTable
from miniray.contained_edges import ContainedReferenceEdge
from miniray.core import _ObjectWaiter
from miniray.errors import ActorDiedError, SystemTaskError, TaskError
from miniray.ids import (
    ActorGeneration, ActorID, AttemptID, LeaseID, NodeID, ObjectID, TaskID, WorkerID,
)
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputSlotManifest,
)
from miniray.ownership import ObjectState
from miniray.recovery import UnknownTaskError
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest
from tests.unit._pure_core import make_pure_core, close_pure_core


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    violations = []

    def forbidden(*_args, **_kwargs):
        violations.append("runtime boundary")
        raise AssertionError("pure Actor publication test attempted runtime work")

    for owner, attribute in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"), (socket, "socket"),
        (socket, "create_connection"), (subprocess, "Popen"),
        (mp.Process, "start"), (time, "sleep"),
    ):
        monkeypatch.setattr(owner, attribute, forbidden)
    if hasattr(os, "fork"):
        monkeypatch.setattr(os, "fork", forbidden)
    yield
    # Dispatcher catches BaseException to publish call errors; recording the
    # violation makes a swallowed tripwire fail negative-result tests too.
    assert not violations


def _id(kind, value):
    return kind(bytes((value,)) * 16)


@dataclass(frozen=True)
class _Call:
    endpoint: object
    request: protocol.ActorCallRequest
    ref: object
    fence: object


class _Fixture:
    def __init__(self):
        self.core = make_pure_core()
        self.core._actor_clients = ActorClientTable()
        actor = _id(ActorID, 1)
        self.snapshot = protocol.ActorSnapshot(
            actor, ActorGeneration(actor, 0), protocol.ActorState.ALIVE,
            1, 0, 1, node_id=_id(NodeID, 2), worker_id=_id(WorkerID, 3),
            worker_address=("actor.invalid", 1), worker_pid=1201,
        )
        self.core._actor_clients.register(self.snapshot, ("value",))
        self.calls = []
        self.effects = []
        self.dispatched = []
        self.published = []

        def forbidden_effect(*_args, **_kwargs):
            self.effects.append("non-Actor effect")
            raise AssertionError("Actor result entered task publication/runtime")

        # Check the architectural split, not only the eventual READY state.
        for name in (
            "_rpc", "_borrow_rpc", "_borrow_rpc_with_deadline", "_push_task_rpc",
            "_publish_reply", "_drive_output_publication_adoption",
            "_drive_output_node_loss", "_retry_system_failure",
            "submit_actor_call",
        ):
            setattr(self.core, name, forbidden_effect)
        self.core.owner_table.validate_output_publication = forbidden_effect
        self.core.owner_table.commit_output_publication = forbidden_effect
        publish = self.core._publish_actor_reply

        def publish_actor(pending, reply, *, expected_node_id=None):
            self.published.append((pending, reply, expected_node_id))
            assert pending.target_execution is None
            assert pending.output_ids == (pending.object_id,)
            assert pending.task_id == reply.task_id
            assert pending.spec.attempt_id == reply.attempt_id
            assert self.core.owner_table.snapshot(pending.object_id).producer_task_spec is None
            return publish(pending, reply, expected_node_id=expected_node_id)

        self.core._publish_actor_reply = publish_actor

    def begin(self):
        assert len(self.calls) < 2
        task = TaskID.derive(self.core.job_id, self.core.driver_task_id, len(self.calls))
        attempt = AttemptID(task, 0)
        output = ObjectID.for_task(task)
        self.core.owner_table.register(output, current_attempt=attempt, producer_task_spec=None)
        self.core._objects[output] = _ObjectWaiter(threading.Event())
        ref = self.core._new_object_ref(output)
        snapshot, sequence, fence = self.core._actor_clients.begin_call(self.snapshot.actor_id, output)
        endpoint = self.core._endpoint_from_actor_snapshot(snapshot, ("value",))
        request = protocol.ActorCallRequest(
            snapshot.actor_id, snapshot.generation, self.core.worker_id, sequence,
            "value", task, attempt, self.core.worker_id, b"()",
            snapshot.worker_id, snapshot.route_epoch,
        )
        call = _Call(endpoint, request, ref, fence)
        self.calls.append(call)
        return call

    def reply(self, call, storage=protocol.ResultStorage.INLINE, *, payload=b"old"):
        request, endpoint = call.request, call.endpoint
        descriptor = protocol.ResultDescriptor(
            call.ref.object_id, storage, len(payload), self.core.worker_id,
            endpoint.node_id, hashlib.sha256(payload).hexdigest(),
            payload if storage is protocol.ResultStorage.INLINE else None,
        )
        return protocol.ActorCallReply(
            request.actor_id, request.generation, request.caller_worker_id, request.sequence,
            protocol.TaskReply(
                request.task_id, request.attempt_id, endpoint.worker_id,
                protocol.TaskReplyStatus.SUCCEEDED, (descriptor,),
            ),
            request.route_epoch,
        )

    def dispatch(self, call, reply, *, before_reply=None):
        trace_start = len(self.dispatched)
        assert not hasattr(reply.task_reply, "stored_publication")
        assert not hasattr(reply.task_reply, "inline_publication")
        assert not hasattr(reply.task_reply, "contained_edges")

        def actor_rpc(endpoint, request, output, fence):
            assert endpoint == call.endpoint and request == call.request
            assert output == call.ref.object_id and fence == call.fence
            self.dispatched.append(call)
            if before_reply is not None:
                before_reply()
            return reply

        self.core._actor_call_rpc = actor_rpc
        self.core._dispatch_actor_call(call.endpoint, call.request, call.ref.object_id, call.fence)
        assert not self.effects
        assert self.dispatched[trace_start] is call
        assert not self.core._actor_call_threads
        assert not self.core._actor_clients.can_publish(call.ref.object_id, call.fence)
        assert not self.core._actor_clients.finish_call(call.ref.object_id, call.fence)

    def assert_no_lineage_or_graph(self):
        for call in self.calls:
            owner = self.core.owner_table.snapshot(call.ref.object_id)
            assert owner.producer_task_spec is None
            assert owner.output_publication is None
            assert owner.output_retirement_id is None
            assert all(not hasattr(owner, name) for name in (
                "inline_publication", "stored_publication", "stored_retirement_id",
            ))
            assert not owner.outgoing_contained_edges and not owner.contained_holds
            assert self.core._recovery.lineage_for_object(call.ref.object_id) is None
            with pytest.raises(UnknownTaskError):
                self.core._recovery.task_record(call.request.task_id)
        assert self.core._accepted_task_count == 0
        assert not self.core._task_finish_barriers and not self.core._protocol_unresolved
        assert not hasattr(self.core, "_orphan_contained_edges")
        assert not hasattr(self.core, "_dispose_reply_contained_edges")
        assert not hasattr(self.core, "_retain_orphan_contained_edges")
        assert not getattr(self.core, "_output_result_custody", {})
        assert not self.effects


@contextmanager
def _case():
    values = _Fixture()
    try:
        yield values
    finally:
        for call in values.calls:
            call.ref._closed = True
            call.ref._finalizer()
            assert call.ref._release_done.is_set()
        close_pure_core(values.core)


@pytest.mark.parametrize("storage", (protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE), ids=("inline", "stored"))
def test_actor_success_uses_descriptor_path_without_task_lineage(storage):
    with _case() as values:
        call = values.begin()
        reply = values.reply(call, storage)
        values.dispatch(call, reply)
        owner = values.core.owner_table.snapshot(call.ref.object_id)
        descriptor = reply.task_reply.results[0]
        assert owner.current_attempt == call.request.attempt_id and owner.error is None
        assert values.core._objects[call.ref.object_id].event.is_set()
        if storage is protocol.ResultStorage.INLINE:
            assert owner.state is ObjectState.READY_INLINE and owner.inline_data == b"old"
            assert owner.canonical_stored_result is None and not owner.locations
            assert call.ref.object_id not in values.core._stored_descriptors
        else:
            assert owner.state is ObjectState.READY_STORED and owner.inline_data is None
            assert owner.canonical_stored_result == descriptor
            assert owner.locations == frozenset((call.endpoint.node_id,))
            assert values.core._stored_descriptors[call.ref.object_id] == descriptor
        assert len(values.published) == 1
        assert values.published[0][2] == call.endpoint.node_id
        values.assert_no_lineage_or_graph()


def test_actor_application_failure_is_typed_without_task_retry_state():
    with _case() as values:
        call = values.begin()
        reply = values.reply(call)
        remote = protocol.RemoteErrorInfo("ValueError", "actor rejected value", "actor-trace")
        reply = replace(reply, task_reply=replace(
            reply.task_reply, status=protocol.TaskReplyStatus.APPLICATION_ERROR,
            results=(), error=remote,
        ))
        values.dispatch(call, reply)
        owner = values.core.owner_table.snapshot(call.ref.object_id)
        assert owner.state is ObjectState.ERROR and type(owner.error) is TaskError
        assert owner.error.remote_type == remote.type_name
        assert owner.error.remote_message == remote.message
        assert owner.error.remote_traceback == remote.traceback
        assert owner.inline_data is None and owner.canonical_stored_result is None
        assert not owner.locations and call.ref.object_id not in values.core._stored_descriptors
        assert values.core._objects[call.ref.object_id].event.is_set()
        assert len(values.published) == 1
        values.assert_no_lineage_or_graph()


@pytest.mark.parametrize("mismatch", ("generation", "worker", "route", "result-node", "result-owner"))
def test_actor_wrong_identity_never_publishes_result_or_changes_another_call(mismatch):
    with _case() as values:
        other = values.begin()
        values.dispatch(other, values.reply(other, payload=b"one"))
        stable = values.core.owner_table.snapshot(other.ref.object_id)
        call = values.begin()
        reply = values.reply(call)
        if mismatch == "generation":
            reply = replace(reply, generation=reply.generation.next())
        elif mismatch == "route":
            reply = replace(reply, route_epoch=reply.route_epoch + 1)
        elif mismatch == "worker":
            reply = replace(reply, task_reply=replace(reply.task_reply, worker_id=_id(WorkerID, 4)))
        else:
            descriptor = reply.task_reply.results[0]
            descriptor = (replace(descriptor, node_id=_id(NodeID, 5)) if mismatch == "result-node"
                          else replace(descriptor, owner_worker_id=_id(WorkerID, 4)))
            reply = replace(reply, task_reply=replace(reply.task_reply, results=(descriptor,)))
        values.dispatch(call, reply)
        owner = values.core.owner_table.snapshot(call.ref.object_id)
        assert owner.state is ObjectState.ERROR and type(owner.error) is SystemTaskError
        assert owner.inline_data is None and owner.canonical_stored_result is None
        assert not owner.locations and call.ref.object_id not in values.core._stored_descriptors
        assert values.core.owner_table.snapshot(other.ref.object_id) == stable
        assert values.core._actor_clients.snapshot(values.snapshot.actor_id) == values.snapshot
        values.assert_no_lineage_or_graph()


@pytest.mark.parametrize("change", ("generation", "worker", "route"))
def test_actor_route_fence_keeps_authoritative_error_and_new_call_result(change):
    with _case() as values:
        old = values.begin()
        old_reply = values.reply(old)
        initial = values.snapshot
        fields_to_change = {"route_epoch": initial.route_epoch + 1}
        if change == "worker":
            fields_to_change.update(worker_id=_id(WorkerID, 6), worker_address=("actor-next.invalid", 2), worker_pid=1202)
        elif change == "generation":
            exit_record = protocol.ActorWorkerExitRecord(
                "actor-route-exit", initial.actor_id, initial.generation, initial.route_epoch,
                initial.node_id, 1101, 1, initial.worker_id, initial.worker_pid, 1,
            )
            fields_to_change.update(generation=initial.generation.next(), restarts_used=1, last_exit=exit_record)
        latest = replace(initial, **fields_to_change)
        captured = []
        replacements = []

        def revoke_before_delivery():
            assert values.core._install_actor_snapshot(latest)
            snapshot = values.core.owner_table.snapshot(old.ref.object_id)
            assert snapshot.state is ObjectState.ERROR and type(snapshot.error) is ActorDiedError
            captured.append(snapshot)
            assert values.published == []
            new = values.begin()
            assert new.fence != old.fence
            # A synchronous second callback models the new route finishing
            # before the old reply reaches its publication fence.
            values.dispatch(new, values.reply(new, payload=b"new"))
            fresh = values.core.owner_table.snapshot(new.ref.object_id)
            assert fresh.state is ObjectState.READY_INLINE and fresh.inline_data == b"new"
            replacements.append((new, fresh))

        values.dispatch(old, old_reply, before_reply=revoke_before_delivery)
        assert values.core.owner_table.snapshot(old.ref.object_id) == captured[0]
        new, fresh = replacements[0]
        assert values.core.owner_table.snapshot(new.ref.object_id) == fresh
        assert values.core._actor_clients.snapshot(initial.actor_id) == latest
        assert values.dispatched == [old, new]
        assert len(values.published) == 1
        values.assert_no_lineage_or_graph()


@pytest.mark.parametrize("extra", ("output-envelope", "contained-edge"))
def test_actor_rejects_task_publication_authority_without_graph_side_effects(extra):
    with _case() as values:
        call = values.begin()
        reply = values.reply(call)
        descriptor = reply.task_reply.results[0]
        if extra == "output-envelope":
            execution = TaskExecutionKey(TaskOutputManifest.for_task(call.request.task_id, 1), call.request.attempt_id)
            identity = OutputPublicationID(_id(LeaseID, 7), execution)
            header = OutputPublicationHeader(
                identity, values.core.job_id, call.endpoint.worker_id, values.core.worker_id,
                OutputPublicationNodeIncarnation(call.endpoint.node_id, 1101, 1),
            )
            manifest = OutputPublicationManifest.create(header, (OutputSlotManifest(
                descriptor.object_id, descriptor.storage, descriptor.size_bytes, descriptor.checksum,
            ),))
            envelope = OutputPublicationEnvelope(
                manifest, OutputPublicationCompleteWitness.for_manifest(manifest), (descriptor,),
            )
            task_reply = replace(reply.task_reply, output_publication=envelope)
        else:
            # Metadata-only invalid Actor output: no child owner/hold exists.
            edge = ContainedReferenceEdge(
                call.ref.object_id, ObjectID.for_task(_id(TaskID, 8)),
                call.endpoint.worker_id, call.endpoint.worker_address, "forbidden-actor-hold",
            )
            before = values.core.owner_table.snapshot(call.ref.object_id)
            with pytest.raises(TypeError, match="unexpected keyword argument.*contained_edges"):
                replace(reply.task_reply, contained_edges=(edge,))
            assert values.core.owner_table.snapshot(call.ref.object_id) == before
            assert before.state is ObjectState.PENDING
            assert values.core._actor_clients.can_publish(call.ref.object_id, call.fence)
            assert values.dispatched == values.published == values.effects == []
            values.assert_no_lineage_or_graph()
            assert values.core._actor_clients.finish_call(call.ref.object_id, call.fence)
            return
        values.dispatch(call, replace(reply, task_reply=task_reply))
        owner = values.core.owner_table.snapshot(call.ref.object_id)
        assert owner.state is ObjectState.ERROR and type(owner.error) is SystemTaskError
        assert owner.inline_data is None and owner.canonical_stored_result is None
        assert call.ref.object_id not in values.core._stored_descriptors
        values.assert_no_lineage_or_graph()
