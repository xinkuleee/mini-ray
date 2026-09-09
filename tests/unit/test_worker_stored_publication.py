"""Pure Worker/Node contracts for the single-output owner-led publication.

The historical filename no longer exercises a Worker-owned stored saga.
Workers discover once and retain custody; the real Node adapter owns every
publication effect. Existing fixtures provide only in-memory RPC callbacks,
one output, at most two child transfers, and a 1 KiB Node store.
No Core, server, process, thread, socket or background waiter is started.
"""

from __future__ import annotations

from dataclasses import replace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import output_protocol as wire, protocol
from miniray.core import ObjectRef
from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import LeaseID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationConflictError, OutputPublicationManifest,
)
from miniray.ownership import ConflictingBorrowerTokenError, DeadWorkerReferenceError
from miniray.publication_sources import BorrowedContainedSource, OwnedContainedSource
from miniray.transport import TransportError
from miniray.worker import COMPLETE_WORKER_LEASE_HANDLER, START_WORKER_LEASE_HANDLER
from tests.unit.test_output_publication_node_server import _node
from tests.unit.test_worker_unified_output import (
    _Fixture as _WorkerFixture, _borrowed_fixture,
)


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("unified Worker/Node contract attempted real runtime work")

    for kind, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for target in (
        "miniray.core.CoreWorker.__init__",
        "miniray.worker.WorkerServer.__init__",
        "miniray.node.NodeServer.__init__",
        "miniray.worker.TCPServer.__init__",
        "miniray.worker.rpc_request", "miniray.node.rpc_request",
        "miniray.core.rpc_request", "miniray.transport.request",
    ):
        monkeypatch.setattr(target, forbidden)



class _NodeFixture:
    """Use the existing exact Node lease/journal/owner fixture, not a new backend."""

    def __init__(self, *, stored=True):
        self.fixture, self.node, self.record, self.completion = _node(stored=stored)
        self.values = self.fixture.values
        self.manifest, self.id = self.fixture.manifest, self.fixture.id
        self.adapter, self.journal = self.fixture.adapter, self.fixture.journal
        self.child_owners, self.store = self.fixture.child_owners, self.fixture.store
        self.events, self.ledger = self.fixture.events, self.fixture.ledger

    def prepare(self):
        request = wire.PrepareOutputPublication(self.manifest, self.values.payloads)
        assert self.node._handle_prepare_output_publication(request).accepted

    def complete(self):
        reply = self.node._handle_complete_worker_lease_inner(self.completion)
        assert reply.accepted
        return reply.output_publication


@pytest.mark.parametrize("stored", (False, True))
def test_worker_discovers_once_without_rpc_and_preserves_complete_sources(monkeypatch, stored):
    worker = _WorkerFixture(monkeypatch, lambda: None, threshold=0 if stored else 65536)
    owned = ObjectRef(ObjectID.for_task(TaskID.random()), worker.worker.worker_id,
                      worker.worker.address)
    borrowed = ObjectRef(ObjectID.for_task(TaskID.random()), WorkerID.random(),
                         ("127.0.0.1", 30140))
    source = protocol.ContainedTransferSource(ContainedReferenceHold(
        ObjectID.for_task(TaskID.random()), borrowed.owner_worker_id, "upstream-borrowed-child"))
    borrowed._borrower_token = "accepted-borrower"
    borrowed._borrow_source = source
    reductions = []

    class Once:
        def __init__(self, name, child):
            self.name, self.child = name, child

        def __reduce__(self):
            reductions.append(self.name)
            assert worker.calls == []
            return dict, (((self.name, self.child), ("alias", self.child)),)

    discovery = worker.worker._output_discovery_session(worker.push, worker.incarnation)
    values = (Once("owned", owned), Once("borrowed", borrowed))
    batch = discovery.discover((values,))
    assert reductions == ["owned", "borrowed"]
    assert worker.calls == worker.prepares == worker.executions == []
    assert discovery.source_references == (owned, borrowed)
    assert discovery.discovered is batch
    assert batch.manifest.header.node_incarnation == worker.incarnation
    slot, = batch.manifest.slots
    assert slot.object_id.return_index == 0
    assert slot.tier is (protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE)
    first, second = slot.transfers
    assert first.source == OwnedContainedSource(worker.worker.worker_id)
    assert second.source == BorrowedContainedSource(worker.worker.worker_id, borrowed.borrower_token, source)
    assert second.contained_owner_address == borrowed.owner_address
    for transfer in slot.transfers:
        assert transfer.provisional_hold.container_owner_worker_id == worker.worker.worker_id
        assert transfer.final_hold.container_owner_worker_id == worker.push.spec.owner_worker_id
        assert transfer.final_hold.container_object_id == slot.object_id
        assert transfer.final_hold.transfer_token.encode() in batch.slot_payloads[0]
    with pytest.raises(RuntimeError, match="one-shot"):
        discovery.discover((values,))
    assert reductions == ["owned", "borrowed"]
    discovery.release_sources_after_promotions()
    discovery.release_sources_after_promotions()
    assert discovery.source_references == () and worker.calls == []


@pytest.mark.parametrize("invalid, error, detail", (
    ("endpoint", ValueError, "detached from its owner endpoint"),
    ("token", ValueError, "live borrower credential"),
    ("source", ValueError, "live borrower credential"),
    ("closed", RuntimeError, "closed ObjectRef"),
))
def test_invalid_borrowed_handle_is_rejected_during_zero_effect_discovery(
    monkeypatch, invalid, error, detail,
):
    worker, child, _imports, reductions = _borrowed_fixture(monkeypatch, threshold=0)
    field = {
        "endpoint": "_owner_address", "token": "_borrower_token",
        "source": "_borrow_source", "closed": "_closed",
    }[invalid]
    setattr(child, field, True if invalid == "closed" else None)
    discovery = worker.worker._output_discovery_session(worker.push, worker.incarnation)
    with pytest.raises(error, match=detail):
        discovery.discover((child,))
    assert discovery.discovered is None and discovery.source_references == ()
    assert worker.calls == worker.executions == reductions == []
    with pytest.raises(RuntimeError, match="one-shot"):
        discovery.discover((child,))


@pytest.mark.parametrize("invalid", ("source", "token", "released", "dead-executor"))
def test_child_owner_rejects_stale_or_rebound_borrowed_capability_without_a_pin(invalid):
    node = _NodeFixture()
    transfer = node.manifest.slots[0].transfers[1]
    table = node.child_owners[transfer.contained_owner_worker_id]
    assert isinstance(transfer.source, BorrowedContainedSource)
    if invalid == "source":
        transfer = replace(transfer, source=replace(
            transfer.source, original_source=protocol.ContainedTransferSource(ContainedReferenceHold(
                ObjectID.for_task(TaskID.random()), node.values.owner, "another-source")),
        ))
    elif invalid == "token":
        transfer = replace(transfer, source=replace(transfer.source, borrower_token="another-token"))
    elif invalid == "released":
        assert table.release_borrowed_reference(
            transfer.contained_object_id, transfer.source.owner_table_token,
        )
    else:
        # Install a pure authoritative fact; this test never infers OS death.
        table.install_dead_worker(transfer.source.borrower_worker_id, "confirmed-executor-death")
    before = table.snapshot(transfer.contained_object_id)
    error = DeadWorkerReferenceError if invalid == "dead-executor" else ConflictingBorrowerTokenError
    with pytest.raises(error):
        table.prepare_stored_contained_reference(
            transfer, authority_worker_id=transfer.contained_owner_worker_id,
        )
    assert table.snapshot(transfer.contained_object_id) == before
    assert transfer.provisional_hold not in before.contained_holds
    assert transfer.final_hold not in before.contained_holds
    assert node.events == [] and node.store.used_bytes == 0


@pytest.mark.parametrize("stored", (False, True))
def test_node_owns_owner_prepare_materialize_promote_order(monkeypatch, stored):
    node = _NodeFixture(stored=stored)
    real_ack = node.journal.ack_materialized

    def materialized(ack, descriptor):
        result = real_ack(ack, descriptor)
        node.events.append("materialize:{}".format(ack.effect.slot_index))
        return result

    monkeypatch.setattr(node.journal, "ack_materialized", materialized)
    node.prepare()
    assert node.events == ["owner-register", "prepare", "prepare", "materialize:0", "promote", "promote"]
    snapshot = node.journal.snapshot(node.id)
    assert snapshot.ready_to_complete and snapshot.complete is None
    assert node.fixture.handoffs.query(node.id).manifest == node.manifest
    assert node.fixture.handoffs.query(node.id).complete is None
    assert node.record.state is protocol.LeaseExecutionState.RUNNING
    for transfer in node.manifest.slots[0].transfers:
        holds = node.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id).contained_holds
        assert transfer.final_hold in holds and transfer.provisional_hold not in holds
    assert node.complete() == node.values.envelope
    assert node.record.state is protocol.LeaseExecutionState.COMPLETED
    assert node.ledger.available == node.ledger.total
    assert "complete-report" not in node.events
    assert node.adapter.pending_terminal_reports() == (node.values.witness,)


@pytest.mark.parametrize("stage", ("owner-register", "prepare", "seal", "promote"))
def test_node_effect_ack_loss_replays_exact_metadata_not_worker_steps(monkeypatch, stage):
    node = _NodeFixture()
    attribute = {"owner-register": "_register_owner", "prepare": "_prepare_child",
                 "seal": "_seal_replica", "promote": "_promote_child"}[stage]
    callback = getattr(node.adapter, attribute)
    calls = []

    def observe(*args):
        calls.append(args)
        result = callback(*args)
        if len(calls) == 1:
            raise TimeoutError("effect ACK lost: " + stage)
        return result

    monkeypatch.setattr(node.adapter, attribute, observe)
    with pytest.raises(TimeoutError, match=stage):
        node.prepare()
    assert node.journal.snapshot(node.id).complete is None
    assert node.record.state is protocol.LeaseExecutionState.RUNNING
    node.prepare()
    assert calls[0] == calls[1]
    before = tuple(node.events)
    node.prepare()
    assert tuple(node.events) == before
    assert node.complete() == node.values.envelope
    assert node.ledger.available == node.ledger.total


@pytest.mark.parametrize("phase", ("prepare", "complete"))
def test_worker_ack_loss_keeps_once_bytes_and_exact_borrower_source(monkeypatch, phase):
    worker, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=0)
    source = BorrowedContainedSource(worker.worker.worker_id, child.borrower_token, child.borrow_source)
    lost_requests = []

    def lose_ack(request):
        lost_requests.append(request)
        assert worker.key not in worker.worker._replies
        assert worker.pending.outputs.manifest.slots[0].transfers[0].source == source
        if phase == "prepare":
            assert not child.closed and worker.pending.nested_imports is imports
        else:
            assert child.closed and worker.pending.nested_imports is None
        raise TransportError("exact Node acknowledgement was lost")

    setattr(worker, "on_" + phase, lose_ack)
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        worker.worker._handle_push_task(worker.push)
    pending = worker.pending
    batch = pending.outputs
    assert worker.key not in worker.worker._completion_acked
    if phase == "prepare":
        assert COMPLETE_WORKER_LEASE_HANDLER not in worker.handlers
    else:
        assert len(worker.prepares) == 1
    setattr(worker, "on_" + phase, None)
    reply = worker.worker._handle_push_task(worker.push)
    assert pending.outputs is batch and reply.output_publication.manifest == batch.manifest
    assert worker.executions == reductions == [True]
    assert child.closed and child.closes == 1
    assert all(request == worker.prepares[0] for request in worker.prepares)
    assert all(request.slot_payloads == batch.slot_payloads for request in worker.prepares)
    handler = wire.PREPARE_OUTPUT_PUBLICATION_HANDLER if phase == "prepare" else COMPLETE_WORKER_LEASE_HANDLER
    assert all(request == lost_requests[0] for called, request in worker.calls if called == handler)
    assert not hasattr(reply, "inline_publication")
    assert not hasattr(reply, "stored_publication")


@pytest.mark.parametrize("changed", ("lease", "digest"))
def test_wrong_batch_ack_cannot_advance_to_complete_or_release_sources(monkeypatch, changed):
    worker, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=0)

    def wrong_ack(request):
        identity = request.request_identity
        if changed == "lease":
            identity = replace(identity, publication_id=replace(identity.publication_id, lease_id=LeaseID.random()))
        else:
            identity = replace(identity, manifest_digest="0" * 64)
        return wire.PreparedOutputPublicationReply(identity, True)

    worker.on_prepare = wrong_ack
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        worker.worker._handle_push_task(worker.push)
    assert worker.handlers == [START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER]
    assert worker.pending.nested_imports is imports and not child.closed
    assert not worker.pending.prepare_acked and worker.key not in worker.worker._replies
    worker.on_prepare = None
    assert worker.worker._handle_push_task(worker.push).status is protocol.TaskReplyStatus.SUCCEEDED
    assert worker.executions == reductions == [True] and child.closes == 1


@pytest.mark.parametrize("changed", ("source", "tier"))
def test_batch_digest_binds_child_source_and_tier_before_any_replay_effect(changed):
    node = _NodeFixture()
    node.prepare()
    last = node.manifest.slots[0]
    if changed == "source":
        transfer = last.transfers[1]
        last = replace(last, transfers=(last.transfers[0], replace(
            transfer, source=replace(transfer.source, borrower_token="rebound-source"),
        )))
    else:
        last = replace(last, tier=protocol.ResultStorage.INLINE)
    changed_manifest = OutputPublicationManifest.create(node.manifest.header, (last,))
    assert changed_manifest.publication_id == node.id
    assert changed_manifest.manifest_digest != node.manifest.manifest_digest
    before = node.journal.snapshot(node.id), tuple(node.events), node.store.used_bytes
    with pytest.raises(OutputPublicationConflictError):
        node.adapter.prepare(changed_manifest, node.values.payloads)
    assert (node.journal.snapshot(node.id), tuple(node.events), node.store.used_bytes) == before
    assert node.complete() == node.values.envelope


def test_later_serialization_failure_creates_no_node_publication_or_pin(monkeypatch):
    values, sessions, reductions = [], [], []
    worker = _WorkerFixture(monkeypatch, lambda: tuple(values), threshold=0)
    child = ObjectRef(ObjectID.for_task(TaskID.random()), worker.worker.worker_id, worker.worker.address)

    class Bad:
        def __reduce__(self):
            reductions.append("bad")
            raise ValueError("later output cannot serialize")

    values.extend((child, Bad()))
    make_discovery = worker.worker._output_discovery_session

    def discovery(*args):
        session = make_discovery(*args)
        sessions.append(session)
        return session

    monkeypatch.setattr(worker.worker, "_output_discovery_session", discovery)
    reply = worker.worker._handle_push_task(worker.push)
    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR and reply.results == ()
    assert reply.output_publication is None
    assert worker.handlers == [START_WORKER_LEASE_HANDLER, COMPLETE_WORKER_LEASE_HANDLER]
    assert worker.prepares == [] and len(sessions) == 1
    assert sessions[0].discovered is None and sessions[0].source_references == ()
    assert worker.executions == [True] and reductions == ["bad"]
    assert not child.closed
    assert worker.worker._handle_push_task(worker.push) is reply
    assert len(sessions) == 1 and reductions == ["bad"]


@pytest.mark.parametrize("fail_cache_once", (False, True))
def test_complete_ack_then_reply_cache_discharge_the_push_obligation(monkeypatch, fail_cache_once):
    fixture, child, _imports, reductions = _borrowed_fixture(monkeypatch, threshold=0)
    worker = fixture.worker
    worker._lifecycle = threading.Condition(threading.RLock())
    worker._accepted_pushes = {}
    worker._push_obligations = set()
    worker._accepting_tasks = True
    worker._active_tasks = 0
    cache_attempts = []
    real_cache = worker._cache_complete_and_return

    def complete(request):
        assert fixture.key not in worker._replies
        assert fixture.key not in worker._completion_acked
        assert worker._push_obligations == {fixture.key}
        assert fixture.pending.prepare_acked and fixture.pending.nested_imports is None
        assert fixture.pending.discovery.source_references == () and child.closed
        return fixture.complete(request)

    def cache(request, reply, key):
        assert key == fixture.key and key in worker._completion_acked
        assert key not in worker._replies and worker._push_obligations == {key}
        assert reply.output_publication == fixture.pending.complete_envelope
        cache_attempts.append(reply)
        if fail_cache_once and len(cache_attempts) == 1:
            raise RuntimeError("post-Complete cache failed")
        return real_cache(request, reply, key)

    fixture.on_complete = complete
    monkeypatch.setattr(worker, "_cache_complete_and_return", cache)
    if fail_cache_once:
        with pytest.raises(RuntimeError, match="exact PushTask replay"):
            worker._handle_push_task(fixture.push)
        assert worker._push_obligations == {fixture.key}
        assert fixture.key in worker._completion_acked and fixture.key not in worker._replies
        worker._accepting_tasks = False
    reply = worker._handle_push_task(fixture.push)
    assert fixture.handlers == [START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER, COMPLETE_WORKER_LEASE_HANDLER]
    assert worker._replies[fixture.key] is reply and worker._cached_pushes[fixture.key] == fixture.push
    assert fixture.key not in worker._prepared_output_replies
    assert worker._push_obligations == set() and worker._active_tasks == 0
    assert worker._handle_push_task(fixture.push) is reply
    assert fixture.executions == reductions == [True] and child.closes == 1
    assert len(cache_attempts) == (2 if fail_cache_once else 1)
