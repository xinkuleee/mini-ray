"""Narrow Complete ACK boundaries over actual Node/owner handoff methods.

Two threadless Cores, one accepted Task, one8KiBStore, zero/one child, at most
two exact report attempts; no runtime constructor, thread/socket/process/wait.
Complete reporting remains distinct from owner visibility and physical GC.
"""
from dataclasses import replace
import multiprocessing.process
import pickle
import queue
import socket
import subprocess
import threading
import time

import pytest

from miniray import core as core_module, node, worker, control, transport, protocol
from miniray import output_protocol as wire
from miniray.core import CoreWorker, _PendingTask, _WAKE_COORDINATOR
from miniray.ids import LeaseID
from miniray.output_handoff import OutputHandoffPhase, OutputHandoffTable
from miniray.ownership import ObjectCollectionState, ObjectState
from tests.support._contained_output import ContainedOutput
from tests.unit._pure_core import close_pure_core

pytestmark = pytest.mark.unit

@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail('Complete ACK contract attempted infrastructure')
    for kind in (CoreWorker, node.NodeServer, worker.WorkerServer, control.GCSLite, transport.TCPServer):
        monkeypatch.setattr(kind, '__init__', forbidden)
    for kind, name in ((threading.Thread, 'start'), (threading.Timer, 'start'),
                       (threading.Condition, 'wait'), (queue.Queue, 'join'),
                       (multiprocessing.process.BaseProcess, 'start')):
        monkeypatch.setattr(kind, name, forbidden)
    for name in ('socket', 'socketpair', 'create_connection'):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, 'Popen', forbidden)
    monkeypatch.setattr(time, 'sleep', forbidden)
    for module in (core_module, node, worker, control):
        monkeypatch.setattr(module, 'rpc_request', forbidden)
    def already_set(event, timeout=None):
        assert event.is_set(), 'reference receipt must already be set'
        return True
    monkeypatch.setattr(threading.Event, 'wait', already_set)

def _consume(core):
    tasks = []
    assert core._submissions.qsize() <= 16
    for _ in range(core._submissions.qsize()):
        event = core._submissions.get_nowait()
        core._submissions.task_done()
        if event is not _WAKE_COORDINATOR:
            assert type(event) is _PendingTask
            tasks.append(event)
    return tasks

@pytest.fixture
def publication():
    values = []
    def make(contained):
        assert not values
        f = ContainedOutput(contained=contained, stored=True)
        values.append(f)
        f.register()
        assert _consume(f.core) == [f.pending] and _consume(f.child_owner) == []
        f.reply = f.complete()
        return f
    yield make
    for f in values:
        for ref in (f.ref, f.child_ref):
            if ref is not None and not ref.closed:
                ref.close(timeout=0)
        close_pure_core(f.core)
        close_pure_core(f.child_owner)

@pytest.mark.parametrize('contained', (False, True), ids=('ordinary', 'contained'))
def test_lost_complete_ack_replays_exact_witness_without_query_or_early_ready(publication, contained):
    f = publication(contained)
    identity, witness = f.envelope.publication_id, f.envelope.complete
    original = f.node._background_rpc
    reports = []
    def lose_first(address, handler, request):
        assert handler == wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER
        reply = original(address, handler, request)
        assert type(reply) is wire.OutputHandoffCompleteAck and reply.accepted
        assert reply.witness == request.witness == witness
        reports.append((request, reply))
        assert len(reports) <= 2
        if len(reports) == 1:
            raise TimeoutError('owner accepted Complete; response lost')
        return pickle.loads(pickle.dumps(reply))
    f.node._background_rpc = lose_first
    with pytest.raises(TimeoutError):
        f.adapter.report_terminal(identity)
    assert f.adapter.pending_terminal_reports() == (witness,)
    assert f.core.owner_table.snapshot(f.ref.object_id).state is ObjectState.PENDING
    assert f.core._output_handoff_table().query(identity).complete == witness
    assert f.node.resource_ledger.available == f.node.resource_ledger.total
    assert f.adapter.report_terminal(identity)
    assert reports[0] == reports[1]
    assert not f.adapter.report_terminal(identity) and len(reports) == 2
    assert f.core._publish_reply(f.pending, f.reply, expected_node_id=f.node.node_id, expected_lease_id=f.grant.lease_id)
    assert f.core._finish_pending_task(f.pending) and _consume(f.core) == []
    assert f.node.object_store.contains(f.ref.object_id)
    f.ref.close(timeout=0)
    if f.child_ref is not None:
        f.child_ref.close(timeout=0)
    for _ in range(4):
        f.core._reference_mailbox.drain()
        f.child_owner._reference_mailbox.drain()
    assert f.node.object_store.used_bytes == 0
    assert f.core.owner_table.collection_state(f.ref.object_id) is ObjectCollectionState.COLLECTED

@pytest.mark.parametrize('bad', ('subclass', 'wrong-witness', 'nested-tamper', 'rejected'))
def test_complete_ack_rejected_by_real_node_keeps_pending_report(publication, bad):
    f = publication(True)
    identity, witness = f.envelope.publication_id, f.envelope.complete
    if bad == 'subclass':
        class Hidden(wire.OutputHandoffCompleteAck):
            pass
        invalid = Hidden(witness, True)
        object.__setattr__(invalid, 'payload', b'hidden')
    elif bad == 'wrong-witness':
        invalid = wire.OutputHandoffCompleteAck(replace(witness, publication_id=replace(identity, lease_id=LeaseID.random())), True)
    elif bad == 'rejected':
        invalid = wire.OutputHandoffCompleteAck(witness, False, 'owner unavailable')
    else:
        invalid = wire.OutputHandoffCompleteAck(witness, True)
        object.__setattr__(invalid.witness.publication_id.execution.attempt_id, 'attempt_number', -1)
    original = f.node._background_rpc
    f.node._background_rpc = lambda *_args: invalid
    with pytest.raises((TypeError, ValueError, protocol.ProtocolError)):
        f.adapter.report_terminal(identity)
    assert f.adapter.pending_terminal_reports() == (witness,)
    assert f.core.owner_table.snapshot(f.ref.object_id).state is ObjectState.PENDING
    f.node._background_rpc = original
    assert f.adapter.report_terminal(identity)

def test_complete_ack_is_not_forward_permission_and_query_stays_detached(publication):
    f = publication(True)
    witness, identity = f.envelope.complete, f.envelope.publication_id
    report = wire.ReportOutputHandoffComplete(witness)
    ack = f.core.report_output_handoff_complete(report)
    assert type(ack) is wire.OutputHandoffCompleteAck and ack.accepted
    assert not hasattr(ack, 'snapshot')
    with pytest.raises(protocol.ProtocolError):
        wire.OutputHandoffReply(report, True, f.core._output_handoff_table().query(identity))
    query = f.core.get_output_handoff(wire.GetOutputHandoff(identity))
    assert query.snapshot.complete == ack.witness
    object.__setattr__(query.snapshot.manifest.value, 'size_bytes', -1)
    assert f.core.get_output_handoff(wire.GetOutputHandoff(identity)).snapshot.manifest == f.outputs.manifest
    table = OutputHandoffTable()
    table.register(f.outputs.manifest, identity.attempt_id)
    table.record_complete(witness)
    assert table.abort(identity, 'no future publication')
    replay = wire.OutputHandoffCompleteAck(table.record_complete(witness).complete, True)
    assert replay == ack and table.query(identity).phase is OutputHandoffPhase.ABORTED
    with pytest.raises(ValueError):
        table.register(f.outputs.manifest, identity.attempt_id)

def test_worker_without_owner_returns_narrow_complete_rejection(publication):
    f = publication(False)
    endpoint = object.__new__(worker.WorkerServer)
    endpoint._embedded_core_lock = threading.Lock()
    endpoint._embedded_core = None
    request = wire.ReportOutputHandoffComplete(f.envelope.complete)
    reply = endpoint._handle_output_handoff('report_output_handoff_complete', request)
    assert type(reply) is wire.OutputHandoffCompleteAck and not reply.accepted
    assert reply.witness == request.witness
    endpoint._embedded_core = f.core
    accepted = endpoint._handle_output_handoff('report_output_handoff_complete', request)
    assert type(accepted) is wire.OutputHandoffCompleteAck and accepted.accepted
    assert accepted.witness == request.witness
