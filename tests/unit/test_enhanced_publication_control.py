"""Finite pure GCS composition checks with real membership/child reducers.

No sockets, subprocesses, threads, sleeps or pytest discovery are started.
Constructed Complete/preparation are metadata input, not runtime evidence.
Child releases below do execute the real owner table tombstone transitions.
"""
from dataclasses import replace
from threading import RLock
import hashlib

import pytest

from miniray import control, enhanced_publication as ep, protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputSlotManifest,
    OutputPublicationCompleteWitness)
from miniray.ownership import ObjectOwnerTable
from miniray.publication_sources import BorrowedContainedSource, PreparedContainedTransfer
from miniray.put_handoff import PutManifest
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest

pytestmark = pytest.mark.unit


def _id(kind, n):
    return kind(bytes([n]) * 16)


class _Server:
    def __init__(self, handlers, **kwargs):
        self.handlers = handlers
        self.address = ('127.0.0.1', 39000)


class _Fixture:
    def __init__(self, monkeypatch):
        monkeypatch.setattr(control, 'TCPServer', _Server)
        self.calls = []
        self.lose_release = False
        self.table = ObjectOwnerTable()
        self.gcs = control.GCSLite(owner_fence_rpc=self.rpc)
        self.nodes = []
        for n in (1, 2):
            self.nodes.append(self.gcs.register_node(protocol.RegisterNode(
                _id(NodeID, n), 1000 + n, ('node.invalid', 12000 + n), ResourceVector({'CPU': 1}))))
        self.publisher = self.worker(3, self.nodes[0])
        self.owner = self.worker(4, self.nodes[1])
        self.child_owner = self.worker(5, self.nodes[1])
        self.job = _id(JobID, 6)
        self.child = ObjectID.for_task(_id(TaskID, 7))
        self.table.register(self.child, current_attempt=AttemptID(self.child.task_id, 0))

    def worker(self, n, node):
        incarnation = protocol.WorkerIncarnation(node.node_id, node.node_pid,
            node.registration_epoch, _id(WorkerID, n), 2000 + n)
        assert self.gcs.register_worker_incarnation(protocol.RegisterWorkerIncarnation(incarnation)).accepted
        return incarnation

    def task(self, *, children=True, owner=None):
        owner = self.owner.worker_id if owner is None else owner
        task = _id(TaskID, 8)
        object_id = ObjectID.for_task(task)
        identity = OutputPublicationID(_id(LeaseID, 9), TaskExecutionKey(
            TaskOutputManifest.for_task(task, 1), AttemptID(task, 0)))
        node = self.nodes[0]
        header = OutputPublicationHeader(identity, self.job, self.publisher.worker_id, owner,
            OutputPublicationNodeIncarnation(node.node_id, node.node_pid, node.registration_epoch))
        transfers = (self.transfer(object_id, owner, publisher=self.publisher.worker_id),) if children else ()
        slot = OutputSlotManifest(object_id, protocol.ResultStorage.INLINE, 1, hashlib.sha256(b'x').hexdigest(), transfers)
        return ep.TaskPublication(OutputPublicationManifest.create(header, (slot,)), ('owner.invalid', 1234))

    def put(self):
        outer = ObjectID.for_task(_id(TaskID, 10))
        return ep.PutPublication(self.job, ('owner.invalid', 1234), PutManifest(
            outer, self.owner.worker_id, protocol.ResultStorage.INLINE, 1,
            hashlib.sha256(b'x').hexdigest(), (self.transfer(outer, self.owner.worker_id),)))

    def transfer(self, outer, owner, publisher=None):
        publisher = owner if publisher is None else publisher
        token = 'final:' + outer.hex
        return PreparedContainedTransfer(self.child, self.child_owner.worker_id, ('child.invalid', 1235),
            BorrowedContainedSource(publisher, 'borrowed-child', protocol.ContainedTransferSource(
                ContainedReferenceHold(self.child, self.child_owner.worker_id, 'source'))),
            ContainedReferenceHold(outer, publisher, 'provisional:' + token if publisher == owner else token),
            ContainedReferenceHold(outer, owner, token))

    def rpc(self, address, handler, request):
        self.calls.append((handler, request))
        if handler == 'install_owner_death_fence':
            return protocol.InstallOwnerDeathFenceReply(request, protocol.OwnerDeathFenceDisposition.FENCED)
        assert handler == 'release_contained_reference'
        released = self.table.release_contained_reference(request.object_id, request.hold)
        assert self.table.contained_release_was_seen(request.object_id, request.hold)
        if self.lose_release:
            self.lose_release = False
            raise OSError('actual release applied but ACK lost')
        return protocol.ReleaseContainedReferenceReply(request.object_id, request.owner_worker_id, request.hold, True, released)

    def call(self, request):
        reply = self.gcs.enhanced_publication(request)
        assert reply.accepted, reply.error
        return reply

    def begin(self, publication):
        self.call(ep.BeginPublication(publication))
        self.call(ep.PrepareGraph(publication.reference))

    def worker_death(self, incarnation):
        reply = self.gcs.report_worker_death(protocol.ReportWorkerDeath(
            'exit:' + incarnation.worker_id.hex, incarnation, 7, protocol.WorkerDeathReason.PROCESS_EXIT))
        assert reply.death is not None
        return reply.death

    def node_death(self):
        node = self.nodes[0]
        return self.gcs.report_node_death(protocol.ReportNodeDeath(
            'node-exit', node.node_id, node.node_pid, node.registration_epoch, 7,
            protocol.NodeDeathReason.PROCESS_EXIT, 'test'))

    def drain(self):
        for _ in range(20):
            reply = self.gcs.drain_owner_death_fences(protocol.DrainOwnerDeathFences('drain'))
            if reply.clean:
                return reply
        raise AssertionError('finite exact death cleanup did not converge')


def test_dead_publisher_node_does_not_release_surviving_child_without_actual_ack(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.task()
    f.begin(publication)
    for hold in (publication.transfers[0].final_hold, publication.transfers[0].provisional_hold):
        f.table.add_contained_reference(f.child, hold)
    f.node_death()
    assert f.call(ep.GetPublication(publication.reference)).snapshot.graph_active
    f.worker_death(f.owner)
    f.lose_release = True
    f.gcs.publication_control.drive_one()
    snapshot = f.call(ep.GetPublication(publication.reference)).snapshot
    assert snapshot.graph_active
    f.drain()
    snapshot = f.call(ep.GetPublication(publication.reference)).snapshot
    assert not snapshot.graph_active and len(snapshot.closed_holds.releases) == 2
    assert not f.table.snapshot(f.child).contained_holds
    assert any(not reply.released for reply in snapshot.closed_holds.releases)


def test_put_owner_death_releases_real_remote_holds_without_task_complete(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.put()
    f.begin(publication)
    for hold in (publication.transfers[0].final_hold, publication.transfers[0].provisional_hold):
        f.table.add_contained_reference(f.child, hold)
    f.worker_death(f.owner)
    first = f.gcs.drain_owner_death_fences(protocol.DrainOwnerDeathFences('drain'))
    assert not first.clean and first.active_publication_cleanups == 1
    f.drain()
    snapshot = f.call(ep.GetPublication(publication.reference)).snapshot
    assert snapshot.complete is None and not snapshot.graph_active
    assert len(snapshot.closed_holds.releases) == 2
    assert not f.table.snapshot(f.child).contained_holds
    assert all(handler != 'finalize_output_owner_death' for handler, _ in f.calls)


def test_unmanaged_owner_route_is_bound_but_dead_executor_blocks_new_admission(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.task(children=False, owner=_id(WorkerID, 99))
    f.begin(publication)
    changed = replace(publication, owner_address=('changed.invalid', 1234))
    assert not f.gcs.enhanced_publication(ep.BeginPublication(changed)).accepted
    f.worker_death(f.publisher)
    assert f.call(ep.GetPublication(publication.reference)).snapshot.forward_open
    prepared = ep.TaskPreparedReceipt(publication.reference, (), (),
        ep.MaterializationReceipt(publication.reference, publication.manifest.header.node_incarnation))
    assert not f.gcs.enhanced_publication(ep.ArmTask(prepared)).accepted


def test_accurate_terminal_and_commit_survive_executor_death(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.task(children=False)
    f.begin(publication)
    f.call(ep.ArmTask(ep.TaskPreparedReceipt(publication.reference, (), (),
        ep.MaterializationReceipt(publication.reference, publication.manifest.header.node_incarnation))))
    complete = OutputPublicationCompleteWitness.for_manifest(publication.manifest)
    f.worker_death(f.publisher)
    first = f.call(ep.RecordTerminal(complete))
    f.node_death()
    assert f.call(ep.RecordTerminal(complete)).receipt == first.receipt
    assert f.call(ep.CommitGraph(publication.reference)).snapshot.complete == complete


def test_prior_abort_fence_is_not_rebound_by_later_owner_death(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.put()
    f.begin(publication)
    proof = ep.OwnerAbortReceipt(publication.reference, publication.owner_worker_id, 'abort')
    first = f.call(ep.FencePublication(publication, proof))
    death = f.worker_death(f.owner)
    reply = f.call(ep.FencePublication(publication, death))
    assert reply.receipt == first.receipt and reply.snapshot.fence == proof
    f.drain()
    assert f.call(ep.GetPublication(publication.reference)).snapshot.fence == proof


def test_live_publication_does_not_block_pre_core_drain_but_blocks_final_shutdown(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.task(children=False)
    f.begin(publication)
    assert f.gcs.drain_owner_death_fences(protocol.DrainOwnerDeathFences('drain')).clean
    assert not f.gcs.shutdown(protocol.Shutdown('shutdown', 'test')).clean
    # Exact accepted history remains available while unfinished work blocks
    # final exit; a different publication cannot add work behind shutdown.
    assert f.call(ep.BeginPublication(publication)).snapshot.graph_active
    assert not f.gcs.enhanced_publication(ep.BeginPublication(f.put())).accepted
    f.call(ep.FencePublication(publication, ep.OwnerAbortReceipt(publication.reference, publication.owner_worker_id, 'abort')))
    f.call(ep.RetireGraph(ep.ClosedContainedHolds(publication.reference)))
    assert f.gcs.shutdown(protocol.Shutdown('shutdown', 'test')).clean


def test_clean_shutdown_permanently_rejects_late_new_begin(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.put()
    assert f.gcs.shutdown(protocol.Shutdown('shutdown', 'test')).clean
    reply = f.gcs.enhanced_publication(ep.BeginPublication(publication))
    assert not reply.accepted and reply.error_kind is ep.PublicationErrorKind.INVALID_STATE
    assert f.call(ep.GetPublication(publication.reference)).snapshot is None
    assert not f.gcs.publication_control.has_active_operations()


def test_forged_child_death_is_rejected_before_retirement(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.put()
    f.begin(publication)
    f.call(ep.FencePublication(publication, ep.OwnerAbortReceipt(publication.reference, publication.owner_worker_id, 'abort')))
    forged = protocol.WorkerDeathRecord('not-committed', f.child_owner, 1, 7, protocol.WorkerDeathReason.PROCESS_EXIT)
    reply = f.gcs.enhanced_publication(ep.RetireGraph(ep.ClosedContainedHolds(publication.reference, child_deaths=(forged,))))
    assert not reply.accepted
    assert f.call(ep.GetPublication(publication.reference)).snapshot.graph_active


def test_dead_owner_progress_rotates_even_when_sweep_is_blocked(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.put()
    f.begin(publication)
    f.worker_death(f.owner)
    choices = []
    monkeypatch.setattr(f.gcs, '_drive_owner_death_fence', lambda effect: choices.append('sweep') or False)
    monkeypatch.setattr(f.gcs.publication_control, 'drive_one', lambda: choices.append('publication') or False)
    for _ in range(4):
        f.gcs._drive_owner_death_fence_once()
    assert choices == ['sweep', 'publication', 'sweep', 'publication']
