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
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputValue,
    OutputPublicationCompleteWitness)
from miniray.ownership import ObjectOwnerTable
from miniray.node import NodeServer
from miniray.publication_sources import BorrowedContainedSource, PreparedContainedTransfer
from miniray.put_handoff import PutManifest
from miniray.resources import ResourceVector
from miniray.task_outputs import TaskExecution

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
        self.fence_nodes = {}
        for n in (1, 2):
            self.nodes.append(self.gcs.register_node(protocol.RegisterNode(
                _id(NodeID, n), 1000 + n, ('node.invalid', 12000 + n), ResourceVector({'CPU': 1}))))
        for info in self.nodes:
            # Empty physical scope, but an actual Node fence reducer owns the
            # permanent owner fence and exact replay receipt. No fake fence ACK.
            node = object.__new__(NodeServer)
            node.node_id = info.node_id
            node._state_lock = RLock()
            node._sealed_metadata = {}
            node._owner_death_fences = {}
            node._owner_death_fence_outcomes = {}
            node._object_localization_locks = {}
            self.fence_nodes[node.node_id] = node
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
        identity = OutputPublicationID(_id(LeaseID, 9), TaskExecution(AttemptID(task, 0)))
        node = self.nodes[0]
        header = OutputPublicationHeader(identity, self.job, self.publisher.worker_id, owner,
            OutputPublicationNodeIncarnation(node.node_id, node.node_pid, node.registration_epoch))
        transfers = (self.transfer(object_id, owner, publisher=self.publisher.worker_id),) if children else ()
        value = OutputValue(protocol.ResultStorage.INLINE, 1, hashlib.sha256(b'x').hexdigest(), transfers)
        return ep.TaskPublication(OutputPublicationManifest.create(header, value), ('owner.invalid', 1234))

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
            node = self.fence_nodes[request.node_id]
            assert address == self.gcs.nodes.address(request.node_id)
            assert request.scope is protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP
            assert request.expected_replicas == () and node._sealed_metadata == {}
            reply = node._handle_install_owner_death_fence(request)
            assert reply.request == request and reply.complete
            assert node._owner_death_fences[request.owner_worker_id] == request.owner_death
            return reply
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
    assert f.call(ep.CommitGraph(publication.reference)).accepted_fact == complete


def test_prior_abort_fence_is_not_rebound_by_later_owner_death(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.put()
    f.begin(publication)
    proof = ep.OwnerAbortReceipt(publication.reference, publication.owner_worker_id, 'abort')
    first = f.call(ep.FencePublication(publication, proof))
    death = f.worker_death(f.owner)
    reply = f.call(ep.FencePublication(publication, death))
    assert reply.receipt == first.receipt and reply.accepted_fact == proof and reply.fence == proof
    assert not reply.forward_open and reply.request.proof == death
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
    assert f.call(ep.BeginPublication(publication)).receipt.stage is ep.PublicationStage.INTENT
    assert f.call(ep.GetPublication(publication.reference)).snapshot.graph_active
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


def test_closed_shutdown_accepts_exact_node_death_and_late_terminal_until_cleanup(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.task(owner=f.publisher.worker_id)
    f.begin(publication)
    transfer, = publication.transfers
    source = transfer.source
    f.table.add_contained_reference(f.child, source.original_source.hold)
    f.table.acquire_exported_reference(f.child, source.original_source, source.owner_table_token)
    prepare = protocol.PrepareStoredContainedPin(transfer, transfer.contained_owner_worker_id)
    prepared = protocol.StoredContainedPinReply(prepare, f.table.prepare_stored_contained_reference(
        transfer, authority_worker_id=prepare.authority_worker_id))
    promote = protocol.PromoteStoredContainedPin(transfer, transfer.contained_owner_worker_id)
    promoted = protocol.StoredContainedPinReply(promote, f.table.promote_stored_contained_reference(
        transfer, authority_worker_id=promote.authority_worker_id))
    f.table.release_borrowed_reference(f.child, source.owner_table_token)
    f.table.release_contained_reference(f.child, source.original_source.hold)
    arm = ep.ArmTask(ep.TaskPreparedReceipt(publication.reference, (prepared,), (promoted,),
        ep.MaterializationReceipt(publication.reference, publication.manifest.header.node_incarnation)))
    f.call(arm)
    complete = OutputPublicationCompleteWitness.for_manifest(publication.manifest)
    shutdown = protocol.Shutdown('shutdown', 'test')
    assert not f.gcs.shutdown(shutdown).clean
    assert f.call(ep.BeginPublication(publication)).receipt.stage is ep.PublicationStage.INTENT
    assert f.call(ep.GetPublication(publication.reference)).snapshot.graph_active
    assert f.call(arm).receipt.stage is ep.PublicationStage.ARMED
    assert not f.gcs.enhanced_publication(ep.BeginPublication(f.put())).accepted

    first = f.node_death()
    assert first.disposition is protocol.NodeDeathDisposition.APPLIED
    owner_death = f.gcs.workers.get(f.publisher.worker_id).death
    fenced = f.call(ep.GetPublication(publication.reference)).snapshot
    assert fenced.fence == owner_death and not fenced.forward_open and fenced.graph_active
    assert fenced.complete is None and fenced.closed_holds is None
    replay = f.node_death()
    assert replay.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD
    assert replay.death == first.death
    assert f.call(ep.GetPublication(publication.reference)).snapshot == fenced
    terminal = f.call(ep.RecordTerminal(complete))
    assert terminal.accepted_fact == complete and not terminal.forward_open
    assert not f.gcs.enhanced_publication(ep.CommitGraph(publication.reference)).accepted
    assert not f.gcs.shutdown(shutdown).clean

    f.drain()
    retired = f.call(ep.GetPublication(publication.reference)).snapshot
    assert retired.complete == complete and retired.fence == owner_death
    assert not retired.graph_active and not retired.forward_open
    assert len(retired.closed_holds.releases) == 2
    assert not retired.closed_holds.child_deaths and retired.closed_holds.rollback_scope is None
    assert {reply.hold for reply in retired.closed_holds.releases} == {transfer.final_hold, transfer.provisional_hold}
    assert all(f.table.contained_release_was_seen(f.child, reply.hold) for reply in retired.closed_holds.releases)
    assert not f.table.snapshot(f.child).contained_holds
    calls = tuple(f.calls)
    assert f.call(ep.RecordTerminal(complete)).receipt == terminal.receipt
    assert f.call(ep.RetireGraph(retired.closed_holds)).accepted_fact == retired.closed_holds
    assert f.call(ep.GetPublication(publication.reference)).snapshot == retired
    assert f.node_death().death == first.death
    assert f.gcs.drain_owner_death_fences(protocol.DrainOwnerDeathFences('drain')).clean
    assert tuple(f.calls) == calls and f.gcs.shutdown(shutdown).clean


def test_membership_commit_failure_replays_both_single_output_publications(monkeypatch):
    f = _Fixture(monkeypatch)
    first = f.task(owner=f.publisher.worker_id)
    task = _id(TaskID, 11)
    identity = OutputPublicationID(_id(LeaseID, 12), TaskExecution(AttemptID(task, 0)))
    header = replace(first.manifest.header, publication_id=identity)
    transfer = f.transfer(ObjectID.for_task(task), f.publisher.worker_id, publisher=f.publisher.worker_id)
    second = ep.TaskPublication(OutputPublicationManifest.create(
        header, replace(first.manifest.value, transfers=(transfer,))), first.owner_address)
    publications = (first, second)
    for publication in publications:
        f.begin(publication)
        for hold in (publication.transfers[0].final_hold, publication.transfers[0].provisional_hold):
            f.table.add_contained_reference(f.child, hold)
    before = tuple(f.call(ep.GetPublication(item.reference)).snapshot for item in publications)
    commit = f.gcs.publication_control.commit_owner_death
    attempts = []

    def fail_once(death):
        assert f.gcs._owner_fence_lock()._is_owned()
        assert f.gcs.nodes.get(f.nodes[0].node_id).state is protocol.NodeMembershipState.DEAD
        assert f.gcs.workers.get(f.publisher.worker_id).death == death
        attempts.append(death)
        if len(attempts) == 1:
            raise RuntimeError('membership committed before publication admission')
        return commit(death)

    monkeypatch.setattr(f.gcs.publication_control, 'commit_owner_death', fail_once)
    with pytest.raises(RuntimeError, match='membership committed before publication admission'):
        f.node_death()
    death = f.gcs.nodes.get(f.nodes[0].node_id).death
    assert death is not None and not f.gcs.publication_control.pending_deaths()
    assert tuple(f.call(ep.GetPublication(item.reference)).snapshot for item in publications) == before
    assert not f.calls and len(f.table.snapshot(f.child).contained_holds) == 4
    replay = f.node_death()
    assert replay.disposition is protocol.NodeDeathDisposition.ALREADY_DEAD and replay.death == death
    assert attempts == [attempts[0], attempts[0]]
    assert set(f.gcs.publication_control.pending_deaths()) == {item.reference for item in publications}
    fenced = tuple(f.call(ep.GetPublication(item.reference)).snapshot for item in publications)
    assert all(item.fence == attempts[0] and not item.forward_open and item.graph_active for item in fenced)
    assert f.node_death().death == death
    assert tuple(f.call(ep.GetPublication(item.reference)).snapshot for item in publications) == fenced
    f.drain()
    retired = tuple(f.call(ep.GetPublication(item.reference)).snapshot for item in publications)
    assert all(not item.graph_active and len(item.closed_holds.releases) == 2 for item in retired)
    releases = tuple(request for handler, request in f.calls if handler == 'release_contained_reference')
    assert len(releases) == len(set(releases)) == 4
    assert not f.table.snapshot(f.child).contained_holds
    calls = tuple(f.calls)
    assert f.node_death().death == death
    assert f.gcs.drain_owner_death_fences(protocol.DrainOwnerDeathFences('drain')).clean
    assert tuple(f.calls) == calls
    assert tuple(f.call(ep.GetPublication(item.reference)).snapshot for item in publications) == retired


def test_reentrant_cleanup_suppresses_inflight_release_and_lost_ack_releases_ticket(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.put()
    f.begin(publication)
    transfer, = publication.transfers
    for hold in (transfer.final_hold, transfer.provisional_hold):
        f.table.add_contained_reference(f.child, hold)
    f.worker_death(f.owner)
    reentered = []

    def release(address, handler, request):
        reply = f.rpc(address, handler, request)
        calls = tuple(f.calls)
        before = f.call(ep.GetPublication(publication.reference)).snapshot
        reentered.append((request, reply))
        assert not f.gcs.publication_control.drive_one()
        assert tuple(f.calls) == calls
        assert f.call(ep.GetPublication(publication.reference)).snapshot == before
        assert before.graph_active and before.closed_holds is None
        if len(reentered) == 1:
            raise TimeoutError('real child release ACK lost after reentry')
        return reply

    monkeypatch.setattr(f.gcs.publication_control, 'rpc', release)
    assert not f.gcs.publication_control.drive_one()
    assert f.gcs.publication_control.pending_deaths() == (publication.reference,)
    assert reentered[0][1].released
    assert not f.gcs.publication_control._deaths[publication.reference].inflight
    assert f.gcs.publication_control.drive_one()
    assert reentered[1][0] == reentered[0][0] and not reentered[1][1].released
    assert f.gcs.publication_control.drive_one()
    assert reentered[2][0].hold == transfer.provisional_hold and reentered[2][1].released
    assert len(reentered) == 3 and not f.table.snapshot(f.child).contained_holds
    f.drain()
    retired = f.call(ep.GetPublication(publication.reference)).snapshot
    assert not retired.graph_active and len(retired.closed_holds.releases) == 2
    assert not f.gcs.publication_control.pending_deaths()
    calls = tuple(f.calls)
    assert not f.gcs.publication_control.drive_one()
    assert f.call(ep.RetireGraph(retired.closed_holds)).accepted_fact == retired.closed_holds
    assert f.call(ep.GetPublication(publication.reference)).snapshot == retired
    assert tuple(f.calls) == calls and len(reentered) == 3


def test_first_begin_rejects_wrong_registered_publisher_incarnation_without_history(monkeypatch):
    f = _Fixture(monkeypatch)
    publication = f.task(children=False)
    node = publication.manifest.header.node_incarnation
    for field in ('node_pid', 'registration_epoch'):
        header = replace(publication.manifest.header, node_incarnation=replace(node, **{field: getattr(node, field) + 1}))
        changed = ep.TaskPublication(OutputPublicationManifest.create(header, publication.manifest.value), publication.owner_address)
        reply = f.gcs.enhanced_publication(ep.BeginPublication(changed))
        assert not reply.accepted and reply.error_kind is ep.PublicationErrorKind.UNAVAILABLE
        assert f.call(ep.GetPublication(changed.reference)).snapshot is None
        assert f.call(ep.GetPublication(publication.reference)).snapshot is None
        assert not f.gcs.publication_control.authority.snapshots()
        assert not f.gcs.publication_control.has_active_operations() and not f.calls
    accepted = f.call(ep.BeginPublication(publication))
    assert accepted.request.publication == publication
    assert accepted.owner_worker_id == publication.owner_worker_id
    assert f.call(ep.GetPublication(publication.reference)).snapshot.publication == publication
    assert accepted.receipt.stage is ep.PublicationStage.INTENT
    assert f.call(ep.BeginPublication(publication)).receipt == accepted.receipt


@pytest.mark.parametrize('corruption', ('wrong-request', 'invalid-closed-hold'))
def test_controller_rejects_malformed_actual_node_finalize_and_retries_exact_cleanup(monkeypatch, corruption):
    from threading import Lock
    from types import SimpleNamespace
    from miniray import output_protocol as wire
    from miniray.node import _LeaseRecord, _WorkerSlot
    from miniray.object_manager import ObjectManager
    from miniray.object_store import ObjectStore
    from miniray.output_handoff import OutputHandoffTable
    from miniray.output_publication_journal import OutputPublicationJournal
    from miniray.output_publication_node import OutputPublicationNodeAdapter
    from miniray.resources import AllocationToken, NodeSnapshot, ResourceLedger
    from miniray.worker import WorkerServer

    f = _Fixture(monkeypatch)
    publication = f.task()
    manifest, identity = publication.manifest, publication.manifest.publication_id
    transfer, = publication.transfers
    source = transfer.source
    f.table.add_contained_reference(f.child, source.original_source.hold)
    f.table.acquire_exported_reference(f.child, source.original_source, source.owner_table_token)
    journal, handoffs = OutputPublicationJournal(), OutputHandoffTable()
    node = f.fence_nodes[f.nodes[0].node_id]
    node._node_pid, node._registration_epoch = f.nodes[0].node_pid, f.nodes[0].registration_epoch
    node._object_store = ObjectStore(1024)
    node._object_manager = ObjectManager(node.node_id, node._object_store)
    node._dropped_metadata, node._local_replica_write_claims = {}, {}
    node._ledger = ResourceLedger(ResourceVector({'CPU': 1}))
    node._cluster_nodes = (NodeSnapshot(node.node_id, node._ledger.total, node._ledger.available),)
    token = AllocationToken('controller-finalize-lease')
    node._ledger.allocate(ResourceVector({'CPU': 1}), token)
    lease = protocol.RequestWorkerLease(identity.lease_id, identity.task_id, identity.attempt_id,
        ResourceVector({'CPU': 1}), node.node_id, f.owner.worker_id, return_ids=(identity.object_id,))
    grant = protocol.GrantWorkerLease(identity.lease_id, identity.task_id, identity.attempt_id,
        node.node_id, f.publisher.worker_id, ('publisher.invalid', 1236), token)
    record = _LeaseRecord(lease, token, grant, state=protocol.LeaseExecutionState.RUNNING)
    node._leases = {identity.lease_id: record}
    # The publisher remains registered ALIVE; only the remote logical owner
    # receives a real membership death below. No callback executes dead Core.
    node._workers = {f.publisher.worker_id: _WorkerSlot(f.publisher.worker_id,
        process=SimpleNamespace(is_alive=lambda: True), address=grant.worker_address,
        active_lease_id=identity.lease_id)}
    node._output_publication_journal = journal
    releases, worker_calls, finalize_calls, actual_replies = [], [], [], []
    lose_release = True
    corrupt_reply = True

    def pin(address, request):
        assert address == transfer.contained_owner_address
        assert f.call(ep.GetPublication(publication.reference)).snapshot.graph_active
        method = (f.table.prepare_stored_contained_reference
                  if type(request) is protocol.PrepareStoredContainedPin
                  else f.table.promote_stored_contained_reference)
        return protocol.StoredContainedPinReply(request, method(
            request.transfer, authority_worker_id=request.authority_worker_id))

    def release(address, request):
        nonlocal lose_release
        assert address == transfer.contained_owner_address
        assert request.owner_worker_id == transfer.contained_owner_worker_id
        assert f.call(ep.GetPublication(publication.reference)).snapshot.fence is not None
        reply = f.rpc(address, 'release_contained_reference', request)
        releases.append((request, reply))
        if lose_release:
            lose_release = False
            raise TimeoutError('actual child Release ACK lost before Node finalize')
        return reply

    def register(value):
        assert value == manifest
        assert handoffs.register(value, identity.attempt_id).manifest == manifest

    def complete_owner(witness):
        assert f.gcs.workers.get(f.owner.worker_id).state is protocol.WorkerMembershipState.ALIVE
        recorded = handoffs.record_complete(witness)
        return wire.OutputHandoffCompleteAck(recorded.complete, True)

    def forbidden(*_args, **_kwargs):
        pytest.fail('completed INLINE case attempted rollback, storage or unexpected transport')

    adapter = OutputPublicationNodeAdapter(journal, register_owner=register,
        report_complete=complete_owner, report_rollback=forbidden,
        publication_value=lambda value: ep.TaskPublication(value, publication.owner_address),
        publication_rpc=f.call, abort_owner=forbidden, prepare_child=pin, promote_child=pin,
        release_child=release, seal_replica=forbidden, drop_replica=forbidden)
    node._output_publications = adapter
    assert node._handle_prepare_output_publication(wire.PrepareOutputPublication(manifest, b'x')).accepted
    completion = protocol.CompleteWorkerLease(identity.lease_id, identity.task_id, identity.attempt_id,
        f.publisher.worker_id, protocol.TaskReplyStatus.SUCCEEDED)
    completed = node._handle_complete_output_worker_lease(completion, identity)
    assert completed.accepted and completed.released
    envelope = completed.output_publication
    assert envelope is not None and adapter.report_terminal(identity)
    assert f.table.release_borrowed_reference(f.child, source.owner_table_token)
    assert f.table.release_contained_reference(f.child, source.original_source.hold)
    assert f.table.snapshot(f.child).contained_holds == frozenset((transfer.final_hold,))
    publisher = object.__new__(WorkerServer)
    publisher.worker_id, publisher.node_id = f.publisher.worker_id, node.node_id
    publisher._execution_lock = Lock()
    key = identity.attempt_id, identity.lease_id
    publisher._lease_bindings = {identity.lease_id: identity.attempt_id}
    publisher._attempt_leases = {identity.attempt_id: identity.lease_id}
    publisher._replies = {key: protocol.TaskReply(identity.task_id, identity.attempt_id,
        f.publisher.worker_id, protocol.TaskReplyStatus.SUCCEEDED, (envelope.result,), output_publication=envelope)}
    publisher._cached_pushes = {}
    publisher._cached_output_manifests = {key: manifest}
    publisher._completion_acked = {key}

    def worker_rpc(address, handler, request):
        assert address == grant.worker_address and handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER
        assert f.gcs.workers.get(f.publisher.worker_id).state is protocol.WorkerMembershipState.ALIVE
        worker_calls.append(request)
        return publisher._handle_finalize_output_owner_death(request)

    node._background_rpc = worker_rpc

    def controller_rpc(address, handler, request):
        nonlocal corrupt_reply
        assert address == f.gcs.nodes.address(node.node_id)
        if handler == 'install_owner_death_fence':
            assert request.scope is protocol.OwnerDeathFenceScope.PUBLICATION_EXACT
            return node._handle_install_owner_death_fence(request)
        assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER
        finalize_calls.append(request)
        actual = node._handle_finalize_output_owner_death(request)
        assert actual.cleaned and actual.closed_holds == adapter.owner_death_closed_holds(identity)
        actual_replies.append(actual)
        if corrupt_reply:
            corrupt_reply = False
            malformed = replace(actual)
            if corruption == 'wrong-request':
                changed = replace(request, owner_death=replace(request.owner_death, detection_id='changed-response-request'))
                object.__setattr__(malformed, 'request', changed)
            else:
                object.__setattr__(malformed.closed_holds.releases[0].hold, 'transfer_token', '')
            return malformed
        return actual

    controller = f.gcs.publication_control
    monkeypatch.setattr(controller, 'rpc', controller_rpc)
    death = f.worker_death(f.owner)
    assert f.gcs.workers.get(f.owner.worker_id).death == death
    assert controller.pending_deaths() == (publication.reference,)
    assert controller.drive_one()  # actual narrow Node fence
    work = controller._deaths[publication.reference]
    assert work.node_fenced and not work.node_finalized
    assert not controller.drive_one()  # actual first child Release, lost ACK
    assert len(releases) == 1 and releases[0][1].released
    assert not worker_calls and journal.snapshot(identity).result_retained
    assert work.closed_holds is None and not work.inflight
    assert not controller.drive_one()  # actual Node cleanup, malformed outer ACK
    assert len(releases) == 3 and releases[0][0] == releases[1][0]
    assert not releases[1][1].released and len(worker_calls) == 1
    assert not journal.snapshot(identity).result_retained and not publisher._replies
    assert journal.snapshot(identity).complete == envelope.complete
    assert not f.table.snapshot(f.child).contained_holds
    assert work.closed_holds is None and not work.node_finalized and not work.finished and not work.inflight
    pending = f.call(ep.GetPublication(publication.reference)).snapshot
    assert pending.complete == envelope.complete and pending.graph_active
    assert pending.closed_holds is None and pending.receipt(ep.PublicationStage.RETIRED) is None
    assert controller.pending_deaths() == (publication.reference,)
    assert controller.drive_one()  # exact Node reply replay, no repeated custody work
    assert finalize_calls[0] == finalize_calls[1] == finalize_calls[2]
    assert actual_replies[0] == actual_replies[1]
    assert len(releases) == 3 and len(worker_calls) == 1
    assert work.node_finalized and work.closed_holds == actual_replies[0].closed_holds
    f.drain()  # independent real owner-wide sweeps and graph retirement
    retired = f.call(ep.GetPublication(publication.reference)).snapshot
    assert retired.closed_holds == actual_replies[0].closed_holds
    assert retired.complete == envelope.complete and not retired.graph_active
    assert retired.receipt(ep.PublicationStage.RETIRED) is not None
    assert not controller.pending_deaths() and work.finished and not work.inflight
    assert node._ledger.available == node._ledger.total and node._object_store.used_bytes == 0
    assert len(releases) == 3 and len(worker_calls) == 1
