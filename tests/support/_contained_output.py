"""One current single-output owner-led publication with optional child Ref.

Real Core/owner/Node journal/Store callbacks; no runtime constructors, process,
socket or thread. Reference FIFO is manual unless a caller explicitly installs
the real bounded reference thread. No global graph or extra slot authority.
"""
from types import SimpleNamespace
import threading

from miniray import output_protocol as wire, protocol
from miniray.core import CoreWorker
from miniray.ids import LeaseID, WorkerID
from miniray.node import NodeServer, _LeaseRecord, _LeaseOutcome, _WorkerSlot
from miniray.object_manager import ObjectManager
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.resources import AllocationToken, NodeSnapshot, ResourceLedger, ResourceVector
from tests.unit._pure_core import make_pure_core


class ContainedOutput:
    def __init__(self, *, same_owner=False, contained=True, stored=False):
        self.core = make_pure_core()
        self.child_owner = self.core if same_owner else make_pure_core()
        self.child_owner.job_id = self.core.job_id
        self.child_owner.owner_address = ('child.invalid', 103) if not same_owner else self.core.owner_address
        self.contained, self.stored = contained, stored
        self.ref = self.child_ref = self.pending = self.transfer = self.edge = None
        self.release_calls, self.released, self.drops, self.prepares, self.promotions = [], [], [], [], []
        self.fail_release = False
        self.release_observer = None
        self.journal = OutputPublicationJournal()
        self.core._rpc = self.rpc
        self.core._borrow_rpc = self.release_child
        self.node = node = object.__new__(NodeServer)
        node.node_id = self.core.node_id
        node._node_pid, node._registration_epoch = 21001, 1
        node._state_lock = threading.RLock()
        node._registered_with_gcs = True
        node._gcs_address = None
        node._ledger = ResourceLedger(ResourceVector({'CPU': 1}))
        node._cluster_nodes = (NodeSnapshot(node.node_id, node._ledger.total, node._ledger.total),)
        node._leases, node._lease_outcomes = {}, {}
        node._owner_death_fences, node._dependency_pin_cleanups = {}, {}
        node._object_store = ObjectStore(8192)
        node._object_manager = ObjectManager(node.node_id, node._object_store)
        node._sealed_metadata, node._dropped_metadata, node._local_replica_write_claims = {}, {}, {}
        node._object_localization_locks = {}
        node._output_publication_journal = self.journal
        node.event_sink = None
        node._background_rpc = self.owner_rpc
        node._output_publications = self.adapter = node._make_output_publication_adapter()

    def register(self):
        if self.contained:
            self.child_ref = self.child_owner.put({'answer': 42})
        args = ({'child': self.child_ref},) if self.contained and self.child_owner is self.core else ()
        self.pending, self.ref = self.core._register_submission(
            self.core.define_remote_function(lambda: None), args, {}, ResourceVector({'CPU': 1}),
            max_retries=1, _enqueue=True)
        if self.child_ref is not None:
            self.child_id = self.child_ref.object_id
            self.lineage = self.child_owner.owner_table.snapshot(self.child_id).lineage_tokens
        node, p = self.node, self.pending
        executor = self.child_owner.worker_id
        request = protocol.RequestWorkerLease(LeaseID.random(), p.task_id, p.spec.attempt_id, p.spec.resources,
            node.node_id, self.core.worker_id, target_node_id=node.node_id, return_ids=p.output_ids,
            requester_owner_address=self.core.owner_address)
        token = node._ledger.allocate(p.spec.resources, AllocationToken('contained-lease'))
        self.grant = protocol.GrantWorkerLease(request.lease_id, p.task_id, p.spec.attempt_id,
            node.node_id, executor, self.child_owner.owner_address, token)
        self.request = request
        node.worker_id = executor
        node._worker_order = (executor,)
        node._workers = {executor: _WorkerSlot(executor, process=SimpleNamespace(is_alive=lambda: True),
            address=self.child_owner.owner_address, active_lease_id=request.lease_id)}
        node._sync_first_worker_compat_locked()
        node._leases[request.lease_id] = _LeaseRecord(request, token, self.grant)
        node._lease_outcomes[request.lease_id] = _LeaseOutcome(request, self.grant)
        node._refresh_local_cached_availability_locked()

    def complete(self):
        p, node = self.pending, self.node
        assert node._handle_start_worker_lease(protocol.StartWorkerLease(
            self.request.lease_id, p.task_id, p.spec.attempt_id, self.grant.worker_id)).accepted
        identity = OutputPublicationID(self.request.lease_id, p.execution)
        self.session = OutputDiscoverySession(OutputPublicationHeader(identity, p.spec.job_id,
            self.grant.worker_id, self.core.worker_id,
            OutputPublicationNodeIncarnation(node.node_id, node._node_pid, node._registration_epoch)),
            inline_threshold=0 if self.stored else 4096, owner_address=self.child_owner.owner_address)
        value = {'child': self.child_ref} if self.contained else 'stored-result'
        self.outputs = self.session.discover((value,))
        if self.contained:
            self.transfer, = self.outputs.manifest.slots[0].transfers
            self.edge, = self.outputs.manifest.slots[0].edges
        prepared = node._handle_prepare_output_publication(wire.PrepareOutputPublication(
            self.outputs.manifest, self.outputs.slot_payloads))
        assert prepared.accepted
        self.session.release_sources_after_promotions()
        reply = node._handle_complete_worker_lease(protocol.CompleteWorkerLease(
            self.request.lease_id, p.task_id, p.spec.attempt_id, self.grant.worker_id, protocol.TaskReplyStatus.SUCCEEDED))
        assert reply.accepted and reply.released and reply.output_publication is not None
        self.envelope = reply.output_publication
        return protocol.TaskReply(p.task_id, p.spec.attempt_id, self.grant.worker_id,
            protocol.TaskReplyStatus.SUCCEEDED, self.envelope.results, output_publication=self.envelope)

    def owner_rpc(self, address, handler, request):
        if handler in ('prepare_stored_contained_pin', 'promote_stored_contained_pin'):
            assert address == self.child_owner.owner_address
            result = getattr(self.child_owner, handler)(request)
            assert result.accepted
            (self.prepares if handler.startswith('prepare') else self.promotions).append((request,result))
            return result
        assert address == self.core.owner_address
        method = {wire.REGISTER_OUTPUT_HANDOFF_HANDLER:self.core.register_output_handoff,
                  wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER:self.core.report_output_handoff_complete,
                  wire.REPORT_OUTPUT_HANDOFF_ROLLBACK_HANDLER:self.core.report_output_handoff_rollback}[handler]
        return method(request)

    def rpc(self, address, handler, request):
        assert address == self.core.node_address
        if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
            return self.node._handle_ack_output_publication_adopted(request)
        assert handler == 'drop_object_replica'
        reply = self.node._handle_drop_object_replica(request)
        self.drops.append((request,reply))
        return reply

    def release_child(self, address, handler, request):
        assert address == self.child_owner.owner_address and handler == 'release_contained_reference'
        assert request.object_id == self.child_id and request.hold == self.transfer.final_hold
        self.release_calls.append(request)
        assert len(self.release_calls) <= 3
        if self.fail_release:
            self.fail_release = False
            raise RuntimeError('child release unavailable')
        result = self.child_owner.release_contained_reference(request)
        assert result.accepted
        self.released.append(result)
        if self.release_observer is not None:
            self.release_observer(request, result)
        return result
