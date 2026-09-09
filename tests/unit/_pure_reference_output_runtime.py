"""Small real publication/store composition for reference reconstruction tests.

Only in-process Node/owner delivery is supplied here. Real discovery,
publication adapter/journal, owner handoff and ObjectStore(1024) run
synchronously, with at most three publications and exactly one output each.
The default INLINE helper path is reused; STORED never becomes INLINE to make
a test pass. No Node, Core, Worker, thread, timer, socket or wait is started.
"""

import hashlib
import threading

from miniray import protocol
from miniray.object_store import ObjectStore
from miniray.node import NodeServer
from miniray.object_manager import ObjectManager
from miniray.output_publication import OutputPublicationID
from miniray.output_publication_journal import OutputPublicationStage
from miniray.task_outputs import TaskExecutionKey
from tests.unit._pure_output_runtime import PureOutputRuntime, _metadata


class PureReferenceOutputRuntime(PureOutputRuntime):
    """Reuse the existing output path with one 1-KiB physical store."""

    def __init__(self, core):
        super().__init__(core)
        self.store = ObjectStore(1024)
        self.node = node = object.__new__(NodeServer)
        node.node_id = core.node_id
        node._node_pid = self.incarnation.node_pid
        node._registration_epoch = self.incarnation.registration_epoch
        node._state_lock = threading.RLock()
        node._object_store = self.store
        node._object_manager = ObjectManager(core.node_id, self.store)
        node._sealed_metadata, node._dropped_metadata = {}, {}
        node._local_replica_write_claims, node._object_localization_locks = {}, {}
        node._owner_death_fences = {}
        node._output_publication_journal = self.journal
        self.replicas = node._sealed_metadata
        self.dropped = set()
        self.adapter._seal_replica = self._seal
        self.adapter._drop_replica = self._rollback_drop
        core.gcs_address = self.gcs_address
        core._rpc = self.rpc

    def complete(self, push, values, *, inline_threshold=1024):
        execution = TaskExecutionKey.from_task_spec(push.spec)
        identity = OutputPublicationID(push.lease_id, execution)
        assert len(identity.output_ids) == 1
        assert identity in self.pushes or len(self.pushes) < 3
        return super().complete(push, values, inline_threshold=inline_threshold)

    def _seal(self, effect, descriptor, payload):
        assert effect.stage is OutputPublicationStage.MATERIALIZE
        assert len(payload) == descriptor.size_bytes <= 1024
        assert hashlib.sha256(payload).hexdigest() == descriptor.checksum
        return self.node._seal_output_publication_replica(effect, descriptor, payload)

    def _drop(self, request):
        assert type(request) is protocol.DropObjectReplica
        reply = self.node._handle_drop_object_replica(request)
        if reply.status in (protocol.DropObjectReplicaStatus.DROPPED, protocol.DropObjectReplicaStatus.ALREADY_DROPPED):
            self.dropped.add(request)
        return reply

    def _rollback_drop(self, effect, request):
        assert effect.stage is OutputPublicationStage.SLOT_DROP
        assert effect.publication_id.attempt_id == request.producer_attempt_id
        reply = self.node._drop_output_publication_replica(effect, request)
        if reply.status in (protocol.DropObjectReplicaStatus.DROPPED, protocol.DropObjectReplicaStatus.ALREADY_DROPPED):
            self.dropped.add(request)
        return reply

    def rpc(self, address, handler, request):
        if handler == 'drop_object_replica':
            assert address == self.node_address
            _metadata(request)
            self.calls.append((handler, request))
            reply = self._drop(request)
            _metadata(reply)
            return reply
        return super().rpc(address, handler, request)
