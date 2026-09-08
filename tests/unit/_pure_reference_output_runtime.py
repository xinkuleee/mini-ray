"""Small real publication/store composition for reference reconstruction tests.

Only the in-process Node/GCS boundary is supplied here. Real discovery,
publication adapter/journal, recovery authority and ObjectStore(1024) run
synchronously, with at most two publications and three selected slots each.
The default INLINE helper path is reused; STORED never becomes INLINE to make
a test pass. No Node, Core, Worker, thread, timer, socket or wait is started.
"""

import hashlib

from miniray import output_protocol as wire, protocol
from miniray.object_store import ObjectStore
from miniray.output_publication import OutputPublicationID
from miniray.output_publication_journal import (
    OutputPublicationSlotCleanupProof, OutputPublicationStage,
)
from miniray.ownership import ObjectState
from miniray.task_outputs import TaskExecutionKey
from tests.unit._pure_output_runtime import PureOutputRuntime, _metadata


class PureReferenceOutputRuntime(PureOutputRuntime):
    """Reuse the existing output path with one 1-KiB physical store."""

    def __init__(self, core):
        super().__init__(core)
        self.store = ObjectStore(1024)
        self.replicas = {}
        self.dropped = set()
        self.adapter._seal_replica = self._seal
        self.adapter._drop_replica = self._rollback_drop
        core.gcs_address = self.gcs_address
        core._rpc = self.rpc

    def complete(self, push, values, *, inline_threshold=1024):
        execution = push.target_execution or TaskExecutionKey.from_task_spec(push.spec)
        identity = OutputPublicationID(push.lease_id, execution)
        assert 1 <= len(identity.output_ids) <= 3
        assert identity in self.pushes or len(self.pushes) < 2
        return super().complete(push, values, inline_threshold=inline_threshold)

    def _seal(self, effect, descriptor, payload):
        assert effect.stage is OutputPublicationStage.MATERIALIZE
        manifest = self.journal.snapshot(effect.publication_id).manifest
        slot = manifest.slots[effect.slot_index]
        assert descriptor == protocol.ResultDescriptor(
            slot.object_id, protocol.ResultStorage.OBJECT_STORE, slot.size_bytes,
            manifest.header.owner_worker_id, manifest.header.node_incarnation.node_id,
            slot.checksum,
        )
        assert len(payload) == descriptor.size_bytes <= 1024
        assert hashlib.sha256(payload).hexdigest() == descriptor.checksum
        identity = (effect.publication_id, descriptor)
        previous = self.replicas.get(descriptor.object_id)
        if previous is not None:
            assert previous == identity
            assert self.store.get(descriptor.object_id) == payload
        else:
            self.store.put(descriptor.object_id, payload)
            self.replicas[descriptor.object_id] = identity
        assert self.store.snapshot(descriptor.object_id).sealed
        return descriptor

    def _drop(self, request):
        assert type(request) is protocol.DropObjectReplica
        if request in self.dropped:
            status = protocol.DropObjectReplicaStatus.ALREADY_DROPPED
        else:
            identity, descriptor = self.replicas[request.object_id]
            assert request == protocol.DropObjectReplica(
                descriptor.object_id, identity.attempt_id, descriptor.owner_worker_id,
                descriptor.node_id, descriptor.checksum,
            ), "a stale cleanup must never delete a successor's bytes"
            payload = self.store.get(request.object_id)
            assert len(payload) == descriptor.size_bytes
            assert hashlib.sha256(payload).hexdigest() == descriptor.checksum
            assert self.store.delete(request.object_id)
            del self.replicas[request.object_id]
            self.dropped.add(request)
            status = protocol.DropObjectReplicaStatus.DROPPED
        return protocol.DropObjectReplicaReply(
            request.object_id, request.producer_attempt_id, request.owner_worker_id,
            request.node_id, request.checksum, status,
        )

    def _rollback_drop(self, effect, request):
        assert effect.stage is OutputPublicationStage.SLOT_DROP
        assert effect.publication_id.attempt_id == request.producer_attempt_id
        return self._drop(request)

    def rpc(self, address, handler, request):
        if handler == "drop_object_replica":
            assert address == self.node_address
            _metadata(request)
            self.calls.append((handler, request))
            reply = self._drop(request)
            _metadata(reply)
            return reply
        if (handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER
                and type(request) is wire.ReportOutputPublicationSlotCollected):
            # Reconstruction retires a LOST membership without collecting the
            # logical ObjectID. Its exact active retirement plan is the proof.
            snapshot = self.core.owner_table.snapshot(request.proof.object_id)
            if snapshot.output_retirement_id is not None:
                assert address == self.gcs_address
                assert snapshot.state is ObjectState.LOST
                assert snapshot.output_retirement_id == request.proof.cleanup_id
                assert snapshot.output_publication.publication_id == (
                    request.proof.complete.publication_id
                )
                assert request.proof.object_id not in self.replicas
                _metadata(request)
                self.calls.append((handler, request))
                reply = wire.OutputRecoveryReply(
                    request, self.recovery.report_slot_collected(request.proof),
                )
                _metadata(reply)
                return reply
        return super().rpc(address, handler, request)

    def retire_fenced_reply(self, pending, reply):
        """Explicit test-Node cleanup after a legal old envelope is fenced.

        This is not successful owner adoption. It keeps the old Complete
        witness, deletes only its exact unadopted replicas, then gives the
        actual recovery authority and Node journal matching cleanup receipts.
        """
        envelope = reply.output_publication
        identity = envelope.publication_id
        assert self.replies[identity] == reply
        assert identity.execution == pending.execution
        assert self.core._recovery.task_record(pending.task_id).current_attempt != (
            pending.spec.attempt_id
        )
        assert self.recovery.snapshot(identity).adopted is None
        assert self.adapter.report_terminal(identity)
        for index, descriptor in enumerate(envelope.results):
            current = self.core.owner_table.snapshot(descriptor.object_id)
            assert current.current_attempt != identity.attempt_id
            assert current.output_publication is None
            if descriptor.storage is protocol.ResultStorage.OBJECT_STORE:
                self._drop(protocol.DropObjectReplica(
                    descriptor.object_id, identity.attempt_id, descriptor.owner_worker_id,
                    descriptor.node_id, descriptor.checksum,
                ))
            proof = OutputPublicationSlotCleanupProof(
                envelope.complete, self.core.worker_id, index, descriptor.object_id,
                "fenced-test-output:{}:{}".format(identity.graph_transaction_id, index),
            )
            self.recovery.report_slot_collected(proof)
            self.journal.retire_slot(identity, index, proof)
        assert not self.journal.snapshot(identity).retained_result_slots
