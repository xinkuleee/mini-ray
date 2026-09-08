"""Small in-memory unified Prepare/Complete replies for pure Worker tests."""

from dataclasses import replace

from miniray import output_protocol as wire, protocol
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope,
)


class UnifiedWorkerRPC:
    def __init__(self):
        self.prepared = {}
        self.prepare_requests = []

    @staticmethod
    def _key(request):
        return request.lease_id, request.task_id, request.attempt_id, request.worker_id

    def prepare(self, request):
        assert type(request) is wire.PrepareOutputPublication
        request = replace(request)
        identity = request.manifest.publication_id
        header = request.manifest.header
        key = identity.lease_id, identity.task_id, identity.attempt_id, header.executor_worker_id
        previous = self.prepared.setdefault(key, request)
        assert previous == request, "publication replay changed its exact bytes or manifest"
        self.prepare_requests.append(request)
        return wire.PreparedOutputPublicationReply(request.request_identity, True)

    def envelope(self, completion):
        if completion.status is not protocol.TaskReplyStatus.SUCCEEDED:
            return None
        request = self.prepared[self._key(completion)]
        manifest = request.manifest
        header = manifest.header
        results = tuple(protocol.ResultDescriptor(
            slot.object_id, slot.tier, slot.size_bytes, header.owner_worker_id,
            header.node_incarnation.node_id, slot.checksum,
            payload if slot.tier is protocol.ResultStorage.INLINE else None,
        ) for slot, payload in zip(manifest.slots, request.slot_payloads))
        return OutputPublicationEnvelope(
            manifest, OutputPublicationCompleteWitness.for_manifest(manifest), results,
        )

    def complete(self, request):
        return protocol.CompleteWorkerLeaseReply(
            request.lease_id, request.task_id, request.attempt_id, request.worker_id,
            request.status, protocol.LeaseExecutionState.COMPLETED, True, True,
            scheduling_key=request.scheduling_key, target_execution=request.target_execution,
            output_publication=self.envelope(request),
        )
