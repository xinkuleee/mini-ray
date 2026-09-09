"""Owner-side exact RPC composition for the two teaching guarantees.

This module owns no READY, reference-count, graph or resource authority. It
retains metadata needed to address the GCS after local GC; each caller still
serializes its local decision and rechecks it after RPC. Bytes never go here.
"""
from __future__ import annotations

from dataclasses import replace
from threading import RLock

from . import enhanced_publication as wire
from .errors import SystemTaskError


class PublicationRejected(SystemTaskError):
    def __init__(self, reply):
        self.reply = reply
        super().__init__(reply.error or 'GCS rejected publication')


class PublicationClient:
    def __init__(self, rpc):
        self._rpc = rpc
        self._publications = {}
        self._current = {}
        self._lock = RLock()

    def remember(self, publication):
        publication = replace(publication)
        reference = publication.reference
        with self._lock:
            previous = self._publications.setdefault(reference, publication)
            if previous != publication:
                raise SystemTaskError('owner publication metadata changed')
            current = self._current.get(publication.object_id)
            if current is None or current.key.attempt_id.attempt_number <= reference.key.attempt_id.attempt_number:
                self._current[publication.object_id] = reference
        return replace(publication)

    def current(self, object_id):
        with self._lock:
            reference = self._current.get(object_id)
            publication = self._publications.get(reference)
            return None if publication is None else replace(publication)

    def call(self, request, stage=None):
        request = replace(request)
        reply = self._rpc(wire.PUBLICATION_HANDLER, request)
        if type(reply) is not wire.PublicationReply:
            raise SystemTaskError('invalid GCS publication reply')
        reply = replace(reply)
        if reply.request != request:
            raise SystemTaskError('GCS publication reply changed its request')
        if not reply.accepted:
            raise PublicationRejected(reply)
        if stage is not None and (reply.receipt is None or reply.receipt.stage is not stage):
            raise SystemTaskError('GCS reply lacks its exact stage receipt')
        return reply

    def query(self, publication):
        return self.call(wire.GetPublication(publication.reference)).snapshot

    def begin(self, publication):
        self.remember(publication)
        self.call(wire.BeginPublication(publication), wire.PublicationStage.INTENT)
        reply = self.call(wire.PrepareGraph(publication.reference), wire.PublicationStage.PREPARED)
        if not reply.snapshot.forward_open:
            raise SystemTaskError('historical graph reservation is not forward permission')
        return reply

    def commit_task(self, publication, complete):
        if wire.PublicationRef(complete.publication_id, complete.manifest_digest) != publication.reference:
            raise SystemTaskError('Task Complete changed the publication being committed')
        self.call(wire.RecordTerminal(complete), wire.PublicationStage.TERMINAL)
        reply = self.call(wire.CommitGraph(publication.reference), wire.PublicationStage.COMMITTED)
        if not reply.snapshot.forward_open:
            raise SystemTaskError('fenced graph commit cannot authorize owner publication')
        return reply.receipt

    def commit_put(self, publication, prepared):
        reply = self.call(wire.CommitGraph(publication.reference, prepared), wire.PublicationStage.COMMITTED)
        if not reply.snapshot.forward_open:
            raise SystemTaskError('fenced put graph cannot authorize owner installation')
        return reply.receipt

    def adopt(self, proof):
        return self.call(wire.RecordAdoption(proof), wire.PublicationStage.ADOPTED).receipt

    def fence(self, publication, decision):
        """Ensure an existing exact record is closed; do not ACK a new choice.

GC can follow an earlier Node-loss abort. Returning that canonical history
is a barrier only, never a receipt asserting this later decision committed.
"""
        snapshot = self.query(publication)
        if snapshot is not None and snapshot.receipt(wire.PublicationStage.RETIRED) is not None:
            return snapshot.receipt(wire.PublicationStage.RETIRED)
        if snapshot is not None and snapshot.fence is not None:
            return snapshot.receipt(wire.PublicationStage.FENCED)
        return self.call(wire.FencePublication(publication, decision), wire.PublicationStage.FENCED).receipt

    def retire(self, publication, releases, deaths=()):
        """Ensure this membership is retired, preserving its first proof."""
        snapshot = self.query(publication)
        if snapshot is not None and snapshot.receipt(wire.PublicationStage.RETIRED) is not None:
            return snapshot.receipt(wire.PublicationStage.RETIRED)
        closed = wire.ClosedContainedHolds(publication.reference, tuple(releases), tuple(deaths))
        return self.call(wire.RetireGraph(closed), wire.PublicationStage.RETIRED).receipt
