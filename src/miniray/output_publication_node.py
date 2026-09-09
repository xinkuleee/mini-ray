"""Node effects for one single-output publication journal.

Storage tier chooses how the result is materialized: INLINE bytes stay in the
journal's reply cache; STORED bytes are sealed through a local callback. The
owner registers the exact manifest before child custody or materialization;
local Complete then requires all materialization and child promotion ACKs.

The journal is the only phase/intent authority.  This adapter owns transient
nonblocking operation tickets and a metadata-only terminal-report outbox, not
another publication state machine.  External callbacks run without a journal
or adapter-state lock.  The two local callbacks (seal/drop and lease completion)
must never perform RPC; their mutations are composed with the journal lock.

NodeServer registers this backend for ordinary Task outputs.  Its lease,
storage and Worker-cleanup callbacks keep process-local effects here while
owner adoption remains a separate authority. The owner callbacks validate the
exact request and ACK before returning; their failures preserve local replay
obligations. Complete reporting does not gate local lease/resource release.
"""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import replace
from threading import RLock
from typing import Callable, Optional, Tuple

from . import protocol
from .ids import NodeID, WorkerID
from .output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationConflictError,
    OutputPublicationEnvelope, OutputPublicationError, OutputPublicationID,
    OutputPublicationManifest, _opaque, _require_type, _sequence, _uint,
)
from .output_publication_journal import (
    OutputPublicationAck, OutputPublicationAckDisposition,
    OutputPublicationEffect, OutputPublicationJournal,
    OutputPublicationJournalState, OutputPublicationJournalStateError,
    OutputPublicationRollbackTombstone, OutputPublicationStage,
)
from .ownership import StoredContainedReferenceDisposition
from .publication_gate import OutputPublicationGatePhase, GraphReservationOutcome
from .transport import Address


class OutputPublicationBusy(OutputPublicationError):
    """Another operation holds this publication's nonblocking progress ticket."""


class OutputPublicationRemoteError(OutputPublicationError):
    """An exact typed reply rejected an effect; no rollback is implied."""


class OutputPublicationNodeAdapter:
    """Compose owner registration and child handoffs with the local journal.

    ``seal_replica`` and ``drop_replica`` receive the full effect identity.
    They are local, idempotent operations: a failure after mutation is recovered
    with the same identity.  A slot DROP must also discard any partial write.
    ``commit_lease`` passed to :meth:`complete` must likewise be local and
    idempotent.  NodeServer must validate the exact lease/Worker/owner binding
    before entry and use one fixed lock order for all such local operations.
    """

    def __init__(
        self, journal: OutputPublicationJournal, *,
        register_owner: Callable[[OutputPublicationManifest], None],
        report_complete: Callable[[OutputPublicationCompleteWitness], None],
        report_rollback: Callable[..., None],
        publication_value: Callable[[OutputPublicationManifest], object],
        publication_rpc: Callable[[object], object],
        abort_owner: Callable[[object, object], object],
        prepare_child: Callable[[Address, protocol.PrepareStoredContainedPin], protocol.StoredContainedPinReply],
        promote_child: Callable[[Address, protocol.PromoteStoredContainedPin], protocol.StoredContainedPinReply],
        release_child: Callable[[Address, protocol.ReleaseContainedReference],
                                protocol.ReleaseContainedReferenceReply | protocol.GetWorkerStateReply],
        seal_replica: Callable[[OutputPublicationEffect, protocol.ResultDescriptor, bytes], protocol.ResultDescriptor],
        drop_replica: Callable[[OutputPublicationEffect, protocol.DropObjectReplica], protocol.DropObjectReplicaReply],
        test_checkpoint: Optional[Callable[[OutputPublicationManifest, OutputPublicationGatePhase], None]] = None,
    ) -> None:
        _require_type(journal, OutputPublicationJournal, "journal")
        callbacks = {
            "register_owner": register_owner, "report_complete": report_complete,
            "report_rollback": report_rollback,
            "publication_value": publication_value, "publication_rpc": publication_rpc,
            "abort_owner": abort_owner,
            "prepare_child": prepare_child, "promote_child": promote_child,
            "release_child": release_child, "seal_replica": seal_replica,
            "drop_replica": drop_replica,
        }
        for name, callback in callbacks.items():
            if not callable(callback):
                raise TypeError("{} must be callable".format(name))
        if test_checkpoint is not None and not callable(test_checkpoint):
            raise TypeError("test_checkpoint must be callable or None")
        self.journal = journal
        self._register_owner = register_owner
        self._report_complete = report_complete
        self._report_rollback = report_rollback
        self._publication_value = publication_value
        self._publication_rpc = publication_rpc
        self._abort_owner = abort_owner
        self._prepare_child = prepare_child
        self._promote_child = promote_child
        self._release_child = release_child
        self._seal_replica = seal_replica
        self._drop_replica = drop_replica
        self._test_checkpoint = test_checkpoint
        self._lock = RLock()
        self._tickets: set[OutputPublicationID] = set()
        self._terminal_pending: dict[OutputPublicationID, OutputPublicationCompleteWitness] = {}
        self._terminal_reported: dict[OutputPublicationID, OutputPublicationCompleteWitness] = {}
        self._lease_pending: dict[OutputPublicationID, OutputPublicationCompleteWitness] = {}
        self._lease_converged: dict[OutputPublicationID, OutputPublicationCompleteWitness] = {}
        self._rollback_reported: dict[OutputPublicationID, OutputPublicationRollbackTombstone] = {}
        self._owner_cleaned: dict[OutputPublicationID, object] = {}
        # Death cleanup can start after Complete and therefore is not rollback.
        # Its exact child receipts survive partial progress and fence all new
        # forward work until Node/Worker cleanup has also acknowledged.
        self._owner_cleanup_deaths: dict[OutputPublicationID, object] = {}
        self._owner_cleanup_acks: dict[
            OutputPublicationEffect,
            protocol.ReleaseContainedReferenceReply | protocol.GetWorkerStateReply,
        ] = {}

    @contextmanager
    def _ticket(self, publication_id: OutputPublicationID, *, owner_cleanup: bool = False):
        _require_type(publication_id, OutputPublicationID, "publication_id")
        publication_id = replace(publication_id)
        with self._lock:
            if publication_id in self._tickets:
                raise OutputPublicationBusy("publication progress is already in flight")
            if not owner_cleanup and publication_id in self._owner_cleanup_deaths:
                raise OutputPublicationJournalStateError("owner death permanently fenced publication progress")
            self._tickets.add(publication_id)
        try:
            yield
        finally:
            with self._lock:
                self._tickets.remove(publication_id)

    def prepare(
        self, manifest: OutputPublicationManifest, slot_payloads: Tuple[bytes, ...],
    ) -> None:
        """Validate the whole batch, then resume its first unfinished effect.

        This method does not serialize user values.  A retry must supply the
        caller-retained manifest and exact streams.  In particular a bad later
        payload causes no registration, pin or object-store mutation.
        """
        _require_type(manifest, OutputPublicationManifest, "manifest")
        manifest = replace(manifest)
        payloads = _sequence(slot_payloads, "slot_payloads")
        if len(payloads) != len(manifest.slots):
            raise OutputPublicationConflictError("payloads must cover every selected slot")
        for slot, payload in zip(manifest.slots, payloads):
            _require_type(payload, bytes, "slot payload")
            if (len(payload) != slot.size_bytes
                    or hashlib.sha256(payload).hexdigest() != slot.checksum):
                raise OutputPublicationConflictError("payload changed its slot manifest")
        publication_id = manifest.publication_id
        with self._ticket(publication_id):
            self.journal.open(manifest)
            snapshot = self.journal.snapshot(publication_id)
            if snapshot.complete is not None:
                # Data may already be adopted/retired.  The Complete/outcome
                # path, not new forward effects, determines the reply.
                return
            if snapshot.state is not OutputPublicationJournalState.ACTIVE:
                raise OutputPublicationJournalStateError("rolled-back publication cannot prepare")
            effect = self.journal.begin_owner_register(publication_id)
            if not self.journal.acknowledged(effect):
                # The callback returns only after validating the exact owner ACK.
                # A lost reply leaves this intent pending for precise replay.
                self._register_owner(replace(manifest))
                self.journal.ack_owner_registered(OutputPublicationAck(effect))
            if (self._test_checkpoint is not None
                    and all(item.stage is OutputPublicationStage.OWNER_REGISTER
                            for item in self.journal.snapshot(publication_id).intents)):
                self._test_checkpoint(replace(manifest), OutputPublicationGatePhase.AFTER_OWNER_REGISTER_ACK)
            from .enhanced_publication import BeginPublication, PrepareGraph, ArmTask, PublicationStage
            publication = self._publication_value(replace(manifest))
            for request, stage in ((BeginPublication(publication), PublicationStage.INTENT),
                                   (PrepareGraph(publication.reference), PublicationStage.PREPARED)):
                if self.journal.publication_receipt(publication_id, stage) is None:
                    self._record_gcs(publication_id, request, stage, forward=True)
            for slot_index, slot in enumerate(manifest.slots):
                for transfer_index in range(len(slot.transfers)):
                    self._prepare_pin(publication_id, slot_index, transfer_index)
            for slot_index, payload in enumerate(payloads):
                self._materialize(publication_id, slot_index, payload)
            for slot_index, slot in enumerate(manifest.slots):
                for transfer_index in range(len(slot.transfers)):
                    self._promote_pin(publication_id, slot_index, transfer_index)
            if self._test_checkpoint is not None:
                self._test_checkpoint(replace(manifest), OutputPublicationGatePhase.AFTER_PROMOTIONS_ACK)
            if self.journal.publication_receipt(publication_id, PublicationStage.ARMED) is None:
                self._record_gcs(publication_id, ArmTask(self.journal.preparation_receipt(publication_id)),
                                 PublicationStage.ARMED, forward=True)

    def _record_gcs(self, publication_id, request, stage, *, forward=False):
        from .enhanced_publication import PublicationReply, PrepareGraph, PublicationErrorKind
        is_graph = type(request) is PrepareGraph
        if is_graph and self._test_checkpoint is not None:
            self._test_checkpoint(self._manifest(publication_id), OutputPublicationGatePhase.BEFORE_GRAPH_PREPARE)
        reply = self._publication_rpc(request)
        if type(reply) is not PublicationReply:
            raise OutputPublicationConflictError("GCS returned an invalid publication reply")
        reply = replace(reply)
        if reply.request != request:
            raise OutputPublicationConflictError("GCS publication reply changed request")
        if is_graph and self._test_checkpoint is not None:
            outcome = (GraphReservationOutcome.ACCEPTED if reply.accepted
                       else GraphReservationOutcome.CYCLE if reply.error_kind is PublicationErrorKind.CYCLE
                       else GraphReservationOutcome.REJECTED)
            self._test_checkpoint(self._manifest(publication_id),
                                  OutputPublicationGatePhase.AFTER_GRAPH_PREPARE_REPLY, outcome)
        if not reply.accepted:
            raise OutputPublicationRemoteError(reply.error or "GCS rejected publication progress")
        return self.journal.record_publication_reply(publication_id, request, reply, stage, forward=forward)

    def _prepare_pin(self, publication_id, slot_index, transfer_index):
        effect = self.journal.begin_prepare(publication_id, slot_index, transfer_index)
        if self.journal.acknowledged(effect):
            return
        transfer = self._manifest(publication_id).slots[slot_index].transfers[transfer_index]
        request = protocol.PrepareStoredContainedPin(transfer, transfer.contained_owner_worker_id)
        reply = self._prepare_child(transfer.contained_owner_address, request)
        expected = self._manifest(publication_id).slots[slot_index].transfers[transfer_index]
        expected_request = protocol.PrepareStoredContainedPin(expected, expected.contained_owner_worker_id)
        self._require_pin_reply(reply, expected_request)
        replay = reply.disposition is StoredContainedReferenceDisposition.ALREADY_PREPARED
        self.journal.ack_prepared(self._journal_ack(effect, replay=replay), reply)

    def _materialize(self, publication_id, slot_index, payload):
        # All work inside this scope is local.  A failure after seal but before
        # ACK leaves the MATERIALIZE intent as the exact rollback obligation.
        with self.journal.linearize(publication_id):
            effect = self.journal.begin_materialize(publication_id, slot_index)
            if self.journal.acknowledged(effect):
                return
            manifest = self._manifest(publication_id)
            slot = manifest.slots[slot_index]
            descriptor = protocol.ResultDescriptor(
                slot.object_id, slot.tier, slot.size_bytes, manifest.header.owner_worker_id,
                manifest.header.node_incarnation.node_id, slot.checksum,
                payload if slot.tier is protocol.ResultStorage.INLINE else None,
            )
            if slot.tier is protocol.ResultStorage.OBJECT_STORE:
                descriptor = self._seal_replica(replace(effect), descriptor, payload)
            self.journal.ack_materialized(OutputPublicationAck(effect), descriptor)

    def _promote_pin(self, publication_id, slot_index, transfer_index):
        effect = self.journal.begin_promote(publication_id, slot_index, transfer_index)
        if self.journal.acknowledged(effect):
            return
        transfer = self._manifest(publication_id).slots[slot_index].transfers[transfer_index]
        request = protocol.PromoteStoredContainedPin(transfer, transfer.contained_owner_worker_id)
        reply = self._promote_child(transfer.contained_owner_address, request)
        expected = self._manifest(publication_id).slots[slot_index].transfers[transfer_index]
        expected_request = protocol.PromoteStoredContainedPin(expected, expected.contained_owner_worker_id)
        self._require_pin_reply(reply, expected_request)
        replay = reply.disposition is StoredContainedReferenceDisposition.ALREADY_PROMOTED
        self.journal.ack_promoted(self._journal_ack(effect, replay=replay), reply)

    def complete(
        self, publication_id: OutputPublicationID, *,
        commit_lease: Callable[[OutputPublicationCompleteWitness], None],
    ) -> OutputPublicationEnvelope:
        """Cross local Complete and release the lease before owner reporting.

        NodeServer prevalidates its local lease before entry.  If the local
        callback raises after Complete, the immutable witness and outbox remain:
        exact replay converges that lease rather than executing the task again.
        """
        if not callable(commit_lease):
            raise TypeError("commit_lease must be callable")
        with self._ticket(publication_id):
            with self.journal.linearize(publication_id):
                witness = OutputPublicationCompleteWitness.for_manifest(self._manifest(publication_id))
                try:
                    return self.journal.complete(publication_id, witness)
                finally:
                    # The reducer or a local failpoint may raise *after* its
                    # irreversible mutation.  Read the fact, not the return
                    # value, before registering and driving convergence.
                    completed = self.journal.snapshot(publication_id).complete
                    if completed is not None:
                        self._converge_lease(completed, commit_lease)

    def _converge_lease(self, witness, commit_lease):
        publication_id = witness.publication_id
        with self._lock:
            if publication_id not in self._terminal_reported:
                self._terminal_pending[replace(publication_id)] = replace(witness)
            if publication_id in self._lease_converged:
                return
            self._lease_pending[replace(publication_id)] = replace(witness)
        commit_lease(replace(witness))
        with self._lock:
            self._lease_converged[replace(publication_id)] = replace(witness)
            self._lease_pending.pop(publication_id, None)

    def pending_lease_completions(self) -> Tuple[OutputPublicationCompleteWitness, ...]:
        """Local cleanup obligations, independent of terminal-report delivery."""
        with self._lock:
            return tuple(replace(value) for value in self._lease_pending.values())

    def converge_completed(
        self, publication_id: OutputPublicationID, *,
        commit_lease: Callable[[OutputPublicationCompleteWitness], None],
    ) -> None:
        """Let the Node supervisor finish a known Complete after Worker loss.

        No result payload is needed and no missing Complete is manufactured.
        This also works after owner adoption retired the Node reply cache.
        """
        if not callable(commit_lease):
            raise TypeError("commit_lease must be callable")
        with self._ticket(publication_id):
            with self.journal.linearize(publication_id):
                witness = self.journal.snapshot(publication_id).complete
                if witness is None:
                    raise OutputPublicationJournalStateError("cannot converge an absent Complete")
                self._converge_lease(witness, commit_lease)

    def pending_terminal_reports(self) -> Tuple[OutputPublicationCompleteWitness, ...]:
        with self._lock:
            return tuple(replace(value) for value in self._terminal_pending.values())

    def report_terminal(self, publication_id: OutputPublicationID) -> bool:
        """Report one exact Complete to its owner, retaining it until ACK.

        This outbox does not authorize Complete or control local resource
        release. The callback validates the owner's exact ACK before returning.
        """
        _require_type(publication_id, OutputPublicationID, "publication_id")
        publication_id = replace(publication_id)
        with self._lock:
            if publication_id in self._owner_cleanup_deaths:
                return False
            witness = self._terminal_pending.get(publication_id)
            if witness is None:
                return False
            witness = replace(witness)
        from .enhanced_publication import PublicationStage, RecordTerminal
        self._record_gcs(publication_id, RecordTerminal(witness), PublicationStage.TERMINAL)
        self._report_complete(replace(witness))
        with self._lock:
            pending = self._terminal_pending.get(publication_id)
            if pending is None:
                return False
            if pending != witness:
                raise OutputPublicationConflictError("terminal outbox identity was rebound")
            self._terminal_reported[publication_id] = witness
            del self._terminal_pending[publication_id]
        return True

    def rollback(
        self, publication_id: OutputPublicationID, rollback_id: str, *, max_effects: int = 1,
    ) -> Optional[OutputPublicationRollbackTombstone]:
        """Advance bounded compensation, returning only after the exact owner ACK.

        Missing effect ACKs are compensated because the journal records intent
        before dispatch.  Unknown cleanup replies never remove an obligation.
        The caller retains the same rollback ID and retries this method.
        """
        _uint(max_effects, "max_effects", positive=True)
        with self._ticket(publication_id):
            self.journal.begin_rollback(publication_id, rollback_id)
            from .enhanced_publication import FencePublication, RetireGraph, PublicationStage
            publication = self._publication_value(self._manifest(publication_id))
            scope = self.journal.rollback_scope(publication_id)
            if self.journal.publication_receipt(publication_id, PublicationStage.FENCED) is None:
                owner_abort = self._abort_owner(publication, scope)
                self._record_gcs(publication_id, FencePublication(publication, owner_abort), PublicationStage.FENCED)
            for _ in range(max_effects):
                effect = self.journal.next_rollback_effect(publication_id)
                if effect is None:
                    break
                self._compensate(effect)
            snapshot = self.journal.snapshot(publication_id)
            tombstone = snapshot.rollback_tombstone
            if tombstone is None:
                return None
            self._record_gcs(publication_id, RetireGraph(self.journal.closed_rollback_holds(publication_id)),
                             PublicationStage.RETIRED)
            with self._lock:
                already_reported = self._rollback_reported.get(publication_id)
            if already_reported is not None:
                if already_reported != tombstone:
                    raise OutputPublicationConflictError("rollback report identity was rebound")
                return replace(tombstone)
            # Even an unknown registration reply requires exact owner cleanup
            # acknowledgement before the publisher can forget this obligation.
            self._report_rollback(replace(tombstone), manifest=replace(snapshot.manifest))
            with self._lock:
                self._rollback_reported[replace(publication_id)] = replace(tombstone)
            return replace(tombstone)

    def pending_rollbacks(self):
        """Expose journal-derived cleanup/report work for supervisor/drain.

        RETIRED locally does not mean the owner acknowledged rollback.  Keeping
        this query derived from the journal also covers a local exception at
        the final cleanup ACK without maintaining another phase bitmap.
        """
        pending = []
        for publication_id in self.journal.publication_ids():
            with self._lock:
                if publication_id in self._owner_cleaned:
                    continue
            snapshot = self.journal.snapshot(publication_id)
            if snapshot.rollback is None:
                continue
            with self._lock:
                reported = self._rollback_reported.get(publication_id)
            if reported is None:
                pending.append(snapshot.rollback)
        return tuple(pending)

    def rollback_reported(self, publication_id: OutputPublicationID) -> bool:
        """Whether the owner acknowledged the exact full compensation."""
        _require_type(publication_id, OutputPublicationID, "publication_id")
        with self._lock:
            return publication_id in self._rollback_reported

    def finish_owner_death(self, manifest, death, *, cleanup: Callable[[], bool]) -> bool:
        """Serialize Node/Worker cleanup with every forward publication effect.

        Each possible provisional and final child hold needs an exact release
        ACK or confirmed child-owner death, including a lost earlier ACK. The
        cleanup callback then retires Node bytes and Worker source custody.
        Neither network step holds an adapter or journal lock. A persistent
        owner-death fence survives errors; replay skips only proven releases.
        Successful Complete remains success metadata throughout this cleanup.
        """
        from .death_proofs import owner_death as _owner_death
        _require_type(manifest, OutputPublicationManifest, "manifest")
        manifest = replace(manifest)
        publication_id = manifest.publication_id
        death = _owner_death(death)
        if not callable(cleanup):
            raise TypeError("owner-death cleanup must be callable")
        with self._ticket(publication_id, owner_cleanup=True):
            if self._manifest(publication_id) != manifest or death.worker_id != manifest.header.owner_worker_id:
                raise OutputPublicationConflictError("owner cleanup changed publication identity")
            with self._lock:
                recorded = self._owner_cleanup_deaths.get(publication_id)
                if recorded is not None and recorded != death:
                    raise OutputPublicationConflictError("owner-death cleanup was rebound")
                previous = self._owner_cleaned.get(publication_id)
                if previous is not None:
                    if previous != death:
                        raise OutputPublicationConflictError("owner-death cleanup was rebound")
                    return True
                self._owner_cleanup_deaths[replace(publication_id)] = death
            from .enhanced_publication import FencePublication, PublicationStage, RecordTerminal
            publication = self._publication_value(manifest)
            if self.journal.publication_receipt(publication_id, PublicationStage.FENCED) is None:
                self._record_gcs(publication_id, FencePublication(publication, death), PublicationStage.FENCED)
            complete = self.journal.snapshot(publication_id).complete
            if complete is not None:
                self._record_gcs(publication_id, RecordTerminal(complete), PublicationStage.TERMINAL)
            # Complete forbids rollback, but owner death still removes every
            # exact hold. Unknown effects require releases as well as ACKed ones.
            for stage in (OutputPublicationStage.FINAL_RELEASE,
                          OutputPublicationStage.PROVISIONAL_RELEASE):
                for slot_index, slot in enumerate(manifest.slots):
                    for transfer_index in range(len(slot.transfers)):
                        effect = OutputPublicationEffect(
                            publication_id, manifest.manifest_digest, stage,
                            slot_index, transfer_index,
                        )
                        with self._lock:
                            acknowledged = effect in self._owner_cleanup_acks
                        if acknowledged:
                            continue
                        reply = self._release_child_hold(effect)
                        with self._lock:
                            self._owner_cleanup_acks[replace(effect)] = reply
            if cleanup() is not True:
                return False
            self.journal.retire_owner_death(publication_id, death)
            with self._lock:
                self._owner_cleaned[replace(publication_id)] = death
                self._terminal_pending.pop(publication_id, None)
                self._lease_pending.pop(publication_id, None)
            return True

    def owner_death_finished(self, publication_id):
        with self._lock:
            return publication_id in self._owner_cleaned

    def owner_death_closed_holds(self, publication_id):
        """Actual child closure; GCS retires graph after this Node reply."""
        from .enhanced_publication import ClosedContainedHolds
        with self._lock:
            if publication_id not in self._owner_cleaned:
                return None
            values = [value for effect, value in self._owner_cleanup_acks.items()
                      if effect.publication_id == publication_id]
        publication = self._publication_value(self._manifest(publication_id))
        return ClosedContainedHolds(publication.reference,
            tuple(replace(value) for value in values if type(value) is protocol.ReleaseContainedReferenceReply),
            tuple(dict.fromkeys(replace(value.death) for value in values if type(value) is protocol.GetWorkerStateReply)))

    def _compensate(self, effect: OutputPublicationEffect) -> None:
        publication_id = effect.publication_id
        manifest = self._manifest(publication_id)
        stage = effect.stage
        if stage is OutputPublicationStage.SLOT_DROP:
            with self.journal.linearize(publication_id):
                # This lock also covers local seal, so late materialization
                # cannot slip behind an acknowledged physical DROP.
                if self.journal.next_rollback_effect(publication_id) != effect:
                    raise OutputPublicationJournalStateError("slot DROP lost its rollback turn")
                slot = manifest.slots[effect.slot_index]
                if slot.tier is protocol.ResultStorage.OBJECT_STORE:
                    request = protocol.DropObjectReplica(
                        slot.object_id, publication_id.attempt_id, manifest.header.owner_worker_id,
                        manifest.header.node_incarnation.node_id, slot.checksum,
                    )
                    reply = self._drop_replica(replace(effect), request)
                    # The callback received IDs from the earlier snapshot.
                    # Re-read authority rather than trusting that mutable
                    # Python object graph as the expected ACK identity.
                    self._require_drop_reply(
                        reply, self._manifest(publication_id), effect.slot_index
                    )
                self.journal.ack_rollback(OutputPublicationAck(effect))
            return
        if stage not in (OutputPublicationStage.FINAL_RELEASE, OutputPublicationStage.PROVISIONAL_RELEASE):
            raise OutputPublicationJournalStateError("unknown compensation effect")
        reply = self._release_child_hold(effect)
        replay = type(reply) is protocol.ReleaseContainedReferenceReply and not reply.released
        self.journal.ack_rollback(self._journal_ack(effect, replay=replay), reply)

    def _release_child_hold(self, effect: OutputPublicationEffect):
        """Release one exact manifest hold, retaining real reply/death evidence."""
        publication_id = effect.publication_id
        manifest = self._manifest(publication_id)
        if effect.manifest_digest != manifest.manifest_digest:
            raise OutputPublicationConflictError("child release changed publication manifest")
        if effect.stage not in (OutputPublicationStage.FINAL_RELEASE,
                                OutputPublicationStage.PROVISIONAL_RELEASE):
            raise OutputPublicationJournalStateError("child release requires an exact hold stage")
        transfer = manifest.slots[effect.slot_index].transfers[effect.transfer_index]
        final = effect.stage is OutputPublicationStage.FINAL_RELEASE
        hold = transfer.final_hold if final else transfer.provisional_hold
        request = protocol.ReleaseContainedReference(
            transfer.contained_object_id, transfer.contained_owner_worker_id, hold,
        )
        reply = self._release_child(transfer.contained_owner_address, request)
        # Callback input may have been mutated in-process. Read authority again
        # before accepting a release or an independently queried death record.
        transfer = self._manifest(publication_id).slots[effect.slot_index].transfers[effect.transfer_index]
        expected_hold = transfer.final_hold if final else transfer.provisional_hold
        if type(reply) is protocol.GetWorkerStateReply:
            return self._require_child_owner_death(reply, transfer.contained_owner_worker_id)
        _require_type(reply, protocol.ReleaseContainedReferenceReply, "child release reply")
        reply = replace(reply)
        if (reply.object_id != transfer.contained_object_id
                or reply.owner_worker_id != transfer.contained_owner_worker_id
                or reply.hold != expected_hold):
            raise OutputPublicationConflictError("child release ACK changed effect identity")
        if not reply.accepted:
            raise OutputPublicationRemoteError(reply.error or "child release rejected")
        return reply

    @staticmethod
    def _require_child_owner_death(reply, owner_worker_id):
        """Revalidate GCS's exact registration/death evidence at consumption.

        The Node callback obtains this independently after release failure.
        Reachability, an EXPECTED exit, or a shallow/mutated dataclass is not
        authority to remove any reference obligation.
        """
        from .death_proofs import owner_death as _owner_death

        _require_type(reply, protocol.GetWorkerStateReply, "child owner state")
        _require_type(reply.incarnation, protocol.WorkerIncarnation, "child owner incarnation")
        registered = reply.incarnation
        for name in ("node_pid", "node_registration_epoch", "worker_pid"):
            _uint(getattr(registered, name), name, positive=True)
        incarnation = protocol.WorkerIncarnation(
            _opaque(registered.node_id, NodeID, "child NodeID"),
            registered.node_pid, registered.node_registration_epoch,
            _opaque(registered.worker_id, WorkerID, "child WorkerID"), registered.worker_pid,
        )
        death = _owner_death(reply.death)
        validated = protocol.GetWorkerStateReply(
            _opaque(reply.worker_id, WorkerID, "child owner"), reply.found, reply.watermark,
            reply.state, incarnation, death, reply.error,
        )
        if (validated.worker_id != owner_worker_id or not validated.found
                or validated.state is not protocol.WorkerMembershipState.DEAD
                or death.reason not in (
                    protocol.WorkerDeathReason.PROCESS_EXIT, protocol.WorkerDeathReason.NODE_EXIT,
                )):
            raise OutputPublicationConflictError("child cleanup lacks exact non-expected owner death")
        return validated

    def _manifest(self, publication_id):
        return self.journal.snapshot(publication_id).manifest

    @staticmethod
    def _journal_ack(effect, *, replay=False):
        return OutputPublicationAck(
            effect, OutputPublicationAckDisposition.ALREADY_APPLIED if replay
            else OutputPublicationAckDisposition.APPLIED,
        )

    @staticmethod
    def _require_pin_reply(reply, request):
        _require_type(reply, protocol.StoredContainedPinReply, "child pin reply")
        reply = replace(reply)
        if reply.request != request:
            raise OutputPublicationConflictError("child pin ACK changed effect identity")
        if not reply.accepted:
            raise OutputPublicationRemoteError(reply.error or "child pin rejected")
        return reply

    @staticmethod
    def _require_drop_reply(reply, manifest, slot_index):
        _require_type(reply, protocol.DropObjectReplicaReply, "slot DROP reply")
        reply = replace(reply)
        slot = manifest.slots[slot_index]
        if (reply.object_id != slot.object_id
                or reply.producer_attempt_id != manifest.publication_id.attempt_id
                or reply.owner_worker_id != manifest.header.owner_worker_id
                or reply.node_id != manifest.header.node_incarnation.node_id
                or reply.checksum != slot.checksum):
            raise OutputPublicationConflictError("slot DROP ACK changed replica identity")
        if reply.status not in (protocol.DropObjectReplicaStatus.DROPPED, protocol.DropObjectReplicaStatus.ALREADY_DROPPED):
            raise OutputPublicationRemoteError(reply.error or "slot DROP rejected")


__all__ = [
    "OutputPublicationNodeAdapter", "OutputPublicationBusy",
    "OutputPublicationRemoteError",
]
