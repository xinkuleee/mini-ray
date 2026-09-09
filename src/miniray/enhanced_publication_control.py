"""GCS composition for the two teaching publication guarantees.

Membership and the pure graph/fact reducer share GCS's existing lock. Remote
cleanup runs outside that lock on the existing owner-death progress driver.
This adapter records exact cleanup work, never bytes, READY or child refcounts.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace

from . import enhanced_publication as ep, output_protocol, protocol
from .death_proofs import owner_death as copy_owner_death


@dataclass
class _DeathCleanup:
    publication: ep.Publication
    death: protocol.WorkerDeathRecord
    releases: dict = field(default_factory=dict)
    child_deaths: dict = field(default_factory=dict)
    closed_holds: ep.ClosedContainedHolds | None = None
    node_fenced: bool = False
    node_finalized: bool = False
    finished: bool = False
    inflight: bool = False


class EnhancedPublicationControl:
    """Narrow membership adapter and replayable dead-owner cleanup outbox."""

    def __init__(self, *, nodes, workers, owner_fences, lock, rpc, authority=None):
        self.nodes, self.workers = nodes, workers
        self.owner_fences, self.lock, self.rpc = owner_fences, lock, rpc
        self.authority = authority if authority is not None else ep.PublicationAuthority()
        self._deaths = {}
        self._cursor = 0
        self._admission_closed = False

    def handle(self, request):
        if type(request) not in (ep.BeginPublication, ep.PrepareGraph, ep.ArmTask,
                ep.RecordTerminal, ep.CommitGraph, ep.RecordAdoption,
                ep.FencePublication, ep.RetireGraph, ep.GetPublication):
            raise TypeError('unsupported enhanced publication request')
        request = replace(request)
        with self.lock:
            query = self.authority.query(ep.GetPublication(ep.request_reference(request)))
            if not query.accepted:
                return ep.PublicationReply(request, False, error_kind=query.error_kind, error=query.error)
            snapshot = query.snapshot
            if (type(request) is ep.BeginPublication and self._admission_closed
                    and (snapshot is None or snapshot.receipt(ep.PublicationStage.INTENT) is None)):
                return ep.PublicationReply(
                    request, False, error_kind=ep.PublicationErrorKind.INVALID_STATE,
                    error='GCS shutdown closed new publication admission',
                )
            publication = (request.publication if type(request) in
                (ep.BeginPublication, ep.FencePublication) else
                None if snapshot is None else snapshot.publication)
            try:
                if type(request) is ep.FencePublication and type(request.proof) is protocol.WorkerDeathRecord:
                    death = self._validated_death(request.proof)
                    if death.worker_id != publication.owner_worker_id:
                        raise ValueError('publication death names another owner')
                    # A later membership fact schedules cleanup without rebinding
                    # the exact first owner GC/abort decision.
                    if (snapshot is not None and snapshot.publication == publication
                            and snapshot.fence is not None and snapshot.fence != death):
                        self.commit_owner_death(death)
                        return ep.PublicationReply(request, True, snapshot,
                            snapshot.receipt(ep.PublicationStage.FENCED))
                if type(request) is ep.RetireGraph:
                    self._validate_closed_deaths(request.closed_holds)
                stage = {ep.BeginPublication: ep.PublicationStage.INTENT,
                         ep.PrepareGraph: ep.PublicationStage.PREPARED,
                         ep.ArmTask: ep.PublicationStage.ARMED}.get(type(request))
                if stage is not None and (snapshot is None or snapshot.receipt(stage) is None):
                    if publication is not None:
                        self._require_owner_open(publication.owner_worker_id)
                        if type(publication) is ep.TaskPublication:
                            self._require_publisher_open(publication)
                if type(request) is ep.CommitGraph and (snapshot is None or snapshot.receipt(ep.PublicationStage.COMMITTED) is None):
                    if publication is not None:
                        self._require_owner_open(publication.owner_worker_id)
                    if request.put_prepared is not None:
                        incarnation = request.put_prepared.materialization.node_incarnation
                        if incarnation is not None:
                            self._require_node_open(incarnation)
            except Exception as exc:
                return ep.PublicationReply(request, False, error_kind=ep.PublicationErrorKind.UNAVAILABLE,
                                           error=str(exc) or type(exc).__name__)
            reply = self.authority.apply(request)
            if reply.accepted and reply.snapshot is not None and type(request) is not ep.GetPublication:
                death = self._owner_death(reply.snapshot.publication.owner_worker_id)
                if death is not None:
                    self.commit_owner_death(death)
            return reply

    def _require_node_open(self, incarnation):
        node = self.nodes.get(incarnation.node_id)
        if (node.state is not protocol.NodeMembershipState.ALIVE
                or (node.node_pid, node.registration_epoch) !=
                (incarnation.node_pid, incarnation.registration_epoch)):
            raise ValueError('publishing Node is not the exact live incarnation')
        return node

    def _require_publisher_open(self, publication):
        header = publication.manifest.header
        self._require_node_open(header.node_incarnation)
        worker = self.workers.get(header.executor_worker_id)
        incarnation = worker.incarnation
        node = header.node_incarnation
        if (worker.state is not protocol.WorkerMembershipState.ALIVE
                or (incarnation.node_id, incarnation.node_pid, incarnation.node_registration_epoch)
                != (node.node_id, node.node_pid, node.registration_epoch)):
            raise ValueError('publisher is not the exact registered live Worker')

    def _require_owner_open(self, owner):
        try:
            worker = self.workers.get(owner)
        except KeyError:
            # First Begin freezes the actual owner-handoff/local-put route. An
            # unmanaged endpoint is not a registered Driver or liveness proof.
            return
        if worker.state is not protocol.WorkerMembershipState.ALIVE:
            raise ValueError('publication owner is death-fenced')
        node = self.nodes.get(worker.incarnation.node_id)
        if (node.state is not protocol.NodeMembershipState.ALIVE
                or (node.node_pid, node.registration_epoch) !=
                (worker.incarnation.node_pid, worker.incarnation.node_registration_epoch)):
            raise ValueError('publication owner Node is not alive')

    def _validated_death(self, death):
        death = copy_owner_death(death)
        worker = self.workers.get(death.worker_id)
        if (worker.state is not protocol.WorkerMembershipState.DEAD
                or worker.incarnation != death.incarnation or worker.death != death
                or death.reason not in (protocol.WorkerDeathReason.PROCESS_EXIT,
                                        protocol.WorkerDeathReason.NODE_EXIT)):
            raise ValueError('cleanup lacks an exact committed owner death')
        return death

    def _owner_death(self, owner):
        try:
            worker = self.workers.get(owner)
        except KeyError:
            return None
        if worker.state is protocol.WorkerMembershipState.DEAD and worker.death is not None:
            if worker.death.reason in (protocol.WorkerDeathReason.PROCESS_EXIT, protocol.WorkerDeathReason.NODE_EXIT):
                return self._validated_death(worker.death)
        return None

    def _validate_closed_deaths(self, closed):
        for death in closed.child_deaths:
            self._validated_death(death)

    def commit_owner_death(self, death):
        """Idempotent local admission; callers hold the membership lock."""
        with self.lock:
            death = self._validated_death(death)
            for snapshot in self.authority.for_owner(death.worker_id):
                publication = snapshot.publication
                if snapshot.fence is None:
                    reply = self.authority.fence(ep.FencePublication(publication, death))
                    if not reply.accepted:
                        raise ValueError(reply.error or 'owner death fence was rejected')
                key = publication.reference
                work = self._deaths.get(key)
                if work is None:
                    self._deaths[key] = _DeathCleanup(
                        publication, death, closed_holds=snapshot.closed_holds,
                    )
                elif work.death != death or work.publication != publication:
                    raise ValueError('publication owner death was rebound')

    def pending_deaths(self):
        with self.lock:
            return tuple(key for key, work in self._deaths.items() if not work.finished)

    def close_admission(self):
        """Permanently reject new INTENT while preserving cleanup/history.

        Existing unretired records already prevent final shutdown. Their
        pending operations can converge while the process remains available;
        no new publication can slip behind the final clean observation.
        """
        with self.lock:
            self._admission_closed = True

    def has_active_operations(self):
        """Final shutdown only; live publications need normal owner GC first."""
        with self.lock:
            return (any(not work.finished for work in self._deaths.values())
                    or any(snapshot.receipt(ep.PublicationStage.RETIRED) is None
                           for snapshot in self.authority.snapshots()))

    def drive_one(self):
        with self.lock:
            pending = [(key, work) for key, work in self._deaths.items()
                       if not work.finished and not work.inflight]
            if not pending:
                return False
            key, work = pending[self._cursor % len(pending)]
            self._cursor += 1
            work.inflight = True
        try:
            return self._drive(work)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            # Neither timeout nor rejected/malformed reply removes a duty.
            return False
        finally:
            with self.lock:
                work.inflight = False

    def _node_death(self, publication):
        incarnation = publication.manifest.header.node_incarnation
        node = self.nodes.get(incarnation.node_id)
        if ((node.node_pid, node.registration_epoch) !=
                (incarnation.node_pid, incarnation.registration_epoch)):
            raise ValueError('publishing Node incarnation changed')
        if node.state is protocol.NodeMembershipState.DEAD:
            death = node.death
            if death is None or death.reason is not protocol.NodeDeathReason.PROCESS_EXIT:
                raise ValueError('expected Node exit cannot hide publication cleanup')
            return death
        return None

    def _drive(self, work):
        publication = work.publication
        if type(publication) is ep.TaskPublication and not work.node_finalized:
            with self.lock:
                node_death = self._node_death(publication)
                if node_death is not None:
                    work.node_finalized = True
            if node_death is None:
                node_id = publication.manifest.header.node_incarnation.node_id
                # A narrow fence closes late writes without pretending to have
                # completed the independent owner-wide physical sweep.
                fence = protocol.InstallOwnerDeathFence(
                    'publication-fence:' + publication.reference.digest + ':' + work.death.detection_id,
                    work.death, node_id, (), protocol.OwnerDeathFenceScope.PUBLICATION_EXACT)
                if not work.node_fenced:
                    reply = self.rpc(self.nodes.address(node_id), 'install_owner_death_fence', fence)
                    if type(reply) is not protocol.InstallOwnerDeathFenceReply or reply.request != fence or not reply.accepted:
                        return False
                    with self.lock:
                        work.node_fenced = True
                    return True
                request = output_protocol.FinalizeOutputOwnerDeath(publication.manifest, work.death)
                reply = self.rpc(self.nodes.address(node_id), output_protocol.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER, request)
                if type(reply) is not output_protocol.FinalizeOutputOwnerDeathReply:
                    return False
                reply = replace(reply)
                if reply.request != request or not reply.cleaned or reply.closed_holds is None:
                    return False
                with self.lock:
                    closed = replace(reply.closed_holds)
                    if closed.reference != publication.reference:
                        raise ValueError('Node cleanup changed publication')
                    self._validate_closed_deaths(closed)
                    work.closed_holds = closed
                    work.node_finalized = True
                return True
        if work.closed_holds is None:
            if self._release_one(work):
                return True
            with self.lock:
                needed = self._release_requests(publication)
                if any(request not in work.releases and request.owner_worker_id not in work.child_deaths
                       for _, request in needed):
                    return False
                work.closed_holds = ep.ClosedContainedHolds(publication.reference,
                    tuple(work.releases.values()), tuple(work.child_deaths.values()))
        with self.lock:
            snapshot = self.authority.query(ep.GetPublication(publication.reference)).snapshot
            if snapshot is None:
                raise ValueError('death cleanup lost publication history')
            if snapshot.receipt(ep.PublicationStage.RETIRED) is None:
                reply = self.authority.retire(ep.RetireGraph(work.closed_holds))
                if not reply.accepted:
                    return False
            # Full publication death cleanup waits for the independently owned
            # survivor byte sweep, even though graph edges may be retired first.
            if self.owner_fences.pending_for_owner(work.death.worker_id):
                return False
            work.finished = True
            return True

    @staticmethod
    def _release_requests(publication):
        # RPC callers receive detached nested IDs, never references into this
        # adapter's retained immutable manifest (including injected in-process
        # transports used by the finite contract checks).
        publication = replace(publication)
        return tuple((transfer.contained_owner_address, protocol.ReleaseContainedReference(
            transfer.contained_object_id, transfer.contained_owner_worker_id, hold))
            for transfer in publication.transfers
            for hold in (transfer.final_hold, transfer.provisional_hold))

    def _release_one(self, work):
        with self.lock:
            for address, request in self._release_requests(work.publication):
                if request in work.releases or request.owner_worker_id in work.child_deaths:
                    continue
                death = self._owner_death(request.owner_worker_id)
                if death is not None:
                    work.child_deaths[request.owner_worker_id] = death
                    return True
                break
            else:
                return False
        expected = next(candidate for _, candidate in self._release_requests(work.publication)
                        if candidate == request)
        reply = self.rpc(address, 'release_contained_reference', request)
        if type(reply) is not protocol.ReleaseContainedReferenceReply:
            return False
        reply = replace(reply)
        if (not reply.accepted or (reply.object_id, reply.owner_worker_id, reply.hold)
                != (expected.object_id, expected.owner_worker_id, expected.hold)):
            return False
        with self.lock:
            work.releases[expected] = reply
        return True
