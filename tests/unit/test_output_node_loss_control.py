"""Small pure end-to-end metadata Node-loss resolution for output batches.

Graph/death ordering is modeled with synchronous, bounded callbacks. Genuine
two-thread commit/death atomicity remains a separate reviewed L1 test gap; these
contracts do not start threads, sockets, producers or background progress.
"""

from dataclasses import replace
import pickle

import pytest

from miniray import output_protocol as wire, protocol
from miniray.contained_cycle import (
    ContainedGraphManifestDisposition, ContainedGraphTransactionState,
)
from miniray.errors import ProtocolError
from miniray.output_publication import OutputPublicationCompleteWitness, OutputPublicationEnvelope
from miniray.output_publication_journal import OutputPublicationSlotCleanupProof
from miniray.output_recovery import (
    OutputRecoveryAction, OutputRecoveryOwnerDecision as Decision,
    OutputRecoveryOwnerDecisionRecord, OutputSlotDecision,
)
from tests.unit.test_output_publication_control import _service, _no_runtime
from tests.unit.test_output_publication import _assert_metadata
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


@pytest.mark.parametrize("phase,keep", (("intent", ()), ("armed", ()), ("complete", ()), ("complete", (0,))))
def test_frozen_output_work_cleans_exact_child_vector_and_preserves_kept_graph(monkeypatch, phase, keep):
    service, values = _service(monkeypatch)
    releases = []
    def release(_address, handler, request):
        assert handler == "release_contained_reference"
        releases.append(request)
        return protocol.ReleaseContainedReferenceReply(request.object_id, request.owner_worker_id, request.hold, True, True)
    service._stored_hold_rpc = release
    report = service.report_output_publication
    report(wire.ReportOutputPublicationIntent(values.manifest))
    graph = values.manifest.to_graph_manifest()
    service.prepare_contained_graph(protocol.PrepareContainedGraph(graph))
    if phase != "intent":
        report(wire.ArmOutputPublication(values.publication_id, values.manifest.manifest_digest))
    if phase == "complete":
        report(wire.ReportOutputPublicationTerminal(values.witness))
    node = values.header.node_incarnation
    death = service.publications.commit_node_death(lambda: service.nodes.report_death(protocol.ReportNodeDeath(
        "batch-publisher-lost", node.node_id, node.node_pid, node.registration_epoch, 1,
        protocol.NodeDeathReason.PROCESS_EXIT, "observed exit",
    ))).death
    query = wire.GetOutputNodeLoss(values.publication_id, values.owner, death)
    observed = service.get_output_node_loss(query)
    work = observed.work
    assert observed.found
    _assert_metadata(observed)
    assert pickle.loads(pickle.dumps(observed)) == observed
    if phase != "intent":
        decision = OutputRecoveryOwnerDecisionRecord(
            values.publication_id, values.manifest.manifest_digest, values.owner, "owner-custody",
            tuple(OutputSlotDecision(i, slot.object_id, Decision.KEEP if i in keep else Decision.DROP) for i, slot in enumerate(values.slots)),
            values.witness if phase == "complete" else None,
        )
        assert service.decide_output_node_loss(wire.DecideOutputNodeLoss(work, decision)).snapshot.owner_decision == decision
    request = wire.ProgressOutputNodeLoss(work)
    for _ in range(12):
        result = service.progress_output_node_loss(request)
        if result.snapshot.resolution is not None:
            break
    else:
        pytest.fail("bounded Node-loss cleanup did not finish")
    assert result.snapshot.resolution.kept_slots == keep
    assert len(releases) == (8 if not keep else 6)
    assert service.progress_output_node_loss(request).snapshot == result.snapshot
    assert service.publications.output_recovery.frozen_workset(death) == (work,)
    _assert_metadata(result)
    if keep:
        remaining = service.publications.graph.snapshot().committed_edges
        assert set(remaining) == set(values.slots[0].edges)
        release_request = protocol.ReleaseContainedGraphContainer(graph, values.slots[0].object_id)
        assert service.release_contained_graph_container(release_request).accepted
        proof = OutputPublicationSlotCleanupProof(values.witness, values.owner, 0, values.slots[0].object_id, "later-gc")
        assert report(wire.ReportOutputPublicationSlotCollected(proof)).accepted
    assert not service.publications.has_active_operations()


@pytest.mark.parametrize("case", ("committed-all-drop", "armed-unknown-keep-inline"))
def test_typed_node_loss_routes_preserve_committed_graph_and_owner_witness_choice(monkeypatch, case):
    """Two slots/four transfers, at most twelve explicit progress calls."""
    service, values = _service(monkeypatch)
    adapter, registry = service.publications, service.publications.output_recovery
    graph = values.manifest.to_graph_manifest()
    all_drop = case == "committed-all-drop"
    kept = () if all_drop else (0,)
    released, graph_events = [], []
    assert len(values.slots) == 2
    assert sum(len(slot.transfers) for slot in values.slots) == 4

    def child_release(address, handler, request):
        assert handler == "release_contained_reference"
        assert address in tuple(transfer.contained_owner_address
                                for slot in values.slots for transfer in slot.transfers)
        assert not adapter._composition_lock._is_owned()
        _assert_metadata(request)
        released.append(request)
        return protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, True,
        )

    service._stored_hold_rpc = child_release
    assert service.handle(wire.ReportOutputPublicationIntent(values.manifest)).accepted
    assert service.handle(protocol.PrepareContainedGraph(graph)).accepted
    assert service.handle(wire.ArmOutputPublication(values.publication_id, values.manifest.manifest_digest)).accepted
    if all_drop:
        assert service.handle(wire.ReportOutputPublicationTerminal(values.witness)).accepted
        assert service.handle(protocol.CommitContainedGraph(graph)).accepted
    before_graph = adapter.graph.snapshot().manifests[0]
    assert before_graph.state is (ContainedGraphTransactionState.COMMITTED if all_drop
                                  else ContainedGraphTransactionState.PREPARED)
    node = values.header.node_incarnation
    death_reply = adapter.commit_node_death(lambda: service.nodes.report_death(protocol.ReportNodeDeath(
        "typed-graph-publisher-exit", node.node_id, node.node_pid, node.registration_epoch,
        1, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed publisher exit",
    )))
    assert death_reply.death is not None
    query = wire.GetOutputNodeLoss(values.publication_id, values.owner, death_reply.death)
    observed = service.handle(query)
    assert type(observed) is wire.GetOutputNodeLossReply and observed.request == query and observed.found
    work = observed.work
    assert work.action is (OutputRecoveryAction.POSTCOMPLETE_RESOLVE if all_drop
                            else OutputRecoveryAction.COMPLETION_UNKNOWN)
    assert work.snapshot.complete == (values.witness if all_drop else None)
    _assert_metadata(observed)

    # Model the owner's retained data-plane result explicitly. GCS receives
    # only its Complete witness, never the INLINE bytes in this envelope.
    owner_result = OutputPublicationEnvelope(values.manifest, values.witness, values.results)
    assert owner_result.results[0].inline_data == values.payloads[0]
    assert owner_result.results[1].inline_data is None
    decision = OutputRecoveryOwnerDecisionRecord(
        values.publication_id, values.manifest.manifest_digest, values.owner, "typed-owner-custody",
        tuple(OutputSlotDecision(index, slot.object_id, Decision.KEEP if index in kept else Decision.DROP)
              for index, slot in enumerate(values.slots)),
        owner_result.complete,
    )
    decision_request = wire.DecideOutputNodeLoss(work, decision)
    decided = service.handle(decision_request)
    assert type(decided) is wire.OutputNodeLossReply and decided.request == decision_request
    assert decided.snapshot.owner_decision == decision and decided.progressed
    repeated_decision = service.handle(decision_request)
    assert repeated_decision.snapshot == decided.snapshot and not repeated_decision.progressed
    assert decided.snapshot.complete == (values.witness if all_drop else None)
    _assert_metadata(decided)

    original_commit = adapter.graph.commit_manifest
    original_release = adapter.graph.release_manifest_container

    def commit_graph(manifest):
        assert adapter._composition_lock._is_owned()
        assert manifest == graph
        graph_events.append(("commit", manifest))
        return original_commit(manifest)

    def release_graph(manifest, container):
        assert adapter._composition_lock._is_owned()
        assert manifest == graph
        graph_events.append(("release", container))
        return original_release(manifest, container)

    def no_abort(*_args, **_kwargs):
        pytest.fail("committed or owner-KEPT graph was incorrectly ABORTed")

    monkeypatch.setattr(adapter.graph, "commit_manifest", commit_graph)
    monkeypatch.setattr(adapter.graph, "release_manifest_container", release_graph)
    monkeypatch.setattr(adapter.graph, "abort_manifest", no_abort)
    progress = wire.ProgressOutputNodeLoss(work)
    for _ in range(12):
        before_releases = len(released)
        result = service.handle(progress)
        assert type(result) is wire.OutputNodeLossReply and result.request == progress
        assert len(released) - before_releases <= 1
        assert adapter._output_loss_tickets == set()
        if result.snapshot.resolution is not None:
            break
    else:
        pytest.fail("fixed graph/child work did not converge within its bound")

    expected_releases = tuple(
        protocol.ReleaseContainedReference(transfer.contained_object_id, transfer.contained_owner_worker_id, hold)
        for index, slot in enumerate(values.slots)
        for transfer in slot.transfers
        for hold in ((transfer.final_hold, transfer.provisional_hold) if index not in kept
                     else (transfer.provisional_hold,))
    )
    assert tuple(released) == expected_releases
    assert len(released) == (8 if all_drop else 6)
    assert len(set(released)) == len(released)
    assert graph_events == ([("release", slot.object_id) for slot in values.slots] if all_drop
                            else [("commit", graph), ("release", values.slots[1].object_id)])
    resolution = result.snapshot.resolution
    assert resolution.kept_slots == kept and resolution.complete == values.witness
    assert result.snapshot.complete == (values.witness if all_drop else None)
    assert registry.frozen_workset(death_reply.death) == (work,)
    assert work.snapshot.complete == (values.witness if all_drop else None)
    _assert_metadata(result)
    state = adapter.graph.snapshot()
    assert state.manifests[0].state is ContainedGraphTransactionState.COMMITTED
    assert not state.prepared_edges
    assert set(state.committed_edges) == set(() if all_drop else values.slots[0].edges)

    # Typed progress/query replays return the terminal metadata without child
    # releases or a second graph choice. Late forward PREPARE remains fenced.
    effects = tuple(released), tuple(graph_events)
    assert service.handle(progress).snapshot == result.snapshot
    assert service.handle(query).snapshot == result.snapshot
    assert (tuple(released), tuple(graph_events)) == effects
    prepared = service.handle(protocol.PrepareContainedGraph(graph))
    assert not prepared.accepted and adapter.graph.snapshot() == state

    # Exact generic per-container RELEASE is allowed after resolution, even
    # though the publisher is dead. A DROP slot replays its existing receipt;
    # a KEEP slot retains its edges until this owner's later collection.
    dropped = protocol.ReleaseContainedGraphContainer(graph, values.slots[1].object_id)
    replayed_release = service.handle(dropped)
    assert replayed_release.accepted and replayed_release.request == dropped
    assert replayed_release.receipt.disposition is ContainedGraphManifestDisposition.ALREADY_RELEASED
    assert replayed_release.receipt.released_edges == values.slots[1].edges
    if kept:
        release_kept = protocol.ReleaseContainedGraphContainer(graph, values.slots[0].object_id)
        first = service.handle(release_kept)
        repeated = service.handle(release_kept)
        assert first.accepted and repeated.accepted
        assert first.receipt.disposition is ContainedGraphManifestDisposition.RELEASED
        assert repeated.receipt.disposition is ContainedGraphManifestDisposition.ALREADY_RELEASED
        assert first.receipt.released_edges == repeated.receipt.released_edges == values.slots[0].edges
        proof = OutputPublicationSlotCleanupProof(values.witness, values.owner, 0,
                                                   values.slots[0].object_id, "typed-kept-slot-GC")
        assert service.handle(wire.ReportOutputPublicationSlotCollected(proof)).accepted
    assert tuple(released) == expected_releases
    assert not adapter.graph.snapshot().committed_edges
    assert not adapter.has_active_operations()
    before = adapter.graph.snapshot()
    assert not service.handle(protocol.PrepareContainedGraph(graph)).accepted
    assert adapter.graph.snapshot() == before


@pytest.mark.parametrize("known,keep", ((False, ()), (True, ()), (True, (0,))))
def test_owner_applies_resolved_loss_without_fabricating_payload_or_losing_incoming_refs(monkeypatch, known, keep):
    service, values = _service(monkeypatch)
    registry = service.publications.output_recovery
    registry.report_intent(values.manifest)
    registry.arm_complete(values.publication_id, values.manifest.manifest_digest)
    if known:
        registry.report_terminal(values.witness)
    node = values.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "owner-loss", node.node_id, node.node_pid, node.registration_epoch, 1, 1,
        protocol.NodeDeathReason.PROCESS_EXIT, "publisher exit",
    )
    work = registry.freeze_node_death(death)[0]
    decision = OutputRecoveryOwnerDecisionRecord(
        values.publication_id, values.manifest.manifest_digest, values.owner, "custody",
        tuple(OutputSlotDecision(i, slot.object_id, Decision.KEEP if i in keep else Decision.DROP) for i, slot in enumerate(values.slots)),
        values.witness if known else None,
    )
    registry.decide_owner(work, values.owner, decision.slots, decision_id=decision.decision_id, complete=decision.complete)
    from miniray.output_recovery import OutputRecoveryResolution
    resolution = OutputRecoveryResolution(values.publication_id, values.manifest.manifest_digest, death, values.owner, "resolved", keep, decision.complete)
    owner = ObjectOwnerTable()
    spec = protocol.TaskSpec(values.job, values.task, values.attempt,
                            protocol.FunctionKey(values.job, __name__, "producer", "v1"),
                            (), 2, ResourceVector(), values.owner)
    owner.register_task_outputs(spec, local_tokens=("first", "second"))
    envelope = None
    if keep:
        envelope = OutputPublicationEnvelope(values.manifest, values.witness, values.results)
    assert owner.resolve_output_node_loss(values.manifest, resolution, envelope)
    assert not owner.resolve_output_node_loss(values.manifest, resolution, envelope)
    for i, slot in enumerate(values.slots):
        snapshot = owner.snapshot(slot.object_id)
        assert snapshot.local_tokens == frozenset(("first" if i == 0 else "second",))
        assert snapshot.producer_task_spec == spec and snapshot.current_attempt == values.attempt
        if i in keep:
            assert snapshot.state is ObjectState.READY_INLINE and snapshot.inline_data == values.payloads[i]
        else:
            assert snapshot.state is (ObjectState.LOST if known else ObjectState.PENDING)
            assert snapshot.inline_data is None and snapshot.canonical_stored_result is None


def test_known_complete_cannot_be_downgraded_by_all_drop_decision(monkeypatch):
    service, values = _service(monkeypatch)
    registry = service.publications.output_recovery
    registry.report_intent(values.manifest)
    registry.arm_complete(values.publication_id, values.manifest.manifest_digest)
    registry.report_terminal(values.witness)
    node = values.header.node_incarnation
    death = protocol.NodeDeathRecord(
        "known-success", node.node_id, node.node_pid, node.registration_epoch, 1, 1,
        protocol.NodeDeathReason.PROCESS_EXIT, "confirmed",
    )
    work = registry.freeze_node_death(death)[0]
    vector = tuple(OutputSlotDecision(i, slot.object_id, Decision.DROP) for i, slot in enumerate(values.slots))
    with pytest.raises(ValueError, match="known Complete"):
        registry.decide_owner(work, values.owner, vector, decision_id="bad-drop")
    assert registry.snapshot(values.publication_id).owner_decision is None


def test_owner_death_cleanup_is_driven_after_owner_wide_fences(monkeypatch):
    service, values = _service(monkeypatch)
    registry = service.publications.output_recovery
    registry.report_intent(values.manifest)
    registry.arm_complete(values.publication_id, values.manifest.manifest_digest)
    registry.report_terminal(values.witness)
    graph = values.manifest.to_graph_manifest()
    service.publications.graph.prepare_manifest(graph)
    service.publications.graph.commit_manifest(graph)
    node = values.header.node_incarnation
    death = protocol.WorkerDeathRecord(
        "output-owner-exit", protocol.WorkerIncarnation(node.node_id, node.node_pid, node.registration_epoch, values.owner, 1999),
        5, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    registry.freeze_owner_death(death)
    fences = service._owner_fence_registry()
    effects = fences.commit_owner_death(death)
    assert effects
    for effect in effects:
        fences.acknowledge(effect, protocol.InstallOwnerDeathFenceReply(
            effect.request, protocol.OwnerDeathFenceDisposition.FENCED, (),
        ))
    calls = []
    def rpc(address, handler, request):
        calls.append((handler, request))
        if handler == "release_contained_reference":
            return protocol.ReleaseContainedReferenceReply(request.object_id, request.owner_worker_id, request.hold, True, True)
        assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER
        return wire.FinalizeOutputOwnerDeathReply(request, True)
    service._stored_hold_rpc = rpc
    for _ in range(12):
        service._drive_output_owner_deaths()
        if registry.snapshot(values.publication_id).owner_cleaned is not None:
            break
    else:
        pytest.fail("owner-death cleanup did not finish its bounded work")
    assert registry.snapshot(values.publication_id).owner_cleaned == death
    assert not service.publications.graph.snapshot().committed_edges
    before = tuple(calls)
    service._drive_output_owner_deaths()
    assert tuple(calls) == before
    assert not service.publications.has_active_operations()


def _cleanup_fixture(monkeypatch, *, owner_died):
    """Two slots/four child transfers, no server, thread, socket or wait."""
    service, values = _service(monkeypatch)
    adapter = service.publications
    registry = adapter.output_recovery
    registry.report_intent(values.manifest)
    graph = values.manifest.to_graph_manifest()
    adapter.graph.prepare_manifest(graph)
    if owner_died:
        node = values.header.node_incarnation
        death = protocol.WorkerDeathRecord(
            "cleanup-owner-exit",
            protocol.WorkerIncarnation(node.node_id, node.node_pid, node.registration_epoch, values.owner, 1999),
            5, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
        )
        registry.freeze_owner_death(death)
        fences = service._owner_fence_registry()
        for effect in fences.commit_owner_death(death):
            fences.acknowledge(effect, protocol.InstallOwnerDeathFenceReply(
                effect.request, protocol.OwnerDeathFenceDisposition.FENCED, (),
            ))
        drive = service._drive_output_owner_deaths
        cleanup = adapter._output_owner_cleanup
    else:
        node = values.header.node_incarnation
        death = adapter.commit_node_death(lambda: service.nodes.report_death(protocol.ReportNodeDeath(
            "cleanup-publisher-exit", node.node_id, node.node_pid, node.registration_epoch, 1,
            protocol.NodeDeathReason.PROCESS_EXIT, "confirmed exit",
        ))).death
        (work,) = registry.frozen_workset(death)
        drive = lambda: service.progress_output_node_loss(wire.ProgressOutputNodeLoss(work))
        cleanup = adapter._output_loss_cleanup
    return service, values, drive, cleanup


@pytest.mark.parametrize("owner_died", (False, True))
@pytest.mark.parametrize("bad_reply", ("subclass", "accepted", "released", "hold", "rejected"))
def test_cleanup_revalidates_child_ack_before_retiring_an_obligation(monkeypatch, owner_died, bad_reply):
    service, values, drive, cleanup = _cleanup_fixture(monkeypatch, owner_died=owner_died)
    requests = []
    corrupt = [True]

    class ReplySubclass(protocol.ReleaseContainedReferenceReply):
        pass

    def rpc(_address, handler, request):
        if handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER:
            return wire.FinalizeOutputOwnerDeathReply(request, True)
        assert handler == "release_contained_reference"
        requests.append(request)
        if corrupt[0] and bad_reply == "subclass":
            return ReplySubclass(request.object_id, request.owner_worker_id, request.hold, True, True)
        reply = protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, True,
        )
        if corrupt[0]:
            if bad_reply in ("accepted", "released"):
                object.__setattr__(reply, bad_reply, 1)
            elif bad_reply == "hold":
                object.__setattr__(reply, "hold", replace(request.hold, transfer_token="other-effect"))
            else:
                return protocol.ReleaseContainedReferenceReply(
                    request.object_id, request.owner_worker_id, request.hold, False, False, "not released",
                )
        return reply

    service._stored_hold_rpc = rpc
    if owner_died:
        assert not drive()
    else:
        with pytest.raises((ValueError, TypeError, ProtocolError)):
            drive()
    assert len(requests) == 1
    assert cleanup[values.publication_id]["released"] == set()
    assert not cleanup[values.publication_id]["graph"]
    assert service.publications._output_loss_tickets == set()
    assert service.publications.graph.snapshot().prepared_edges

    corrupt[0] = False
    for _ in range(10):
        drive()
        snapshot = service.publications.output_recovery.snapshot(values.publication_id)
        if snapshot.owner_cleaned is not None or snapshot.resolution is not None:
            break
    else:
        pytest.fail("valid replay did not finish the fixed cleanup set")
    assert requests[0] == requests[1]
    assert len(cleanup[values.publication_id]["released"]) == 8
    assert not service.publications.graph.snapshot().prepared_edges


def test_owner_finalization_revalidates_cleaned_flag_and_replays_without_child_releases(monkeypatch):
    service, values, drive, cleanup = _cleanup_fixture(monkeypatch, owner_died=True)
    releases, finalizations = [], []
    corrupt = [True]

    def rpc(_address, handler, request):
        if handler == "release_contained_reference":
            releases.append(request)
            return protocol.ReleaseContainedReferenceReply(
                request.object_id, request.owner_worker_id, request.hold, True, True,
            )
        assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER
        finalizations.append(request)
        reply = wire.FinalizeOutputOwnerDeathReply(request, True)
        if corrupt[0]:
            object.__setattr__(reply, "cleaned", 1)
        return reply

    service._stored_hold_rpc = rpc
    for _ in range(8):
        drive()
    assert len(releases) == 8 and len(finalizations) == 1
    assert cleanup[values.publication_id]["graph"]
    assert service.publications.output_recovery.snapshot(values.publication_id).owner_cleaned is None
    corrupt[0] = False
    assert drive()
    assert len(releases) == 8 and finalizations[0] == finalizations[1]
    assert service.publications.output_recovery.snapshot(values.publication_id).owner_cleaned is not None


def test_collected_child_accepts_exact_compensation_without_reviving_its_identity(monkeypatch):
    from miniray.ownership import OwnershipError, UnknownObjectError
    service, values = _service(monkeypatch)
    owner = ObjectOwnerTable()
    transfer = values.slots[0].transfers[0]
    child = transfer.contained_object_id
    from miniray.ids import AttemptID
    attempt = AttemptID(child.task_id, 0)
    owner.register(child, current_attempt=attempt, local_token="ephemeral-source")
    owner.publish_inline(child, attempt, b"tiny")
    owner.release_local_reference(child, "ephemeral-source")
    plan = owner.begin_collection(child, collection_id="collected-ephemeral-child")
    assert owner.complete_collection(plan).collected
    for hold in (transfer.final_hold, transfer.provisional_hold):
        assert not owner.contained_release_was_seen(child, hold)
        assert not owner.release_contained_reference(child, hold)
        assert owner.contained_release_was_seen(child, hold)
        assert not owner.release_contained_reference(child, hold)
    assert not owner.contains(child)
    with pytest.raises(OwnershipError):
        owner.register(child)
    with pytest.raises(UnknownObjectError):
        owner.release_contained_reference(values.borrowed_child, transfer.final_hold)
