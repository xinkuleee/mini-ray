"""Pure contracts for the GCS/publication composition boundary.

Each case has one unstarted GCS, at most two Node/Worker metadata pairs, one
two-slot publication and one real child owner table. Synchronous callback
reentry models competing progress and owner death; it is not a claim about
two-thread timing. No process, socket, thread, wait or user code may run.
Cleanup uses at most eight explicit rounds and preserves real ACK tombstones.
"""

from dataclasses import replace
import multiprocessing.process
import subprocess

import pytest

from miniray import control, output_protocol as wire, protocol
from miniray.ids import NodeID
from miniray.output_recovery import (
    OutputRecoveryOwnerDecision as Decision, OutputRecoveryOwnerDecisionRecord,
    OutputRecoveryStateError, OutputSlotDecision,
)
from miniray.ownership import ObjectOwnerTable
from miniray.resources import ResourceVector
from tests.unit.test_output_publication import _assert_metadata
from tests.unit.test_output_publication_control import _no_runtime, _service


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_process_or_rpc(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("publication boundary contract attempted runtime work")

    monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(control, "rpc_request", forbidden)


def _register_surviving_owner(service, values):
    node_id = NodeID(bytes((55,)) * 16)
    node = service.register_node(protocol.RegisterNode(
        node_id=node_id, node_pid=2501, address=("127.0.0.1", 33001),
        total_resources=ResourceVector({"CPU": 1}),
    ))
    assert node.accepted
    owner = protocol.WorkerIncarnation(
        node_id, 2501, node.registration_epoch, values.owner, 2502,
    )
    assert service.register_worker_incarnation(
        protocol.RegisterWorkerIncarnation(owner)
    ).accepted
    return owner


def _completed_node_loss(monkeypatch, *, surviving_owner=False):
    service, values = _service(monkeypatch)
    owner = _register_surviving_owner(service, values) if surviving_owner else None
    report = service.report_output_publication
    assert report(wire.ReportOutputPublicationIntent(values.manifest)).accepted
    graph = values.manifest.to_graph_manifest()
    assert service.prepare_contained_graph(protocol.PrepareContainedGraph(graph)).accepted
    assert report(wire.ArmOutputPublication(
        values.publication_id, values.manifest.manifest_digest,
    )).accepted
    assert report(wire.ReportOutputPublicationTerminal(values.witness)).accepted
    assert service.commit_contained_graph(protocol.CommitContainedGraph(graph)).accepted
    node = values.header.node_incarnation
    death = service.report_node_death(protocol.ReportNodeDeath(
        "boundary-publisher-exit", node.node_id, node.node_pid,
        node.registration_epoch, 1, protocol.NodeDeathReason.PROCESS_EXIT,
        "pure committed membership proof",
    )).death
    query = wire.GetOutputNodeLoss(values.publication_id, values.owner, death)
    observed = service.get_output_node_loss(query)
    assert observed.found
    decision = OutputRecoveryOwnerDecisionRecord(
        values.publication_id, values.manifest.manifest_digest, values.owner,
        "boundary-owner-drop", tuple(
            OutputSlotDecision(index, slot.object_id, Decision.DROP)
            for index, slot in enumerate(values.slots)
        ), values.witness,
    )
    assert service.decide_output_node_loss(
        wire.DecideOutputNodeLoss(observed.work, decision)
    ).snapshot.owner_decision == decision
    return service, values, observed.work, owner


def _live_child_peer(values):
    """Only the foreign child survives the publisher/executor death."""
    table = ObjectOwnerTable()
    table.register(values.borrowed_child, local_token="child-stays-live")
    expected = []
    for slot in values.slots:
        transfer = slot.transfers[1]
        assert transfer.contained_owner_worker_id == values.foreign_owner
        for hold in (transfer.final_hold, transfer.provisional_hold):
            assert table.add_contained_reference(values.borrowed_child, hold)
            expected.append((transfer.contained_owner_address, protocol.ReleaseContainedReference(
                transfer.contained_object_id, transfer.contained_owner_worker_id, hold,
            )))
    return table, tuple(expected)


def test_service_uses_only_public_adapter_methods_and_current_callbacks(monkeypatch):
    service, values, work, owner = _completed_node_loss(monkeypatch, surviving_owner=True)
    death = service.report_worker_death(protocol.ReportWorkerDeath(
        "boundary-spy-owner-exit", owner, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
    )).death
    calls, replies = [], {name: object() for name in ("report", "get", "decide", "progress")}
    expected_callbacks = {}

    class PublicOnlyAdapter:
        # No graph, registry, composition lock or cleanup collections exist.
        __slots__ = ()

        def report_registered(self, request, *, node_lookup, worker_lookup, worker_state):
            assert service._owner_fence_lock()._is_owned()
            assert (node_lookup, worker_lookup, worker_state) == expected_callbacks["membership"]
            calls.append(("report", request))
            return replies["report"]

        def get_node_loss(self, request, *, node_lookup):
            assert node_lookup is expected_callbacks["membership"][0]
            calls.append(("get", request))
            return replies["get"]

        def decide_node_loss(self, request):
            calls.append(("decide", request))
            return replies["decide"]

        def progress_node_loss(self, request, *, worker_state, effect_rpc):
            assert not service._owner_fence_lock()._is_owned()
            assert worker_state is expected_callbacks["membership"][2]
            assert effect_rpc is expected_callbacks["rpc"]
            calls.append(("progress", request))
            return replies["progress"]

        def progress_owner_death(self, observed, *, node_lookup, worker_state, effect_rpc):
            assert observed == death
            assert not service._owner_fence_lock()._is_owned()
            assert not service._owner_fence_registry().pending_for_owner(values.owner)
            assert node_lookup is expected_callbacks["membership"][0]
            assert worker_state is expected_callbacks["membership"][2]
            assert effect_rpc is expected_callbacks["rpc"]
            calls.append(("owner", observed))
            return True

        def active_owner_deaths(self, owner_worker_id=None):
            calls.append(("active", owner_worker_id))
            return 2 if owner_worker_id is None else 1

    monkeypatch.setattr(service, "publications", PublicOnlyAdapter())
    intent = wire.ReportOutputPublicationIntent(values.manifest)
    query = wire.GetOutputNodeLoss(values.publication_id, values.owner, work.death)
    decision = OutputRecoveryOwnerDecisionRecord(
        values.publication_id, values.manifest.manifest_digest, values.owner,
        "boundary-spy-drop", tuple(
            OutputSlotDecision(index, slot.object_id, Decision.DROP)
            for index, slot in enumerate(values.slots)
        ), values.witness,
    )
    decide = wire.DecideOutputNodeLoss(work, decision)
    progress = wire.ProgressOutputNodeLoss(work)
    for index in range(2):
        def node_lookup(_node_id):
            pytest.fail("service used a membership observer outside its adapter")

        def worker_lookup(_worker_id):
            pytest.fail("service used a membership observer outside its adapter")

        def worker_state(_request):
            pytest.fail("service used a membership observer outside its adapter")

        def rpc(*_args):
            pytest.fail("delegation-only service attempted an effect")

        expected_callbacks.update(membership=(node_lookup, worker_lookup, worker_state), rpc=rpc)
        monkeypatch.setattr(service.nodes, "get", node_lookup)
        monkeypatch.setattr(service.workers, "get", worker_lookup)
        monkeypatch.setattr(service.workers, "get_state_reply", worker_state)
        monkeypatch.setattr(service, "_stored_hold_rpc", rpc)
        assert service.report_output_publication(intent) is replies["report"]
        assert service.get_output_node_loss(query) is replies["get"]
        assert service.decide_output_node_loss(decide) is replies["decide"]
        assert service.progress_output_node_loss(progress) is replies["progress"]
        if index == 0:
            assert not service._drive_output_owner_deaths(values.owner)
            fences = service._owner_fence_registry()
            pending = fences.pending_for_owner(values.owner)
            assert len(pending) == 1
            # Prior owner-wide sweep receipt is explicit fixture state. This
            # test checks delegation after that independent readiness barrier.
            for effect in pending:
                fences.acknowledge(effect, protocol.InstallOwnerDeathFenceReply(
                    effect.request, protocol.OwnerDeathFenceDisposition.FENCED, (),
                ))
        assert service._drive_output_owner_deaths(values.owner)
        assert service._active_output_owner_deaths(values.owner) == 1
        assert service._active_output_owner_deaths() == 2
    assert calls == [item for _ in range(2) for item in (
        ("report", intent), ("get", query), ("decide", decide),
        ("progress", progress), ("owner", death),
        ("active", values.owner), ("active", None),
    )]


def test_reentrant_node_progress_cannot_repeat_the_inflight_child_effect(monkeypatch):
    service, values, work, _ = _completed_node_loss(monkeypatch)
    adapter = service.publications
    table, expected = _live_child_peer(values)
    request = wire.ProgressOutputNodeLoss(work)
    calls, nested = [], []

    def release(address, handler, child_request):
        assert not adapter._composition_lock._is_owned()
        assert not service._owner_fence_lock()._is_owned()
        assert handler == "release_contained_reference"
        assert (address, child_request) == expected[len(calls)]
        calls.append((address, child_request))
        assert len(calls) <= 4
        changed = table.release_contained_reference(child_request.object_id, child_request.hold)
        reply = protocol.ReleaseContainedReferenceReply(
            child_request.object_id, child_request.owner_worker_id, child_request.hold, True, changed,
        )
        before = adapter.output_recovery.snapshot(values.publication_id)
        duplicate = service.progress_output_node_loss(request)
        assert not duplicate.progressed and duplicate.snapshot == before
        nested.append(duplicate)
        return reply

    monkeypatch.setattr(service, "_stored_hold_rpc", release)
    for _ in range(8):
        count = len(calls)
        result = service.progress_output_node_loss(request)
        assert len(calls) - count <= 1
        if result.snapshot.resolution is not None:
            break
    else:
        pytest.fail("fixed publication did not finish in eight rounds")
    assert tuple(calls) == expected and len(nested) == 4
    assert all(reply.snapshot.resolution is None for reply in nested)
    assert not table.snapshot(values.borrowed_child).contained_holds
    assert not adapter.graph.snapshot().committed_edges
    assert not adapter.has_active_operations()
    assert service.progress_output_node_loss(request).snapshot == result.snapshot
    assert tuple(calls) == expected
    _assert_metadata(result)


def test_owner_death_during_last_child_ack_fences_old_driver_and_releases_its_ticket(monkeypatch):
    service, values, work, owner = _completed_node_loss(monkeypatch, surviving_owner=True)
    adapter = service.publications
    table, expected = _live_child_peer(values)
    progress = wire.ProgressOutputNodeLoss(work)
    graph_before = adapter.graph.snapshot()
    calls, deaths, competing, fences_sent, phases = [], [], [], [], []
    death_request = protocol.ReportWorkerDeath(
        "boundary-reentrant-owner-exit", owner, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
    )

    def rpc(address, handler, request):
        assert not adapter._composition_lock._is_owned()
        assert not service._owner_fence_lock()._is_owned()
        if handler == control.INSTALL_OWNER_DEATH_FENCE_HANDLER:
            assert request.owner_worker_id == values.owner
            assert address == service.nodes.get(owner.node_id).address
            fences_sent.append(request)
            assert len(fences_sent) == 1
            return protocol.InstallOwnerDeathFenceReply(
                request, protocol.OwnerDeathFenceDisposition.FENCED, (),
            )
        assert handler == "release_contained_reference"
        assert (address, request) in expected
        calls.append((address, request))
        assert len(calls) <= 8
        changed = table.release_contained_reference(request.object_id, request.hold)
        reply = protocol.ReleaseContainedReferenceReply(
            request.object_id, request.owner_worker_id, request.hold, True, changed,
        )
        if len(calls) == len(expected) and not deaths:
            # This is the last missing live-child ACK. Without the post-RPC
            # authority check the old driver would now mutate graph/resolve.
            death = service.report_worker_death(death_request).death
            assert death is not None
            deaths.append(death)
            competing.append(service.progress_publication_owner_death(
                protocol.ProgressPublicationOwnerDeath(values.owner)
            ))
            assert not competing[-1].clean
            assert not service._owner_fence_registry().pending_for_owner(values.owner)
            assert len(calls) == len(expected)
            assert adapter.graph.snapshot() == graph_before
        return reply

    def watch(name, original):
        def called(*args, **kwargs):
            assert adapter._composition_lock._is_owned()
            phases.append(name)
            return original(*args, **kwargs)
        return called

    for name in ("commit_manifest", "release_manifest_container", "abort_manifest"):
        monkeypatch.setattr(adapter.graph, name, watch(name, getattr(adapter.graph, name)))
    for name in ("resolve_node_loss", "resolve_owner_death"):
        monkeypatch.setattr(adapter.output_recovery, name, watch(name, getattr(adapter.output_recovery, name)))
    monkeypatch.setattr(service, "_stored_hold_rpc", rpc)
    for _ in range(7):
        assert service.progress_output_node_loss(progress).snapshot.resolution is None
    assert tuple(calls) == expected[:-1]
    with pytest.raises(OutputRecoveryStateError, match="owner died during"):
        service.progress_output_node_loss(progress)
    assert phases == [] and adapter.graph.snapshot() == graph_before
    interrupted = adapter.output_recovery.snapshot(values.publication_id)
    assert interrupted.owner_death == deaths[0]
    assert interrupted.resolution is interrupted.owner_cleaned is None
    assert service._active_output_owner_deaths(values.owner) == 1

    # The original ticket must be released by finally. The owner driver can
    # now take over, replaying the exact releases against real tombstones.
    for _ in range(8):
        service._drive_output_owner_deaths(values.owner)
        terminal = adapter.output_recovery.snapshot(values.publication_id)
        if terminal.owner_cleaned is not None:
            break
    else:
        pytest.fail("owner takeover could not acquire or finish the cleanup ticket")
    assert tuple(calls) == expected + expected
    assert terminal.owner_cleaned == deaths[0] and terminal.resolution is None
    assert phases == ["release_manifest_container", "release_manifest_container", "resolve_owner_death"]
    assert not table.snapshot(values.borrowed_child).contained_holds
    assert not adapter.graph.snapshot().committed_edges
    assert not service._active_output_owner_deaths(values.owner)
    assert not service._drive_output_owner_deaths(values.owner)
    assert not adapter.has_active_operations()
    _assert_metadata(terminal)


def test_registered_admission_owns_membership_and_intent_lock_cut_for_driver_owner(monkeypatch):
    service, values = _service(monkeypatch, refs=False)
    adapter = service.publications
    original_node, original_worker = service.nodes.get, service.workers.get
    original_state = service.workers.get_state_reply
    original_intent = adapter.output_recovery.report_intent
    assert not original_state(protocol.GetWorkerState(values.owner)).found
    seen = []

    def cut(label):
        assert service._owner_fence_lock()._is_owned()
        assert adapter._composition_lock._is_owned()
        seen.append(label)

    def node_lookup(node_id):
        cut("node")
        assert node_id == values.node
        return original_node(node_id)

    def worker_lookup(worker_id):
        cut("executor")
        assert worker_id == values.executor
        return original_worker(worker_id)

    def worker_state(request):
        cut("owner")
        assert request.worker_id == values.owner
        reply = original_state(request)
        assert not reply.found
        return reply

    def intent(manifest):
        cut("intent")
        assert manifest == values.manifest
        return original_intent(manifest)

    monkeypatch.setattr(service.nodes, "get", node_lookup)
    monkeypatch.setattr(service.workers, "get", worker_lookup)
    monkeypatch.setattr(service.workers, "get_state_reply", worker_state)
    monkeypatch.setattr(adapter.output_recovery, "report_intent", intent)
    request = wire.ReportOutputPublicationIntent(values.manifest)
    first = service.report_output_publication(request)
    replay = service.report_output_publication(request)
    assert first.accepted and replay.accepted and first.request == replay.request == request
    assert first.ack.snapshot == replay.ack.snapshot
    assert seen == ["node", "executor", "owner", "intent"] * 2
    assert adapter.output_recovery.publication_ids() == (values.publication_id,)
    assert not service._owner_fence_lock()._is_owned()
    assert not adapter._composition_lock._is_owned()
    _assert_metadata(replay)
