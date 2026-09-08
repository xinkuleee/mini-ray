"""Pure wire/GCS contracts for the unified contained-object graph.

The generic graph authority remains shared by every result tier, while its
runtime facade requires the exact OutputPublicationID and recovery facts.
No legacy graph endpoint or unregistered naked publication ID is exercised.
"""

from __future__ import annotations

from dataclasses import replace
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time

import pytest

from miniray import control, protocol, output_protocol as wire
from miniray.contained_cycle import (
    ContainedGraphManifestDisposition, ContainedGraphTransactionState,
)
from miniray.output_publication import OutputPublicationID, OutputPublicationManifest
from miniray.output_publication_journal import OutputPublicationAdoptionProof
from tests.unit.test_output_publication import _Fixture, _assert_metadata
from tests.unit.test_output_publication_control import _service


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure contained-graph contract attempted runtime infrastructure")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait"),
                         (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _finish_publication(adapter, values, graph):
    registry = adapter.output_recovery
    registry.arm_complete(values.publication_id, values.manifest.manifest_digest)
    registry.report_terminal(values.witness)
    committed = adapter.mutate_graph(protocol.CommitContainedGraph(graph))
    assert committed.accepted and committed.receipt.state is ContainedGraphTransactionState.COMMITTED
    registry.report_adopted(OutputPublicationAdoptionProof(values.witness, values.owner, "graph-owner-CAS"))
    for slot in values.slots:
        request = protocol.ReleaseContainedGraphContainer(graph, slot.object_id)
        released = adapter.mutate_graph(request)
        assert released.accepted and released.request == request
        assert released.receipt.released_edges == slot.edges
    assert not adapter.has_active_operations()


@pytest.mark.parametrize("target", (False, True))
def test_generic_graph_wire_round_trip_preserves_exact_selected_publication(target):
    values = _Fixture(target=target)
    graph = values.manifest.to_graph_manifest()
    assert type(graph.publication_id) is OutputPublicationID
    assert graph.publication_id == values.publication_id
    assert tuple(dict.fromkeys(edge.container_object_id for edge in graph.ordered_edges)) == values.publication_id.output_ids
    assert values.publication_id.full_output_ids == values.full.output_ids
    messages = (
        protocol.PrepareContainedGraph(graph), protocol.CommitContainedGraph(graph),
        protocol.AbortContainedGraph(graph),
        protocol.ReleaseContainedGraphContainer(graph, values.slots[0].object_id),
        protocol.GetContainedGraph(graph.transaction_id),
    )
    for message in messages:
        restored = pickle.loads(pickle.dumps(message))
        assert type(restored) is type(message) and restored == message
        _assert_metadata(restored)
    if target:
        assert tuple(output.return_index for output in graph.publication_id.output_ids) == (1, 3)


def test_generic_graph_uses_one_authority_and_requires_output_intent_and_complete():
    values = _Fixture()
    graph = values.manifest.to_graph_manifest()
    adapter = control.PublicationControlAdapter()
    before = adapter.graph.snapshot()
    request = protocol.PrepareContainedGraph(graph)
    rejected = adapter.mutate_graph(request)
    assert not rejected.accepted and rejected.request == request
    assert adapter.graph.snapshot() == before
    assert adapter.output_recovery.publication_ids() == ()
    adapter.output_recovery.report_intent(values.manifest)
    prepared = adapter.mutate_graph(request)
    replay = adapter.mutate_graph(request)
    assert prepared.accepted and prepared.receipt.state is ContainedGraphTransactionState.PREPARED
    assert replay.receipt.disposition is ContainedGraphManifestDisposition.ALREADY_PREPARED
    found = adapter.get_graph(protocol.GetContainedGraph(graph.transaction_id))
    assert found.disposition is protocol.ContainedGraphQueryDisposition.FOUND and found.manifest == graph
    assert not adapter.mutate_graph(protocol.CommitContainedGraph(graph)).accepted
    _finish_publication(adapter, values, graph)
    assert adapter.get_graph(protocol.GetContainedGraph(graph.transaction_id)).manifest == graph


def test_generic_graph_rejects_naked_identity_without_mutation():
    values = _Fixture()
    graph = values.manifest.to_graph_manifest()
    adapter = control.PublicationControlAdapter()
    naked = replace(graph, publication_id=("unregistered", values.job, values.slots[0].object_id))
    before = adapter.graph.snapshot()
    request = protocol.PrepareContainedGraph(naked)
    rejected = adapter.mutate_graph(request)
    assert not rejected.accepted and rejected.request == request and rejected.error
    assert adapter.graph.snapshot() == before
    assert adapter.output_recovery.publication_ids() == ()


def test_graph_manifest_cannot_rebind_an_admitted_output_identity():
    values = _Fixture()
    graph = values.manifest.to_graph_manifest()
    adapter = control.PublicationControlAdapter()
    adapter.output_recovery.report_intent(values.manifest)
    assert adapter.mutate_graph(protocol.PrepareContainedGraph(graph)).accepted
    before = adapter.graph.snapshot()
    changed = replace(graph, manifest_digest="0" * 64)
    rejected = adapter.mutate_graph(protocol.PrepareContainedGraph(changed))
    assert not rejected.accepted and rejected.request.manifest == changed
    assert adapter.graph.snapshot() == before
    assert adapter.output_recovery.snapshot(values.publication_id).manifest == values.manifest


def test_admission_close_keeps_exact_graph_replay_and_cleanup_but_rejects_new_work():
    values = _Fixture()
    graph = values.manifest.to_graph_manifest()
    adapter = control.PublicationControlAdapter()
    intent = wire.ReportOutputPublicationIntent(values.manifest)
    assert adapter.report_output_recovery(intent).accepted
    assert adapter.mutate_graph(protocol.PrepareContainedGraph(graph)).accepted
    adapter.close_admission()
    replay = adapter.report_output_recovery(intent)
    assert replay.accepted and replay.request == intent
    assert adapter.mutate_graph(protocol.PrepareContainedGraph(graph)).receipt.disposition is ContainedGraphManifestDisposition.ALREADY_PREPARED
    other_header = replace(values.header, publication_id=replace(values.publication_id, lease_id=type(values.lease).random()))
    other = OutputPublicationManifest.create(other_header, values.slots)
    before = adapter.graph.snapshot()
    assert not adapter.report_output_recovery(wire.ReportOutputPublicationIntent(other)).accepted
    assert not adapter.mutate_graph(protocol.PrepareContainedGraph(other.to_graph_manifest())).accepted
    assert adapter.graph.snapshot() == before
    assert adapter.output_recovery.publication_ids() == (values.publication_id,)
    _finish_publication(adapter, values, graph)


@pytest.mark.parametrize("kind", ("node", "owner"))
def test_unified_graph_forward_operations_respect_exact_death_fence(kind):
    values = _Fixture()
    graph = values.manifest.to_graph_manifest()
    adapter = control.PublicationControlAdapter()
    registry = adapter.output_recovery
    registry.report_intent(values.manifest)
    assert adapter.mutate_graph(protocol.PrepareContainedGraph(graph)).accepted
    registry.arm_complete(values.publication_id, values.manifest.manifest_digest)
    node = values.header.node_incarnation
    if kind == "node":
        registry.freeze_node_death(protocol.NodeDeathRecord(
            "graph-publisher-exit", node.node_id, node.node_pid, node.registration_epoch,
            1, -9, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed publisher exit",
        ))
    else:
        registry.freeze_owner_death(protocol.WorkerDeathRecord(
            "graph-owner-exit", protocol.WorkerIncarnation(
                node.node_id, node.node_pid, node.registration_epoch, values.owner, 1999,
            ), 1, -9, protocol.WorkerDeathReason.PROCESS_EXIT,
        ))
    before = adapter.graph.snapshot(), registry.snapshot(values.publication_id)
    for request in (protocol.PrepareContainedGraph(graph), protocol.CommitContainedGraph(graph),
                    protocol.AbortContainedGraph(graph)):
        reply = adapter.mutate_graph(request)
        assert not reply.accepted and reply.request == request
    assert (adapter.graph.snapshot(), registry.snapshot(values.publication_id)) == before


def test_gcs_generic_routes_and_typed_dispatch_share_one_publication_authority(monkeypatch):
    # Reuse registered Node/Worker values and the no-socket GCS fixture.
    service, values = _service(monkeypatch)
    adapter = service.publications
    assert type(adapter) is control.PublicationControlAdapter
    assert not hasattr(service, "stored_publications")
    graph = values.manifest.to_graph_manifest()
    handlers = {
        control.PREPARE_CONTAINED_GRAPH_HANDLER, control.COMMIT_CONTAINED_GRAPH_HANDLER,
        control.ABORT_CONTAINED_GRAPH_HANDLER, control.RELEASE_CONTAINED_GRAPH_CONTAINER_HANDLER,
        control.GET_CONTAINED_GRAPH_HANDLER,
    }
    assert handlers <= set(service.handlers)
    assert not any("stored_publication_graph" in name for name in service.handlers)
    assert service.report_output_publication(wire.ReportOutputPublicationIntent(values.manifest)).accepted
    request = protocol.PrepareContainedGraph(graph)
    prepared = service.handlers[control.PREPARE_CONTAINED_GRAPH_HANDLER](request)
    replay = service.handle(request)
    assert prepared.accepted and replay.receipt.disposition is ContainedGraphManifestDisposition.ALREADY_PREPARED
    assert service.report_output_publication(wire.ArmOutputPublication(values.publication_id, values.manifest.manifest_digest)).accepted
    assert service.report_output_publication(wire.ReportOutputPublicationTerminal(values.witness)).accepted
    assert service.handle(protocol.CommitContainedGraph(graph)).accepted
    found = service.handlers[control.GET_CONTAINED_GRAPH_HANDLER](protocol.GetContainedGraph(graph.transaction_id))
    assert found.manifest == graph and adapter.graph.get_manifest(graph.transaction_id) == graph
    proof = OutputPublicationAdoptionProof(values.witness, values.owner, "generic-route-owner-CAS")
    assert service.report_output_publication(wire.ReportOutputPublicationAdopted(proof)).accepted
    for slot in values.slots:
        released = service.release_contained_graph_container(protocol.ReleaseContainedGraphContainer(graph, slot.object_id))
        assert released.accepted and released.receipt.released_edges == slot.edges
    assert not adapter.has_active_operations()
