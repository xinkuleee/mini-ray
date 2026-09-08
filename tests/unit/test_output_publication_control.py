"""Pure GCS handler/graph composition for the unified output wire path."""

from dataclasses import replace
import socket
import threading
import time

import pytest

from miniray import control, protocol, output_protocol as wire
from miniray.contained_cycle import (
    ContainedGraphTransactionState, ContainedReferenceGraphAuthority,
)
from miniray.output_publication import OutputPublicationManifest, OutputPublicationCompleteWitness
from miniray.output_publication_journal import OutputPublicationAdoptionProof, OutputPublicationSlotCleanupProof
from miniray.output_recovery import (
    OutputPublicationRecoveryAuthority, OutputRecoveryAction, OutputRecoveryDisposition,
)
from miniray.resources import ResourceVector
from tests.unit.test_output_publication import _Fixture, _assert_metadata


pytestmark = pytest.mark.unit


class _Server:
    """Constructor-only TCP boundary; no listener or worker thread exists."""

    def __init__(self, handlers, **_kwargs):
        self.handlers = handlers
        self.address = ("127.0.0.1", 32999)


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure control fixture attempted runtime work")

    monkeypatch.setattr(threading.Thread, "start", forbidden)
    monkeypatch.setattr(threading.Event, "wait", forbidden)
    monkeypatch.setattr(threading.Condition, "wait", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _service(monkeypatch, *, refs=True):
    monkeypatch.setattr(control, "TCPServer", _Server)
    service = control.GCSLite()
    values = _Fixture(refs=refs)
    node = values.header.node_incarnation
    registered = service.register_node(protocol.RegisterNode(
        node_id=node.node_id, node_pid=node.node_pid, address=("127.0.0.1", 32998),
        total_resources=ResourceVector({"CPU": 1}),
    ))
    header = replace(values.header, node_incarnation=replace(node, registration_epoch=registered.registration_epoch))
    values.header = header
    values.manifest = OutputPublicationManifest.create(header, values.slots)
    values.witness = OutputPublicationCompleteWitness.for_manifest(values.manifest)
    worker = protocol.WorkerIncarnation(
        node.node_id, node.node_pid, registered.registration_epoch, values.executor, 1801,
    )
    assert service.register_worker_incarnation(protocol.RegisterWorkerIncarnation(worker)).accepted
    return service, values


def test_normal_gcs_constructs_only_unified_publication_authorities_and_routes(monkeypatch):
    """Resource review: one unstarted GCS, no registered nodes or outputs."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("GCS construction attempted transport or a legacy runtime")

    monkeypatch.setattr(control, "TCPServer", _Server)
    monkeypatch.setattr(control, "rpc_request", forbidden)
    # No imports of archived implementations: if an old constructor remains
    # exposed here, invoking it is a regression even if its field is discarded.
    for name in (
        "StoredPublicationControlAdapter", "StoredPublicationRecoveryAuthority",
        "InlinePublicationRecoveryAuthority", "StoredNodeLossRuntime",
        "InlineNodeLossRuntime", "PublicationOwnerDeathRuntime",
    ):
        if hasattr(control, name):
            monkeypatch.setattr(control, name, forbidden)

    service = control.GCSLite()
    adapter = service.publications
    assert type(adapter) is control.PublicationControlAdapter
    assert type(adapter.output_recovery) is OutputPublicationRecoveryAuthority
    assert type(adapter.graph) is ContainedReferenceGraphAuthority
    assert adapter.output_recovery.publication_ids() == ()
    assert not adapter.graph.snapshot().committed_edges
    assert not adapter.graph.snapshot().prepared_edges
    assert not adapter.has_active_operations()
    assert not hasattr(service, "stored_publications")
    for name in ("recovery", "inline_recovery", "node_loss", "inline_node_loss", "owner_death"):
        assert not hasattr(adapter, name), name

    handlers = service.handlers
    assert type(service._server) is _Server
    assert set(service._server.handlers) == set(handlers)
    assert {
        wire.REPORT_OUTPUT_PUBLICATION_HANDLER, wire.GET_OUTPUT_PUBLICATION_RECOVERY_HANDLER,
        wire.GET_OUTPUT_NODE_LOSS_HANDLER, wire.DECIDE_OUTPUT_NODE_LOSS_HANDLER,
        wire.PROGRESS_OUTPUT_NODE_LOSS_HANDLER,
        "prepare_contained_graph", "commit_contained_graph", "abort_contained_graph",
        "release_contained_graph_container", "get_contained_graph",
    }.issubset(handlers)
    assert not any(
        "stored_publication" in handler or "inline_publication" in handler
        or "stored_node_loss" in handler or "inline_node_loss" in handler
        for handler in handlers
    )
    assert "acknowledge_stored_owner_retirement" not in handlers


def test_registered_metadata_path_and_one_graph_commit_have_exact_gates(monkeypatch):
    service, values = _service(monkeypatch)
    report = service.handlers[wire.REPORT_OUTPUT_PUBLICATION_HANDLER]
    graph = values.manifest.to_graph_manifest()
    assert not service.prepare_contained_graph(protocol.PrepareContainedGraph(graph)).accepted
    intent = report(wire.ReportOutputPublicationIntent(values.manifest))
    assert intent.accepted
    assert service.prepare_contained_graph(protocol.PrepareContainedGraph(graph)).accepted
    assert not service.commit_contained_graph(protocol.CommitContainedGraph(graph)).accepted
    assert report(wire.ArmOutputPublication(values.publication_id, values.manifest.manifest_digest)).accepted
    assert not service.commit_contained_graph(protocol.CommitContainedGraph(graph)).accepted
    assert report(wire.ReportOutputPublicationTerminal(values.witness)).accepted
    committed = service.commit_contained_graph(protocol.CommitContainedGraph(graph))
    assert committed.accepted and committed.receipt.state is ContainedGraphTransactionState.COMMITTED
    assert not service.abort_contained_graph(protocol.AbortContainedGraph(graph)).accepted
    proof = OutputPublicationAdoptionProof(values.witness, values.owner, "owner-CAS")
    assert report(wire.ReportOutputPublicationAdopted(proof)).accepted
    assert service.publications.has_active_operations()
    for index, slot in enumerate(values.slots):
        released = service.release_contained_graph_container(protocol.ReleaseContainedGraphContainer(graph, slot.object_id))
        assert released.accepted and released.receipt.released_edges == slot.edges
        assert report(wire.ReportOutputPublicationSlotCollected(OutputPublicationSlotCleanupProof(
            values.witness, values.owner, index, slot.object_id, "gc-{}".format(index),
        ))).accepted
    assert not service.publications.has_active_operations()
    query = service.handlers[wire.GET_OUTPUT_PUBLICATION_RECOVERY_HANDLER](wire.GetOutputPublicationRecovery(values.publication_id))
    assert query.found
    _assert_metadata(query)


def test_wrong_registered_publisher_incarnation_rejects_before_intent(monkeypatch):
    service, values = _service(monkeypatch)
    bad_header = replace(values.header, node_incarnation=replace(values.header.node_incarnation, node_pid=9999))
    bad = OutputPublicationManifest.create(bad_header, values.slots)
    reply = service.report_output_publication(wire.ReportOutputPublicationIntent(bad))
    assert not reply.accepted and reply.error_kind is wire.OutputPublicationRPCErrorKind.CONFLICT
    assert service.publications.output_recovery.publication_ids() == ()


def test_node_death_freezes_metadata_and_blocks_late_terminal_or_graph(monkeypatch):
    service, values = _service(monkeypatch)
    adapter = service.publications
    assert service.report_output_publication(wire.ReportOutputPublicationIntent(values.manifest)).accepted
    graph = values.manifest.to_graph_manifest()
    assert service.prepare_contained_graph(protocol.PrepareContainedGraph(graph)).accepted
    assert service.report_output_publication(wire.ArmOutputPublication(values.publication_id, values.manifest.manifest_digest)).accepted
    node = values.header.node_incarnation
    reply = adapter.commit_node_death(lambda: service.nodes.report_death(protocol.ReportNodeDeath(
        "output-node-exit", node.node_id, node.node_pid, node.registration_epoch, 1,
        protocol.NodeDeathReason.PROCESS_EXIT, "observed child exit",
    )))
    assert reply.death is not None
    (work,) = adapter.output_recovery.frozen_workset(reply.death)
    assert work.action is OutputRecoveryAction.COMPLETION_UNKNOWN
    late = service.report_output_publication(wire.ReportOutputPublicationTerminal(values.witness))
    assert not late.accepted and late.ack.disposition is OutputRecoveryDisposition.FENCED
    assert not service.commit_contained_graph(protocol.CommitContainedGraph(graph)).accepted
    assert adapter.output_recovery.frozen_workset(reply.death) == (work,)
    _assert_metadata(work)


def test_no_edge_publication_still_blocks_control_exit_until_slot_cleanup(monkeypatch):
    service, values = _service(monkeypatch, refs=False)
    report = service.report_output_publication
    assert report(wire.ReportOutputPublicationIntent(values.manifest)).accepted
    assert service.publications.has_active_operations()
    assert report(wire.ArmOutputPublication(values.publication_id, values.manifest.manifest_digest)).accepted
    assert report(wire.ReportOutputPublicationTerminal(values.witness)).accepted
    for index, slot in enumerate(values.slots):
        assert report(wire.ReportOutputPublicationSlotCollected(OutputPublicationSlotCleanupProof(
            values.witness, values.owner, index, slot.object_id, "no-edge-gc-{}".format(index),
        ))).accepted
    assert not service.publications.has_active_operations()
