"""Pure base GCS ownership boundary and shared inert control fixture.

The service/Node registration helpers remain for explicitly tracked legacy
consumers. They construct current base metadata authorities and no server is
started. Ordinary output handoff and cleanup live at Core/Node/child owners.
"""

from dataclasses import replace
import socket
import threading
import time

import pytest

from miniray import control, protocol
from miniray.output_publication import OutputPublicationManifest, OutputPublicationCompleteWitness
from miniray.resources import ResourceVector
from tests.unit.test_output_publication import _Fixture


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
    values.manifest = OutputPublicationManifest.create(header, (values.value))
    values.witness = OutputPublicationCompleteWitness.for_manifest(values.manifest)
    worker = protocol.WorkerIncarnation(
        node.node_id, node.node_pid, registered.registration_epoch, values.executor, 1801,
    )
    assert service.register_worker_incarnation(protocol.RegisterWorkerIncarnation(worker)).accepted
    return service, values


def test_base_gcs_has_no_ordinary_output_publication_authority_or_routes(monkeypatch):
    """One unstarted GCS; its constructor may create only base authorities."""
    def forbidden(*_args, **_kwargs):
        pytest.fail("base GCS attempted a publication authority or runtime effect")

    monkeypatch.setattr(control, "TCPServer", _Server)
    monkeypatch.setattr(control, "rpc_request", forbidden)
    for name in (
        "PublicationControlAdapter", "OutputPublicationRecoveryAuthority",
        "ContainedReferenceGraphAuthority", "PublicationAuthority",
        "StoredPublicationControlAdapter", "StoredPublicationRecoveryAuthority",
        "InlinePublicationRecoveryAuthority", "StoredNodeLossRuntime",
        "InlineNodeLossRuntime", "PublicationOwnerDeathRuntime",
    ):
        if hasattr(control, name):
            monkeypatch.setattr(control, name, forbidden)

    service = control.GCSLite()
    assert type(service._server) is _Server
    assert set(service._server.handlers) == set(service.handlers)
    for field in (
        "publications", "stored_publications", "output_recovery",
        "graph", "contained_graph", "publication_authority",
    ):
        assert not hasattr(service, field), field
    for handler in service.handlers:
        assert not any(part in handler for part in (
            "output_publication", "output_node_loss", "contained_graph",
            "stored_publication", "inline_publication",
            "stored_node_loss", "inline_node_loss",
        )), handler
    assert "report_node_death" in service.handlers
    assert "drain_owner_death_fences" in service.handlers
