"""Pure contracts for Node-owned RPC trace wiring.

These tests use in-memory fakes only: no listener, child process, or loopback
connection is created.
"""

from __future__ import annotations

import hashlib
import threading

import cloudpickle
import pytest

import miniray.actor_worker as actor_worker_module
import miniray.node as node_module
import miniray.worker as worker_module
from miniray import protocol
from miniray.ids import (
    ActorGeneration, ActorID, JobID, NodeID, WorkerID,
)
from miniray.node import NodeServer
from miniray.resources import ResourceLedger, ResourceVector
from miniray.trace import MemoryEventSink


pytestmark = pytest.mark.unit


class _Server:
    address = ("127.0.0.1", 29101)


class _CapturedServer:
    address = ("127.0.0.1", 29110)

    def __init__(self, handlers, **configuration):
        self.handlers = dict(handlers)
        self.configuration = configuration


def _startup_node() -> NodeServer:
    node = object.__new__(NodeServer)
    node.node_id = NodeID.random()
    node._node_pid = 7101
    node._registration_epoch = 0
    node._membership_epoch = 0
    node._server = _Server()
    node._ledger = ResourceLedger(ResourceVector({"CPU": 1}))
    node._state_lock = threading.RLock()
    node._gcs_lifecycle_lock = threading.Lock()
    node._gcs_address = ("127.0.0.1", 29100)
    node._registered_with_gcs = False
    node._resource_report_version = 0
    node._resource_reported_version = 0
    node.event_sink = MemoryEventSink(
        clock_ns=lambda: 1, process_id=lambda: 7101
    )
    return node


def test_background_rpc_attaches_node_sink_and_preserves_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _startup_node()
    calls: list[tuple[object, ...]] = []

    def request(address, handler, message, **options):
        calls.append((address, handler, message, options))
        return "reply"

    monkeypatch.setattr(node_module, "rpc_request", request)
    message = object()

    assert node._background_rpc(
        ("127.0.0.1", 29102), "background", message,
        request_timeout=0.25,
    ) == "reply"
    assert calls == [
        (
            ("127.0.0.1", 29102),
            "background",
            message,
            {
                "request_timeout": 0.25,
                "event_sink": node.event_sink,
                "trace_component": "node",
            },
        )
    ]


def test_worker_and_actor_servers_bind_their_process_trace_sinks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker_servers: list[_CapturedServer] = []
    actor_servers: list[_CapturedServer] = []
    monkeypatch.setattr(
        worker_module, "TCPServer",
        lambda handlers, **configuration: worker_servers.append(
            _CapturedServer(handlers, **configuration)
        ) or worker_servers[-1],
    )
    monkeypatch.setattr(
        actor_worker_module, "TCPServer",
        lambda handlers, **configuration: actor_servers.append(
            _CapturedServer(handlers, **configuration)
        ) or actor_servers[-1],
    )
    worker_sink = MemoryEventSink(process_id=lambda: 7201)
    actor_sink = MemoryEventSink(process_id=lambda: 7202)

    worker_module.WorkerServer(
        WorkerID.random(), event_sink=worker_sink
    )

    actor_id = ActorID.random()
    job_id = JobID.random()
    payload = cloudpickle.dumps(type("TraceActor", (), {}))
    definition = protocol.ActorClassDefinition(
        protocol.FunctionKey(
            job_id, __name__, "TraceActor", "trace-wiring-v1"
        ),
        payload,
        hashlib.sha256(payload).hexdigest(),
        ("work",),
    )
    actor_worker_module.ActorWorkerServer(
        actor_id,
        ActorGeneration(actor_id, 0),
        WorkerID.random(),
        definition,
        object(),
        NodeID.random(),
        ("127.0.0.1", 29111),
        event_sink=actor_sink,
    )

    assert worker_servers[0].configuration["event_sink"] is worker_sink
    assert worker_servers[0].configuration["trace_component"] == "worker"
    assert actor_servers[0].configuration["event_sink"] is actor_sink
    assert (
        actor_servers[0].configuration["trace_component"]
        == "actor_worker"
    )


def test_node_startup_and_resource_outbox_use_background_trace_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    node = _startup_node()
    calls: list[tuple[str, dict[str, object]]] = []

    def request(_address, handler, message, **options):
        calls.append((handler, options))
        if handler == node_module.GCS_REGISTER_NODE_HANDLER:
            assert isinstance(message, protocol.RegisterNode)
            return protocol.RegisterNodeReply(
                node.node_id, node._node_pid, True, 3, 4
            )
        assert handler == node_module.GCS_UPDATE_NODE_RESOURCES_HANDLER
        assert isinstance(message, protocol.UpdateNodeResources)
        return protocol.UpdateNodeResourcesReply(
            node.node_id, node._node_pid, node._registration_epoch,
            message.report_seq, True,
        )

    monkeypatch.setattr(node_module, "rpc_request", request)

    node._register_with_gcs()
    with node._state_lock:
        node._mark_resource_report_pending_locked()
    assert node._flush_pending_resource_report()

    assert [handler for handler, _options in calls] == [
        node_module.GCS_REGISTER_NODE_HANDLER,
        node_module.GCS_UPDATE_NODE_RESOURCES_HANDLER,
    ]
    assert all(
        options == {
            "event_sink": node.event_sink,
            "trace_component": "node",
        }
        for _handler, options in calls
    )


def test_legacy_state_fixture_without_sink_keeps_original_rpc_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tracing stays observational for narrow pre-trace fixtures."""

    node = object.__new__(NodeServer)
    observed: list[tuple[object, ...]] = []

    def request(address, handler, message, *, request_timeout=None):
        observed.append((address, handler, message, request_timeout))
        return "legacy-reply"

    monkeypatch.setattr(node_module, "rpc_request", request)
    message = object()
    assert node._background_rpc(
        ("127.0.0.1", 29103), "legacy", message,
        request_timeout=0.5,
    ) == "legacy-reply"
    assert observed == [
        (("127.0.0.1", 29103), "legacy", message, 0.5)
    ]
