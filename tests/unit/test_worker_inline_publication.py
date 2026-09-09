"""Pure Worker INLINE result contracts on single-output owner-led publication.

The historical filename remains; no OPEN/PREPARE_INLINE success backend is
mocked. Reused fixtures forbid runtime creation, sockets and real waits.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

from miniray import output_protocol as wire, protocol
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope, OutputPublicationManifest,
)
from miniray.ownership import StoredContainedReferenceDisposition
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER, PREPARE_STORED_CONTAINED_PIN_HANDLER,
    PROMOTE_STORED_CONTAINED_PIN_HANDLER, START_WORKER_LEASE_HANDLER, WorkerServer,
)
from miniray.transport import TransportError
from tests.unit.test_output_publication import _Fixture as _OutputValues
from tests.unit.test_worker_unified_output import (
    _borrowed_fixture, _completion, _envelope, _no_runtime as _no_runtime,
)


pytestmark = pytest.mark.unit


def test_inline_result_serializes_once_and_orders_prepare_complete_then_cache(monkeypatch):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=65536)
    prepared_records = []

    def prepare(request):
        assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
        assert f.pending.nested_imports is imports and not child.closed
        assert f.pending.outputs.manifest == request.manifest
        assert f.pending.outputs.slot_payloads == request.slot_payloads
        prepared_records.append(f.pending)
        return wire.PreparedOutputPublicationReply(request.request_identity, True)

    def complete(request):
        assert f.key not in f.worker._replies
        assert f.pending.prepare_acked and f.pending.complete_envelope is None
        assert child.closed and child.closes == 1 and f.pending.nested_imports is None
        return f.complete(request)

    f.on_prepare, f.on_complete = prepare, complete
    reply = f.worker._handle_push_task(f.push)
    assert f.handlers == [START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER,
                          COMPLETE_WORKER_LEASE_HANDLER]
    assert f.executions == reductions == [True]
    assert reply is f.worker._replies[f.key]
    assert reply.output_publication == f.complete_envelope
    assert reply.results == f.complete_envelope.results
    assert len(reply.results) == 1 and reply.results[0].storage is protocol.ResultStorage.INLINE
    assert not hasattr(reply, "contained_edges")
    assert not hasattr(reply, "stored_publication") and not hasattr(reply, "inline_publication")
    assert not f.worker._prepared_output_replies
    assert prepared_records[0].discovery.source_references == ()
    assert f.worker._handle_push_task(f.push) is reply
    assert f.executions == reductions == [True] and child.closes == 1 and len(f.calls) == 3


def test_lost_prepare_ack_replays_exact_batch_without_reserialization(monkeypatch):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=65536)
    accept = [False]

    def prepare(request):
        assert not child.closed and f.pending.nested_imports is imports
        assert request.manifest == f.pending.outputs.manifest
        if not accept[0]:
            raise TransportError("lost PREPARE acknowledgement")
        return wire.PreparedOutputPublicationReply(request.request_identity, True)

    f.on_prepare = prepare
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    outputs = pending.outputs
    assert not pending.prepare_acked and pending.complete_envelope is None
    assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
    assert f.executions == reductions == [True] and child.closes == 0
    assert COMPLETE_WORKER_LEASE_HANDLER not in f.handlers
    previous_prepares = len(f.prepares)
    accept[0] = True
    reply = f.worker._handle_push_task(f.push)
    assert len(f.prepares) == previous_prepares + 1
    assert all(request == f.prepares[0] for request in f.prepares)
    assert pending.outputs is outputs and reply.output_publication.manifest == outputs.manifest
    assert reply.output_publication == f.complete_envelope
    assert f.executions == reductions == [True] and child.closes == 1
    assert f.handlers.count(START_WORKER_LEASE_HANDLER) == 1
    assert f.handlers.count(COMPLETE_WORKER_LEASE_HANDLER) == 1
    rpc_count = len(f.calls)
    assert f.worker._handle_push_task(f.push) is reply and len(f.calls) == rpc_count


def test_lost_complete_ack_replays_only_complete(monkeypatch):
    f, child, _imports, reductions = _borrowed_fixture(monkeypatch, threshold=65536)
    accept = [False]
    completions = []

    def complete(request):
        completions.append(request)
        assert child.closed and child.closes == 1
        assert f.pending.prepare_acked and f.key not in f.worker._replies
        if not accept[0]:
            raise TransportError("lost Complete acknowledgement")
        return f.complete(request)

    f.on_complete = complete
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    outputs = pending.outputs
    assert pending.prepare_acked and pending.complete_envelope is None
    assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
    assert pending.nested_imports is None and pending.discovery.source_references == ()
    assert f.executions == reductions == [True] and len(f.prepares) == 1
    before = tuple(f.calls)
    accept[0] = True
    reply = f.worker._handle_push_task(f.push)
    assert tuple(f.calls[:-1]) == before and f.handlers[-1] == COMPLETE_WORKER_LEASE_HANDLER
    assert all(request == completions[0] for request in completions)
    assert pending.outputs is outputs and reply.output_publication == f.complete_envelope
    assert f.executions == reductions == [True] and len(f.prepares) == 1
    assert not f.worker._prepared_output_replies and f.key in f.worker._completion_acked
    rpc_count = len(f.calls)
    assert f.worker._handle_push_task(f.push) is reply and len(f.calls) == rpc_count


def test_trusted_prepare_rejection_becomes_cached_system_error_after_compensation_ack(monkeypatch):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=65536)
    prepared_records = []

    def prepare(request):
        prepared_records.append(f.pending)
        return wire.PreparedOutputPublicationReply(
            request.request_identity, False, wire.OutputPublicationRPCErrorKind.INVALID_STATE,
            "contained pin admission rejected",
        )

    def complete(request):
        assert request.status is protocol.TaskReplyStatus.SYSTEM_ERROR
        assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
        assert f.pending.failure_reply is not None and f.pending.nested_imports is imports
        assert not child.closed
        return _completion(request)

    f.on_prepare, f.on_complete = prepare, complete
    reply = f.worker._handle_push_task(f.push)
    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert reply.error is not None and "contained pin admission rejected" in reply.error.message
    assert not reply.results and not hasattr(reply, "contained_edges")
    assert reply.output_publication is None
    assert not hasattr(reply, "inline_publication") and not hasattr(reply, "stored_publication")
    assert f.worker._replies[f.key] is reply and not f.worker._prepared_output_replies
    assert f.key in f.worker._completion_acked
    assert f.worker._cached_output_manifests[f.key] == prepared_records[0].outputs.manifest
    assert child.closed and child.closes == 1 and prepared_records[0].nested_imports is None
    assert f.executions == reductions == [True]
    assert f.handlers == [START_WORKER_LEASE_HANDLER, wire.PREPARE_OUTPUT_PUBLICATION_HANDLER,
                          COMPLETE_WORKER_LEASE_HANDLER]
    assert f.worker._handle_push_task(f.push) is reply and len(f.calls) == 3


def test_complete_rejects_same_execution_with_changed_inline_payload_and_manifest(monkeypatch):
    f, child, _imports, reductions = _borrowed_fixture(monkeypatch, threshold=65536)
    expected = []

    def complete(request):
        original = _envelope(f.prepared_request)
        expected.append(original)
        data = original.results[0].inline_data + b"changed"
        slot = replace(original.manifest.slots[0], size_bytes=len(data),
                       checksum=hashlib.sha256(data).hexdigest())
        manifest = OutputPublicationManifest.create(original.manifest.header, (slot,))
        result = replace(original.results[0], size_bytes=slot.size_bytes,
                         checksum=slot.checksum, inline_data=data)
        changed = OutputPublicationEnvelope(
            manifest, OutputPublicationCompleteWitness.for_manifest(manifest), (result,),
        )
        assert changed.publication_id == original.publication_id
        assert changed.manifest.manifest_digest != original.manifest.manifest_digest
        return _completion(request, changed)

    f.on_complete = complete
    with pytest.raises(RuntimeError, match="exact PushTask replay") as raised:
        f.worker._handle_push_task(f.push)
    assert isinstance(raised.value.__cause__, RuntimeError)
    assert "changed the discovered manifest" in str(raised.value.__cause__)
    pending = f.pending
    assert pending.outputs.manifest == expected[0].manifest and pending.failure_reply is None
    assert pending.complete_envelope is None and pending.prepare_acked
    assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
    assert f.executions == reductions == [True] and child.closes == 1
    f.on_complete = None
    reply = f.worker._handle_push_task(f.push)
    assert reply.output_publication == expected[0] and len(f.prepares) == 1
    assert f.executions == reductions == [True] and child.closes == 1


class _ImmediateCondition:
    """Pure drain observation: no execution is running, so waiting is a bug."""

    def __init__(self):
        self.entries = 0

    def __enter__(self):
        self.entries += 1
        return self

    def __exit__(self, *_args):
        return None

    def wait_for(self, *_args, **_kwargs):
        pytest.fail("inactive unresolved output must return unclean without waiting")

    def notify_all(self):
        return None


def test_drain_returns_unclean_for_inactive_prepared_inline_state_without_wait(monkeypatch):
    f, child, imports, reductions = _borrowed_fixture(monkeypatch, threshold=65536)
    f.on_prepare = lambda _request: (_ for _ in ()).throw(TransportError("prepare still unresolved"))
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    f.worker._drain_request_id = "drain-inline"
    f.worker._active_tasks = 0
    f.worker._push_obligations = set()  # retained publication alone must fence clean
    condition = _ImmediateCondition()
    f.worker._lifecycle = condition
    f.worker._shutdown_embedded_core = lambda *_a, **_k: pytest.fail("unresolved task closed owner Core")
    monkeypatch.setattr("miniray.worker.time.monotonic", lambda: 100.0)
    attempts = []

    def pending_rpc(address, handler, request, **options):
        assert address == f.worker.node_address
        assert handler == wire.PREPARE_OUTPUT_PUBLICATION_HANDLER
        assert request.manifest == pending.outputs.manifest
        assert options["deadline"] == 100.25
        assert options["request_timeout"] == 0.25 and options["connect_timeout"] == 0.25
        attempts.append(request)
        raise TransportError("prepare remains unresolved during drain")

    monkeypatch.setattr("miniray.worker.rpc_request", pending_rpc)
    status = f.worker._drain_status("drain-inline", timeout=0.25)
    assert status.drain_started and not status.clean and not f.worker._drain_clean
    assert "unresolved" in status.detail
    assert condition.entries == 1 and len(attempts) == 1
    assert f.pending is pending and pending.nested_imports is imports and not child.closed
    assert f.executions == reductions == [True]
    assert f.key not in f.worker._replies and f.key not in f.worker._completion_acked
    assert f.worker._output_drain_deadline is None
    assert f.worker._execution_lock.acquire(blocking=False)
    f.worker._execution_lock.release()


@pytest.mark.parametrize("operation", ("prepare", "promote"))
def test_worker_handler_map_forwards_contained_pin_to_existing_core_only(monkeypatch, operation):
    """The unified backend reuses these child-owner pin primitives.

    Preserve the old inline-handler authority contract, now on both live
    prepare/promote routes; a fake transport captures only the registration map.
    """
    values = _OutputValues()
    handlers = {}

    class _CapturingServer:
        def __init__(self, supplied, **_kwargs):
            handlers.update(supplied)
            self.address = ("127.0.0.1", 33100)

    monkeypatch.setattr("miniray.worker.TCPServer", _CapturingServer)
    server = WorkerServer(values.executor, node_id=values.node)
    assert "install_inline_contained_pin" not in handlers
    assert not hasattr(server, "_handle_install_inline_contained_pin")
    assert PREPARE_STORED_CONTAINED_PIN_HANDLER in handlers
    assert PROMOTE_STORED_CONTAINED_PIN_HANDLER in handlers
    calls = []
    disposition = (StoredContainedReferenceDisposition.PREPARED if operation == "prepare"
                   else StoredContainedReferenceDisposition.PROMOTED)

    class _ExistingCore:
        def prepare_stored_contained_pin(self, request):
            assert operation == "prepare"
            calls.append(request)
            return protocol.StoredContainedPinReply(request, disposition)

        def promote_stored_contained_pin(self, request):
            assert operation == "promote"
            calls.append(request)
            return protocol.StoredContainedPinReply(request, disposition)

    core = _ExistingCore()
    server._embedded_core = core
    monkeypatch.setattr(server, "_embedded_core_for", lambda *_a, **_k: pytest.fail("owner RPC created a Core"))
    name = PREPARE_STORED_CONTAINED_PIN_HANDLER if operation == "prepare" else PROMOTE_STORED_CONTAINED_PIN_HANDLER
    request_type = protocol.PrepareStoredContainedPin if operation == "prepare" else protocol.PromoteStoredContainedPin
    handler = handlers[name]
    transfer = values.slots[0].transfers[0]
    request = request_type(transfer, values.executor)
    reply = handler(request)
    assert type(reply) is protocol.StoredContainedPinReply
    assert reply.request == request and reply.accepted and reply.disposition is disposition
    assert calls == [request] and server._embedded_core is core

    # Fully valid foreign-owner transfer still cannot cross this endpoint.
    foreign = request_type(values.slots[0].transfers[1], values.foreign_owner)
    assert foreign.authority_worker_id != server.worker_id
    wrong = handler(foreign)
    assert wrong.request == foreign and not wrong.accepted
    assert wrong.error_kind is protocol.StoredPublicationRPCErrorKind.INVALID_REQUEST
    assert calls == [request] and server._embedded_core is core

    server._embedded_core = None
    missing = handler(request)
    assert missing.request == request and not missing.accepted
    assert missing.error_kind is protocol.StoredPublicationRPCErrorKind.UNAVAILABLE
    assert "OWNER_STOPPED" in missing.error
    assert server._embedded_core is None and calls == [request]
    with pytest.raises(TypeError):
        handler(object())
    assert calls == [request]
