"""Pure transfer-pin cleanup from exact Node-death reducer inputs.

Two passive Nodes and two 1 KiB stores per case; the independent-source case
adds one passive Node and one 1 KiB store. Actual Seal/Pin/Release and outbox
reducers run, with at most two sessions for one small object. GCS replies are
explicit typed inputs, not evidence that these fixture Nodes/processes died.

One fixed malformed-reply table and at most two forced drives per normal case.
No GCS service, Core, socket, process, thread, timer, wait or user code starts.
The in-flight case injects death synchronously inside its single Release RPC;
physical source pins are released normally in finally, never cleared as proof.
"""

from copy import deepcopy
from dataclasses import replace

import pytest

from miniray import node as node_module, protocol
from miniray.ids import NodeID, WorkerID
from miniray.transport import TransportTimeout
from tests.unit.test_cancelled_grant_inventory import _NodeFixture, _node, _no_runtime as _no_runtime


pytestmark = pytest.mark.unit


def _outbox(node):
    with node._state_lock:
        return node._source_pin_outbox_locked()


def _clean(node):
    with node._state_lock:
        return node._cleanup_plane_quiescent_locked()


def _release(pin):
    return protocol.ReleaseObjectPin(pin.transfer_id, pin.descriptor.object_id, pin.requester_node_id)


def _dead_reply(node_id, *, pid=4101, registration=1):
    death = protocol.NodeDeathRecord(
        "transfer-peer-exit-{}".format(node_id), node_id, pid, registration, 3, -9,
        protocol.NodeDeathReason.PROCESS_EXIT, "typed transfer-peer death input",
    )
    # DTOs entering the test must not share the fixture NodeID before we
    # deliberately corrupt one nested wire field.
    return deepcopy(protocol.GetNodeStateReply(
        node_id, True, 3, protocol.NodeMembershipState.DEAD, pid, registration, death,
    ))


def _alive_reply(node_id):
    return protocol.GetNodeStateReply(node_id, True, 3, protocol.NodeMembershipState.ALIVE, 4102, 2)


def _install_gcs_inputs(node, monkeypatch, replies, *, limit=4):
    node._gcs_address = ("gcs-input.invalid", 41)
    calls = []

    def query(address, handler, request, **kwargs):
        assert address == node._gcs_address and handler == node_module.GCS_GET_NODE_STATE_HANDLER
        assert type(request) is protocol.GetNodeState and not node._state_lock._is_owned()
        assert kwargs == {"connect_timeout": 0.25, "request_timeout": 0.5}
        calls.append(request)
        assert len(calls) <= limit and request.node_id in replies
        value = replies[request.node_id]
        if isinstance(value, BaseException):
            raise value
        return value

    monkeypatch.setattr(node, "_background_rpc", query)
    return calls


def _begin_actual_pin(f, transfer_id, *, source=None, descriptor=None, requester=None):
    source = f.source if source is None else source
    descriptor = f.descriptors[0] if descriptor is None else descriptor
    requester = f.target.node_id if requester is None else requester
    pin = protocol.PinObjectForTransfer(transfer_id, descriptor, requester)
    reply = source._handle_pin_object_for_transfer(pin)
    assert reply.pinned and reply.transfer_id == transfer_id and reply.descriptor == descriptor
    return pin


def test_malformed_unknown_alive_or_worker_death_inputs_do_not_discharge_node_pin(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    pin = _begin_actual_pin(f, "bad-death-inputs")
    box = _outbox(f.target)
    record = box.begin(f.source_address, pin)
    try:
        assert f.target._query_transfer_node_death(f.source.node_id) is None  # No GCS: no inferred death.
        good = _dead_reply(f.source.node_id)
        wrong_death_node = replace(good, death=replace(good.death, node_id=NodeID(b"x" * 16)))
        bool_epoch = deepcopy(good)
        object.__setattr__(bool_epoch.death, "registration_epoch", True)
        malformed_id = deepcopy(good)
        object.__setattr__(malformed_id.death.node_id, "value", b"short")
        incarnation = protocol.WorkerIncarnation(f.source.node_id, 4101, 1, f.source.worker_id, 4201)
        worker_death = protocol.WorkerDeathRecord(
            "worker-not-node", incarnation, 1, -9, protocol.WorkerDeathReason.PROCESS_EXIT,
        )
        worker_reply = protocol.GetWorkerStateReply(
            f.source.worker_id, True, 1, protocol.WorkerMembershipState.DEAD, incarnation, worker_death,
        )
        invalid = (
            wrong_death_node,
            replace(good, node_pid=4109),
            replace(good, registration_epoch=9),
            replace(good, membership_epoch=2),
            bool_epoch, malformed_id,
            protocol.GetNodeStateReply(f.source.node_id, False, 3, error="node not found"),
            _alive_reply(f.source.node_id), worker_reply, TransportTimeout("GCS reply unavailable"),
        )
        replies = {f.source.node_id: None}
        calls = _install_gcs_inputs(f.target, monkeypatch, replies, limit=len(invalid))
        for candidate in invalid:
            replies[f.source.node_id] = candidate
            assert not f.target._drive_transfer_pins(force=True)
            assert box.pending() == (record,) and record.active and not record.in_flight
            assert record.source_death is None and not f.target._transfer_node_deaths
            assert not _clean(f.target)
            assert f.source.object_store.snapshot(pin.descriptor.object_id).pin_count == 1
        assert len(calls) == len(invalid) and not f.transfers
    finally:
        box.close(pin.transfer_id)
        assert f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert not box.has_pending()
        assert f.source._handle_release_object_pin(_release(pin)).accepted


def test_dead_requester_reclaims_only_its_session_while_same_object_live_reader_survives(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    live_peer = NodeID(b"u" * 16)
    dead_pin = _begin_actual_pin(f, "dead-reader")
    live_pin = _begin_actual_pin(f, "live-reader", requester=live_peer)
    wire = _dead_reply(f.target.node_id)
    expected = deepcopy(wire.death)
    replies = {f.target.node_id: wire, live_peer: _alive_reply(live_peer)}
    calls = _install_gcs_inputs(f.source, monkeypatch, replies, limit=2)
    try:
        assert f.source.object_store.snapshot(dead_pin.descriptor.object_id).pin_count == 2
        assert f.source._drive_transfer_pins(force=True)
        assert {request.node_id for request in calls} == {f.target.node_id, live_peer}
        assert f.source._closed_transfer_pins[dead_pin.transfer_id].closed
        assert f.source._pinned_transfers[dead_pin.transfer_id].released
        assert not f.source._pinned_transfers[live_pin.transfer_id].released
        assert live_pin.transfer_id not in f.source._closed_transfer_pins
        assert f.source.object_store.snapshot(dead_pin.descriptor.object_id).pin_count == 1
        assert f.source._handle_pin_object_for_transfer(live_pin).pinned
        chunk = f.source._handle_get_object_chunk(protocol.GetObjectChunk(
            live_pin.transfer_id, live_pin.descriptor.object_id, live_peer, 0, live_pin.descriptor.size_bytes,
        ))
        assert chunk.ok and chunk.data == f.payloads[0]
        assert not f.source._handle_pin_object_for_transfer(dead_pin).pinned
        assert not f.source._handle_pin_object_for_transfer(replace(dead_pin, transfer_id="late-new-dead-reader")).pinned
        assert not _clean(f.source)
        # The frozen death cache may not retain the mutable response object.
        object.__setattr__(wire.death, "detail", "mutated delivery")
        assert f.source._query_transfer_node_death(f.target.node_id) == expected
        assert len(calls) == 2 and f.source.object_store.get(dead_pin.descriptor.object_id) == f.payloads[0]
    finally:
        assert f.source._handle_release_object_pin(_release(dead_pin)).accepted
        assert f.source._handle_release_object_pin(_release(live_pin)).accepted
        assert f.source.object_store.snapshot(dead_pin.descriptor.object_id).pin_count == 0


def test_dead_source_discharges_only_its_outbox_and_live_source_still_requires_release_ack(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    other = _node(NodeID(b"v" * 16), WorkerID(b"w" * 16))
    other_address = ("other-source.invalid", 42)
    descriptor = replace(f.descriptors[0], node_id=other.node_id)
    assert other._handle_seal_object(protocol.SealObject.from_data(
        descriptor.object_id, descriptor.producer_attempt_id, descriptor.owner_worker_id, f.payloads[0],
    )).sealed
    first = _begin_actual_pin(f, "dead-source-close")
    second = _begin_actual_pin(f, "live-source-close", source=other, descriptor=descriptor)
    box = _outbox(f.target)
    first_record = box.begin(f.source_address, first)
    second_record = box.begin(other_address, second)
    box.close(first.transfer_id)
    box.close(second.transfer_id)
    calls = _install_gcs_inputs(f.target, monkeypatch, {
        f.source.node_id: _dead_reply(f.source.node_id), other.node_id: _alive_reply(other.node_id),
    }, limit=2)
    releases = []

    def release(address, handler, request, **options):
        assert handler == node_module.RELEASE_OBJECT_PIN_HANDLER and not f.target._state_lock._is_owned()
        releases.append((address, request))
        assert len(releases) <= 2
        if address == f.source_address:
            assert request == _release(first)
            raise TransportTimeout("source release unavailable, not itself death proof")
        assert address == other_address and request == _release(second)
        return other._handle_release_object_pin(request)

    monkeypatch.setattr(node_module, "rpc_request", release)
    try:
        f.target._drive_transfer_pins(force=True)
        assert box.pending() == (second_record,) and first_record.source_death is not None
        assert second_record.source_death is None and not second_record.in_flight
        assert not _clean(f.target) and len(releases) == 1 and len(calls) == 2
        # Supplying a death reducer input does not physically destroy a fixture.
        assert f.source.object_store.get(first.descriptor.object_id) == f.payloads[0]
        assert f.source.object_store.snapshot(first.descriptor.object_id).pin_count == 1
        assert other.object_store.snapshot(second.descriptor.object_id).pin_count == 1
        assert f.target._drive_transfer_pins(force=True)
        assert not box.has_pending() and _clean(f.target) and len(releases) == 2
        assert other.object_store.snapshot(second.descriptor.object_id).pin_count == 0
        assert len(calls) == 2
    finally:
        assert f.source._handle_release_object_pin(_release(first)).accepted
        assert other._handle_release_object_pin(_release(second)).accepted
        if box.has_pending():
            box.close(second.transfer_id)
            f.target._drive_source_pin_releases(force=True, max_effects=1)


def test_source_death_keeps_active_reader_pending_until_its_close_boundary(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    pin = _begin_actual_pin(f, "active-source-death")
    box = _outbox(f.target)
    record = box.begin(f.source_address, pin)
    calls = _install_gcs_inputs(f.target, monkeypatch, {f.source.node_id: _dead_reply(f.source.node_id)}, limit=1)

    def forbidden_release(*_args, **_kwargs):
        pytest.fail("an active or confirmed-dead source was contacted for release")

    monkeypatch.setattr(node_module, "rpc_request", forbidden_release)
    try:
        f.target._drive_transfer_pins(force=True)
        assert box.pending() == (record,) and record.active and not record.in_flight
        assert record.source_death is not None and not _clean(f.target)
        assert len(calls) == 1 and f.source.object_store.snapshot(pin.descriptor.object_id).pin_count == 1
        box.close(pin.transfer_id)
        assert box.has_pending()  # The active-reader cut, not death observation, owns removal.
        assert f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert not box.has_pending() and _clean(f.target) and len(calls) == 1
        # A pre-existing/stale caller can leave a new reducer record after the
        # proof was cached. Re-reading that proof must mark the current outbox,
        # not merely return the cached object and strand the new obligation.
        # Normal localization separately rejects this dead source at admission.
        stale_pin = replace(pin, transfer_id="cached-death-late-outbox")
        stale_record = box.begin(f.source_address, stale_pin)
        box.close(stale_pin.transfer_id)
        assert stale_record.source_death is None and box.has_pending()
        assert f.target._query_transfer_node_death(f.source.node_id) == record.source_death
        assert stale_record.source_death == record.source_death
        assert not box.has_pending() and _clean(f.target) and len(calls) == 1
    finally:
        if box.has_pending():
            box.close(pin.transfer_id)
            f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert f.source._handle_release_object_pin(_release(pin)).accepted


def test_source_death_during_close_rpc_preserves_inflight_ticket_until_settlement(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    pin = _begin_actual_pin(f, "inflight-source-death")
    box = _outbox(f.target)
    record = box.begin(f.source_address, pin)
    box.close(pin.transfer_id)
    calls = _install_gcs_inputs(f.target, monkeypatch, {f.source.node_id: _dead_reply(f.source.node_id)}, limit=1)
    observed = []

    def release(address, handler, request, **options):
        assert address == f.source_address and handler == node_module.RELEASE_OBJECT_PIN_HANDLER
        assert request == _release(pin) and not observed
        assert record.in_flight and not record.active and not _clean(f.target)
        proof = f.target._query_transfer_node_death(f.source.node_id)
        assert proof is not None and box.pending() == (record,)
        assert record.source_death == proof and record.in_flight and not _clean(f.target)
        observed.append(request)
        raise TransportTimeout("Release reply failed after exact source death was observed")

    monkeypatch.setattr(node_module, "rpc_request", release)
    try:
        assert f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert observed == [_release(pin)] and len(calls) == 1
        assert not box.has_pending() and not record.in_flight and _clean(f.target)
        assert f.source.object_store.get(pin.descriptor.object_id) == f.payloads[0]
        assert f.source.object_store.snapshot(pin.descriptor.object_id).pin_count == 1
    finally:
        assert f.source._handle_release_object_pin(_release(pin)).accepted


def test_dead_requester_unpin_failure_stays_closing_until_next_forced_drive(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    pin = _begin_actual_pin(f, "dead-reader-unpin-retry")
    calls = _install_gcs_inputs(f.source, monkeypatch, {f.target.node_id: _dead_reply(f.target.node_id)}, limit=1)
    actual_unpin = f.source.object_store.unpin
    unpins = []

    def unpin(object_id, token):
        unpins.append((object_id, token))
        assert (object_id, token) == (pin.descriptor.object_id, pin.transfer_id) and len(unpins) <= 2
        if len(unpins) == 1:
            raise RuntimeError("one exact source unpin failed")
        return actual_unpin(object_id, token)

    monkeypatch.setattr(f.source.object_store, "unpin", unpin)
    try:
        assert not f.source._drive_transfer_pins(force=True)
        session = f.source._pinned_transfers[pin.transfer_id]
        fence = f.source._closed_transfer_pins[pin.transfer_id]
        assert session.closing and not session.released and not fence.closed
        assert f.source.object_store.snapshot(pin.descriptor.object_id).pin_count == 1
        assert not _clean(f.source) and len(calls) == len(unpins) == 1
        assert not f.source._handle_pin_object_for_transfer(pin).pinned
        assert f.source._drive_transfer_pins(force=True)
        assert session.released and fence.closed and _clean(f.source)
        assert len(unpins) == 2 and unpins[0] == unpins[1] and len(calls) == 1
        assert f.source.object_store.snapshot(pin.descriptor.object_id).pin_count == 0
        replay = f.source._handle_release_object_pin(_release(pin))
        assert replay.accepted and not replay.released and len(unpins) == 2
    finally:
        monkeypatch.setattr(f.source.object_store, "unpin", actual_unpin)
        assert f.source._handle_release_object_pin(_release(pin)).accepted
