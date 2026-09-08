"""Pure live-source/live-target transfer-pin cleanup and close fencing.

Two passive Nodes use two 1 KiB stores and one tiny input per transfer. Source
Pin/Chunk/Release handlers and target pull/release drivers are real. Faults
affect response delivery or one actual ObjectStore pin/unpin operation only.
At most three initial close attempts and one forced retry; two-session tests
share one object without overwriting pin/lease tables. No Core, GCS, process,
thread, socket, wait, user code or actual shutdown implementation runs.
"""

from copy import deepcopy
from dataclasses import replace
import time

import pytest

from miniray import node as node_module, protocol
from miniray.ids import NodeID
from miniray.transfer_pins import TransferPinOutbox
from miniray.transport import TransportTimeout
from tests.unit.test_cancelled_grant_inventory import _NodeFixture, _no_runtime as _no_runtime


pytestmark = pytest.mark.unit


def _outbox(node):
    with node._state_lock:
        box = node._source_pin_outbox_locked()
        assert type(box) is TransferPinOutbox
        return box


def _clean(node):
    with node._state_lock:
        return node._cleanup_plane_quiescent_locked()


def _release(pin):
    return protocol.ReleaseObjectPin(pin.transfer_id, pin.descriptor.object_id, pin.requester_node_id)


def _assert_release(reply, pin):
    assert type(reply) is protocol.ReleaseObjectPinReply
    assert (reply.transfer_id, reply.object_id, reply.node_id) == (
        pin.transfer_id, pin.descriptor.object_id, pin.descriptor.node_id,
    )
    assert reply.accepted


class _Delivery:
    def __init__(self, monkeypatch, *, pin_mode="normal", release_mode="normal"):
        self.f = f = _NodeFixture(monkeypatch, commit=False)
        self.descriptor = f.descriptors[0]
        self.pin_mode, self.release_mode = pin_mode, release_mode
        self.pins, self.chunks, self.closes, self.active_checks = [], [], [], []
        self.release_attempts = 0
        self.recovered = False
        self.source_was_explicitly_dropped = False
        monkeypatch.setattr(node_module, "rpc_request", self.rpc)

    def rpc(self, address, handler, request, **options):
        f = self.f
        assert address == f.source_address and not f.target._state_lock._is_owned()
        if handler == node_module.PIN_OBJECT_HANDLER:
            self.pins.append(request)
            assert len(self.pins) == 1 and request.descriptor == self.descriptor
            pending = _outbox(f.target).pending()
            assert len(pending) == 1 and pending[0].pin == request and pending[0].active
            assert not pending[0].in_flight
            assert not f.target._drive_source_pin_releases(force=True, max_effects=1)
            assert not self.closes and pending[0].active
            self.active_checks.append(request)
            if self.pin_mode == "never-delivered":
                raise TransportTimeout("Pin did not reach the source before delivery failed")
            reply = f.source._handle_pin_object_for_transfer(request)
            if self.pin_mode == "effect-ack-lost":
                assert reply.pinned and f.source.object_store.snapshot(self.descriptor.object_id).pin_count == 1
                raise TransportTimeout("source Pin took effect before ACK loss")
            assert self.pin_mode == "normal"
            return reply
        if handler == node_module.GET_OBJECT_CHUNK_HANDLER:
            self.chunks.append(request)
            assert len(self.chunks) == 1
            assert _outbox(f.target).pending()[0].active
            return f.source._handle_get_object_chunk(request)
        assert handler == node_module.RELEASE_OBJECT_PIN_HANDLER
        assert options["connect_timeout"] == 0.25 and options["request_timeout"] == 0.5
        assert 0 < options["deadline"] - time.monotonic() <= 0.75
        self.release_attempts += 1
        assert self.release_attempts <= 4
        pending = _outbox(f.target).pending()
        assert len(pending) == 1 and not pending[0].active and pending[0].in_flight
        # The foreground reader owns one ticket across its immediate retries;
        # a supervisor interleaving cannot steal it and cause false failure.
        assert not f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert request == pending[0].release == _release(self.pins[0])
        reply = deepcopy(f.source._handle_release_object_pin(request))
        self.closes.append((request, reply))
        _assert_release(reply, self.pins[0])
        if f.source.object_store.contains(self.descriptor.object_id, sealed_only=False):
            assert f.source.object_store.snapshot(self.descriptor.object_id).pin_count == 0
        else:
            assert self.source_was_explicitly_dropped
        if not self.recovered and self.release_attempts <= 3:
            if self.release_mode == "effect-ack-lost":
                raise TransportTimeout("source Release took effect before ACK loss")
            if self.release_mode == "wrong-identity":
                return replace(reply, node_id=NodeID(bytes(value ^ 1 for value in reply.node_id.value)))
        assert self.release_mode in ("normal", "effect-ack-lost", "wrong-identity")
        return reply

    def finish(self):
        # Re-enable only transport delivery, preserving the actual source
        # close fence and target obligation. No state table is erased.
        self.recovered = True
        if _outbox(self.f.target).has_pending():
            self.f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert not _outbox(self.f.target).has_pending()
        if self.f.source.object_store.contains(self.descriptor.object_id, sealed_only=False):
            assert self.f.source.object_store.snapshot(self.descriptor.object_id).pin_count == 0
        else:
            assert self.source_was_explicitly_dropped


def test_pin_effect_ack_loss_closes_the_exact_session_even_without_target_bytes(monkeypatch):
    d = _Delivery(monkeypatch, pin_mode="effect-ack-lost")
    try:
        with pytest.raises(TransportTimeout, match="Pin took effect"):
            d.f.target._localize_one_dependency(d.descriptor)
        assert len(d.pins) == len(d.closes) == len(d.active_checks) == 1 and not d.chunks
        assert d.closes[0][1].released
        pin = d.pins[0]
        assert d.f.source._closed_transfer_pins[pin.transfer_id].closed
        assert d.f.source._closed_transfer_pins[pin.transfer_id].request == _release(pin)
        assert d.f.source._pinned_transfers[pin.transfer_id].released
        assert not d.f.target.object_store.contains(d.descriptor.object_id, sealed_only=False)
        assert not _outbox(d.f.target).has_pending() and _clean(d.f.target) and _clean(d.f.source)
    finally:
        d.finish()


def test_release_before_undelivered_pin_fences_the_later_actual_pin(monkeypatch):
    d = _Delivery(monkeypatch, pin_mode="never-delivered")
    try:
        with pytest.raises(TransportTimeout, match="did not reach"):
            d.f.target._localize_one_dependency(d.descriptor)
        assert len(d.pins) == len(d.closes) == 1 and not d.closes[0][1].released and not d.chunks
        pin = d.pins[0]
        assert pin.transfer_id not in d.f.source._pinned_transfers
        closed = deepcopy(d.f.source._closed_transfer_pins[pin.transfer_id])
        assert closed.closed and closed.request == _release(pin)
        late = d.f.source._handle_pin_object_for_transfer(pin)
        assert not late.pinned and late.transfer_id == pin.transfer_id
        assert d.f.source._closed_transfer_pins[pin.transfer_id] == closed
        assert d.f.source.object_store.snapshot(d.descriptor.object_id).pin_count == 0
        assert pin.transfer_id not in d.f.source._pinned_transfers
        assert _clean(d.f.target) and _clean(d.f.source)
    finally:
        d.finish()


def test_source_typed_pin_rejection_still_closes_the_attempt_without_reading_chunks(monkeypatch):
    d = _Delivery(monkeypatch)
    try:
        item = d.descriptor
        dropped = d.f.source._handle_drop_object_replica(protocol.DropObjectReplica(
            item.object_id, item.producer_attempt_id, item.owner_worker_id, item.node_id, item.checksum,
        ))
        assert dropped.status is protocol.DropObjectReplicaStatus.DROPPED
        d.source_was_explicitly_dropped = True
        with pytest.raises(RuntimeError, match="fenced by replica deletion"):
            d.f.target._localize_one_dependency(item)
        assert len(d.pins) == len(d.closes) == 1 and not d.chunks
        assert d.closes[0][1].accepted and not d.closes[0][1].released
        pin = d.pins[0]
        assert pin.transfer_id not in d.f.source._pinned_transfers
        assert d.f.source._closed_transfer_pins[pin.transfer_id].closed
        assert not _outbox(d.f.target).has_pending()
        assert not d.f.target.object_store.contains(item.object_id, sealed_only=False)
        assert _clean(d.f.target) and _clean(d.f.source)
    finally:
        d.finish()


@pytest.mark.parametrize("release_mode", ("effect-ack-lost", "wrong-identity"))
def test_release_unknown_delivery_keeps_outbox_and_drain_unclean_until_exact_replay(monkeypatch, release_mode):
    d = _Delivery(monkeypatch, release_mode=release_mode)
    try:
        with pytest.raises(RuntimeError, match="source pin release failed"):
            d.f.target._localize_one_dependency(d.descriptor)
        assert len(d.pins) == len(d.chunks) == 1 and len(d.closes) == 3
        assert [reply.released for _, reply in d.closes] == [True, False, False]
        assert d.f.target.object_store.get(d.descriptor.object_id) == d.f.payloads[0]
        assert d.f.source.object_store.snapshot(d.descriptor.object_id).pin_count == 0
        pending = _outbox(d.f.target).pending()
        assert len(pending) == 1 and pending[0].rounds == 1
        assert not pending[0].active and not pending[0].in_flight
        assert pending[0].pin == d.pins[0] and pending[0].source_death is None
        assert not _clean(d.f.target), "zero source pins is not target receipt of a release ACK"
        assert _clean(d.f.source)
        d.recovered = True
        assert d.f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert len(d.closes) == 4 and d.closes[-1][0] == d.closes[0][0]
        assert not d.closes[-1][1].released
        assert not _outbox(d.f.target).has_pending() and _clean(d.f.target)
        assert len(d.pins) == len(d.chunks) == 1
    finally:
        d.finish()


def test_source_acquire_effect_then_error_can_only_close_its_recorded_token(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    descriptor = f.descriptors[0]
    pin = protocol.PinObjectForTransfer("acquire-effect-error", descriptor, f.target.node_id)
    original_pin = f.source.object_store.pin
    calls = []

    def apply_then_error(object_id, token=None):
        result = original_pin(object_id, token)
        calls.append((object_id, token, result))
        assert len(calls) == 1
        raise RuntimeError("source acquire committed before error")

    monkeypatch.setattr(f.source.object_store, "pin", apply_then_error)
    try:
        reply = f.source._handle_pin_object_for_transfer(pin)
        assert not reply.pinned and "acquire committed" in reply.error
        assert f.source.object_store.snapshot(descriptor.object_id).pin_count == 1
        session = f.source._pinned_transfers[pin.transfer_id]
        assert session.pin_token == pin.transfer_id and not session.acquired and not session.released
        chunk = f.source._handle_get_object_chunk(protocol.GetObjectChunk(
            pin.transfer_id, descriptor.object_id, pin.requester_node_id, 0, descriptor.size_bytes,
        ))
        assert not chunk.ok and chunk.data == b""
        closed = f.source._handle_release_object_pin(_release(pin))
        _assert_release(closed, pin)
        assert closed.released and session.released and session.closing
        assert f.source.object_store.snapshot(descriptor.object_id).pin_count == 0
        assert _clean(f.source)
    finally:
        monkeypatch.setattr(f.source.object_store, "pin", original_pin)
        assert f.source._handle_release_object_pin(_release(pin)).accepted


def test_source_unpin_effect_then_error_stays_closing_until_supervisor_replay(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    descriptor = f.descriptors[0]
    pin = protocol.PinObjectForTransfer("unpin-effect-error", descriptor, f.target.node_id)
    assert f.source._handle_pin_object_for_transfer(pin).pinned
    original_unpin = f.source.object_store.unpin
    calls = []

    def apply_then_error(object_id, token):
        result = original_unpin(object_id, token)
        calls.append((object_id, token, result))
        if len(calls) == 1:
            raise RuntimeError("source unpin committed before error")
        assert len(calls) == 2
        return result

    monkeypatch.setattr(f.source.object_store, "unpin", apply_then_error)
    try:
        refused = f.source._handle_release_object_pin(_release(pin))
        assert not refused.accepted and not refused.released
        assert f.source.object_store.snapshot(descriptor.object_id).pin_count == 0
        assert not f.source._closed_transfer_pins[pin.transfer_id].closed
        assert f.source._pinned_transfers[pin.transfer_id].closing and not f.source._pinned_transfers[pin.transfer_id].released
        assert not _clean(f.source)
        assert not f.source._handle_pin_object_for_transfer(pin).pinned
        assert f.source._drive_transfer_pins(force=True)
        assert len(calls) == 2 and calls[0][2] and not calls[1][2]
        assert f.source._closed_transfer_pins[pin.transfer_id].closed and _clean(f.source)
        reply = f.source._handle_release_object_pin(_release(pin))
        _assert_release(reply, pin)
        assert not reply.released
    finally:
        monkeypatch.setattr(f.source.object_store, "unpin", original_unpin)
        assert f.source._handle_release_object_pin(_release(pin)).accepted


def test_closed_session_conflicts_cannot_change_fence_or_unpin_another_session(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    descriptor = f.descriptors[0]
    first = protocol.PinObjectForTransfer("closed-identity", descriptor, f.target.node_id)
    second = replace(first, transfer_id="independent-reader")
    try:
        assert f.source._handle_pin_object_for_transfer(first).pinned
        assert f.source._handle_pin_object_for_transfer(second).pinned
        assert f.source.object_store.snapshot(descriptor.object_id).pin_count == 2
        first_close = f.source._handle_release_object_pin(_release(first))
        assert first_close.accepted and first_close.released
        fence = deepcopy(f.source._closed_transfer_pins[first.transfer_id])
        wrong_requester = NodeID(bytes(value ^ 1 for value in f.target.node_id.value))
        bad = replace(_release(first), requester_node_id=wrong_requester)
        rejected = f.source._handle_release_object_pin(bad)
        assert not rejected.accepted and not rejected.released
        assert f.source._closed_transfer_pins[first.transfer_id] == fence
        assert not f.source._handle_pin_object_for_transfer(first).pinned
        assert not f.source._handle_pin_object_for_transfer(replace(first, requester_node_id=wrong_requester)).pinned
        assert f.source.object_store.snapshot(descriptor.object_id).pin_count == 1
        assert f.source._handle_pin_object_for_transfer(second).pinned
        chunk = f.source._handle_get_object_chunk(protocol.GetObjectChunk(
            second.transfer_id, descriptor.object_id, second.requester_node_id, 0, descriptor.size_bytes,
        ))
        assert chunk.ok and chunk.data == f.payloads[0]
        assert not _clean(f.source)
        second_close = f.source._handle_release_object_pin(_release(second))
        assert second_close.accepted and second_close.released
        assert f.source.object_store.snapshot(descriptor.object_id).pin_count == 0 and _clean(f.source)
    finally:
        assert f.source._handle_release_object_pin(_release(first)).accepted
        assert f.source._handle_release_object_pin(_release(second)).accepted


def test_active_target_reader_is_not_released_by_forced_background_progress(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    descriptor = f.descriptors[0]
    pin = protocol.PinObjectForTransfer("active-background-fence", descriptor, f.target.node_id)
    box = _outbox(f.target)
    record = box.begin(f.source_address, pin)
    releases = []

    def release_rpc(address, handler, request, **options):
        assert address == f.source_address and handler == node_module.RELEASE_OBJECT_PIN_HANDLER
        releases.append(request)
        assert len(releases) == 1 and request == _release(pin)
        return f.source._handle_release_object_pin(request)

    monkeypatch.setattr(node_module, "rpc_request", release_rpc)
    try:
        assert f.source._handle_pin_object_for_transfer(pin).pinned
        assert not f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert not f.target._drive_transfer_pins(force=True)
        assert box.pending() == (record,) and record.active and not record.in_flight and not releases
        assert f.source.object_store.snapshot(descriptor.object_id).pin_count == 1
        assert not _clean(f.target)
        box.close(pin.transfer_id)
        assert f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert len(releases) == 1 and not box.has_pending()
        assert f.source.object_store.snapshot(descriptor.object_id).pin_count == 0
        assert _clean(f.target) and _clean(f.source)
    finally:
        if box.has_pending():
            box.close(pin.transfer_id)
            f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert f.source._handle_release_object_pin(_release(pin)).accepted
