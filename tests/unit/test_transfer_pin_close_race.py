"""Pure foreground-close/background-drive interleaving at the lock cut.

Two passive Nodes, two 1 KiB stores and one actual source transfer pin. The
target's close helper releases its real state RLock, then a one-shot synchronous
hook tries the background driver before the foreground sends any Release.
The same foreground ticket must already cover the whole close attempt.

One background attempt and one actual Release ACK; no fabricated reply, Core,
GCS, thread, process, socket, timer, wait or user task. The second tiny fixture
object remains untouched; this test does not claim localization or task GC.
"""

import time

import pytest

from miniray import node as node_module, protocol
from tests.unit.test_cancelled_grant_inventory import _NodeFixture, _no_runtime as _no_runtime
from tests.unit.test_lease_cancel_handoff_interleavings import _AfterOutermostUnlock


pytestmark = pytest.mark.unit


def test_foreground_close_claims_ticket_before_background_can_run_at_first_unlock(monkeypatch):
    f = _NodeFixture(monkeypatch, commit=False)
    pin = protocol.PinObjectForTransfer("foreground-close-lock-cut", f.descriptors[0], f.target.node_id)
    release = protocol.ReleaseObjectPin(pin.transfer_id, pin.descriptor.object_id, pin.requester_node_id)
    unlock = None
    try:
        with f.target._state_lock:
            box = f.target._source_pin_outbox_locked()
            record = box.begin(f.source_address, pin)
        source_reply = f.source._handle_pin_object_for_transfer(pin)
        assert source_reply.pinned and source_reply.descriptor == pin.descriptor
        assert record.active and not record.in_flight and box.pending() == (record,)
        assert f.source.object_store.snapshot(pin.descriptor.object_id).pin_count == 1
        with f.target._state_lock:
            assert not f.target._cleanup_plane_quiescent_locked()
        releases, background = [], []
        underlying_lock = f.target._state_lock
        unlock = _AfterOutermostUnlock(underlying_lock)
        monkeypatch.setattr(f.target, "_state_lock", unlock)

        def release_rpc(address, handler, request, **kwargs):
            assert address == f.source_address and handler == node_module.RELEASE_OBJECT_PIN_HANDLER
            assert request == release and background == [False]
            assert not underlying_lock._is_owned()
            assert not record.active and record.in_flight and box.pending() == (record,)
            assert kwargs["connect_timeout"] == 0.25 and kwargs["request_timeout"] == 0.5
            assert set(kwargs) == {"connect_timeout", "request_timeout", "deadline"}
            assert 0 < kwargs["deadline"] - time.monotonic() <= 0.75
            reply = f.source._handle_release_object_pin(request)
            releases.append((request, reply))
            assert len(releases) == 1
            assert type(reply) is protocol.ReleaseObjectPinReply and reply.accepted and reply.released
            assert (reply.transfer_id, reply.object_id, reply.node_id) == (
                pin.transfer_id, pin.descriptor.object_id, f.source.node_id,
            )
            return reply

        def try_background_after_close_unlock():
            assert not background and not releases and not underlying_lock._is_owned()
            # These facts distinguish the fixed atomic handoff from a hook
            # installed later inside an already claimed foreground RPC.
            assert not record.active and record.in_flight
            assert box.pending() == (record,) and record.rounds == 0
            result = f.target._drive_source_pin_releases(force=True, max_effects=1)
            background.append(result)
            assert result is False and not releases
            assert box.pending() == (record,) and record.in_flight
            assert f.source.object_store.snapshot(pin.descriptor.object_id).pin_count == 1
            with f.target._state_lock:
                assert not f.target._cleanup_plane_quiescent_locked()

        monkeypatch.setattr(node_module, "rpc_request", release_rpc)
        unlock.arm(try_background_after_close_unlock)
        assert f.target._finish_source_pin_read(pin.transfer_id)
        assert unlock.fired == 1 and background == [False] and len(releases) == 1
        assert not record.active and not record.in_flight and record.rounds == 0
        assert not box.has_pending()
        assert f.source._pinned_transfers[pin.transfer_id].released
        assert f.source._closed_transfer_pins[pin.transfer_id].closed
        assert f.source._closed_transfer_pins[pin.transfer_id].request == release
        assert f.source.object_store.snapshot(pin.descriptor.object_id).pin_count == 0
        assert f.source.object_store.get(pin.descriptor.object_id) == f.payloads[0]
        assert not f.target._drive_source_pin_releases(force=True, max_effects=1)
        assert len(releases) == 1
        for node in (f.source, f.target):
            with node._state_lock:
                assert node._cleanup_plane_quiescent_locked()
    finally:
        if unlock is not None:
            unlock.disarm()
        # The source still exists even if a routing assertion fails. Close its
        # exact real pin without erasing target ticket/authority state.
        assert f.source._handle_release_object_pin(release).accepted
