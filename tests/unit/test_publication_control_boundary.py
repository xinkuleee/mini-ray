"""Pure base owner admission and output-cleanup callback boundaries.

One accepted Core task and one child owner. Synchronous reentry exercises the
real driver ticket; it is not a real-thread schedule. No GCS publication
controller, runtime constructors, process, socket, timer, wait or user code.
"""

import pytest

from miniray import output_protocol as wire
from miniray.output_handoff import OutputHandoffTable
from miniray.ownership import ObjectState
from tests.unit.test_common_cleanup_progress import _CorePublication, _no_runtime


pytestmark = pytest.mark.unit


def test_reentrant_node_progress_cannot_repeat_the_inflight_child_effect():
    f = _CorePublication()
    try:
        f.known_complete()
        before = f.core.owner_table.snapshot(f.ref.object_id)
        nested = []
        def release(address, handler, request):
            assert not f.core._state_lock._is_owned()
            reply = f.release(address, handler, request)
            assert f.identity in f.core._output_loss_drivers
            calls = tuple(f.releases)
            assert not f.loss()
            nested.append(request)
            assert tuple(f.releases) == calls
            assert f.core.owner_table.snapshot(f.ref.object_id) == before
            return reply
        f.core._borrow_rpc = release
        assert f.loss()
        assert len(nested) == len(f.releases) == 2
        assert f.releases[0].hold == f.transfer.final_hold
        assert f.releases[1].hold == f.transfer.provisional_hold
        assert not f.child_table.snapshot(f.child).contained_holds
        assert f.core.owner_table.snapshot(f.ref.object_id).state is ObjectState.LOST
        assert not f.core._output_loss_drivers and not f.core._output_node_cleanup
        assert not f.core._protocol_unresolved
        assert f.loss() and len(f.releases) == 2
    finally:
        f.close()


def test_owner_registration_checks_pending_attempt_death_and_manifest_in_one_lock_cut(monkeypatch):
    f = _CorePublication()
    try:
        # A fresh owner table makes this the first registration under the same
        # real accepted task, rather than merely a reducer's cached replay.
        f.core._output_handoffs = OutputHandoffTable()
        table = f.core._output_handoff_table()
        owner_snapshot = f.core.owner_table.snapshot
        is_dead = f.core._node_is_dead
        register = table.register
        seen = []
        def snapshot(object_id):
            assert f.core._state_lock._is_owned()
            seen.append("pending-attempt")
            return owner_snapshot(object_id)
        def node_dead(node_id):
            assert f.core._state_lock._is_owned() and node_id == f.publisher
            seen.append("publisher-death")
            return is_dead(node_id)
        def registered(manifest, attempt):
            assert f.core._state_lock._is_owned()
            assert f.core._task_finish_barriers[f.ref.object_id] == f.pending
            seen.append("manifest")
            return register(manifest, attempt)
        monkeypatch.setattr(f.core.owner_table, "snapshot", snapshot)
        monkeypatch.setattr(f.core, "_node_is_dead", node_dead)
        monkeypatch.setattr(table, "register", registered)
        request = wire.RegisterOutputHandoff(f.manifest)
        first = f.core.register_output_handoff(request)
        replay = f.core.register_output_handoff(request)
        assert first.accepted and replay == first
        assert seen == ["pending-attempt", "publisher-death", "manifest"] * 2
        assert not f.core._state_lock._is_owned()
        assert not f.releases and table.query(f.identity).manifest == f.manifest
        monkeypatch.setattr(f.core.owner_table, "snapshot", owner_snapshot)
    finally:
        f.close()
