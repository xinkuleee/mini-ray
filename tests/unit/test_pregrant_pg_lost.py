"""Pure dispatcher routing for an already retained capacity-wait lease.

Two threadless Cores, two 1 KiB stores and two tiny inputs use the actual
pregrant fixture. One empty, never-Pushed probe occupies the sole target Worker;
six real pin/chunk/release calls then produce a PENDING_CAPACITY response and
an exact delayed ReadyTask for the consumer. The test feeds only that item and
STOP through the dispatcher, synchronously, with an execution-boundary spy.

The initial PG-loss gate is an injected routing seam, not a fabricated PG
creation/death record. This tests that retained lease work reaches _execute
instead of the pre-lease PG shortcut; it does not claim PG end-to-end recovery
or completed custody. No thread, socket, process, wait or user function runs.
"""

import queue

import pytest

from miniray import protocol
from miniray.core import _DelayedReadyTask, _ReadyTask, _STOP
from miniray.errors import PlacementGroupLostError
from miniray.ownership import ObjectState
from tests.unit.test_core_output_surviving_replica import _no_runtime as _no_runtime
from tests.unit.test_pregrant_dependency_custody import _Fixture


pytestmark = pytest.mark.unit


def test_dispatch_routes_retained_lease_without_initial_pg_lost_shortcut(monkeypatch):
    f = _Fixture(monkeypatch, "busy-slot")
    try:
        core = f.consumer
        core._capacity_retry_rounds = 3
        assert not core._execute(f.pending, f.pending.spec, f.sources, lease_state=f.lease_state)
        assert len(f.requests) == 1 and len(f.transfers) == 6
        assert f.requests[0][1].reason is protocol.LeaseRejectReason.PENDING_CAPACITY
        assert not f.cancels and not f.acks and not f.reports and not f.releases
        assert core._submissions.qsize() == 1
        delayed = core._submissions.get_nowait()
        core._submissions.task_done()
        assert type(delayed) is _DelayedReadyTask and type(delayed.ready) is _ReadyTask
        ready = delayed.ready
        assert ready.lease_state == f.lease_state and ready.lease_state.request == f.request
        assert ready.pending.capacity_round == 1 and ready.pending.spec == f.pending.spec
        assert ready.pending.dependency_hold == f.local_hold
        assert ready.dependencies == f.sources and ready.ambiguity_round == 0
        assert ready.cancellation is ready.push_state is ready.location_state is None
        assert ready.output_adoption is ready.output_node_loss is None
        inventory = f.registry().snapshot(f.request.lease_id)
        assert inventory.descriptors == f.expected_descriptors() and f.registry().has_pending()
        before_local = core.owner_table.snapshot(f.local_id)
        before_foreign = f.foreign.owner_table.snapshot(f.foreign_id)
        before_output = core.owner_table.snapshot(f.pending.object_id)
        before_io = tuple(f.requests), tuple(f.cancels), tuple(f.acks), tuple(f.reports), tuple(f.transfers)
        initial_gate_calls, execution_calls = [], []

        def initial_pg_gate(pending):
            initial_gate_calls.append(pending)
            raise PlacementGroupLostError("initial admission gate must not retire a retained lease")

        def execute(pending, prepared, dependencies, **kwargs):
            assert not initial_gate_calls
            assert pending is ready.pending and prepared is ready.spec and dependencies == f.sources
            assert kwargs == {
                "lease_state": ready.lease_state, "ambiguity_round": 0,
                "push_state": None, "location_state": None,
                "output_adoption": None, "output_node_loss": None, "system_failure": None,
            }
            assert f.registry().snapshot(f.request.lease_id) == inventory
            execution_calls.append(pending)
            assert len(execution_calls) == 1
            return False  # The custody-aware execution lane owns later convergence.

        def forbidden_finish(*_args, **_kwargs):
            pytest.fail("dispatcher prematurely finalized retained pre-grant custody")

        monkeypatch.setattr(core, "_raise_if_placement_group_lost", initial_pg_gate)
        monkeypatch.setattr(core, "_execute", execute)
        monkeypatch.setattr(core, "_publish_task_error", forbidden_finish)
        monkeypatch.setattr(core, "_finish_pending_task", forbidden_finish)
        core._ready_tasks = queue.Queue()
        core._ready_tasks.put_nowait(ready)
        core._ready_tasks.put_nowait(_STOP)
        # Both queue entries are already present. The imported tripwires make
        # an unexpected third dequeue fail at Condition.wait instead of hang.
        core._dispatch_loop()
        assert not initial_gate_calls and execution_calls == [ready.pending]
        assert core._ready_tasks.empty() and core._ready_tasks.unfinished_tasks == 0
        assert core._submissions.empty()
        assert (tuple(f.requests), tuple(f.cancels), tuple(f.acks), tuple(f.reports), tuple(f.transfers)) == before_io
        assert core.owner_table.snapshot(f.local_id) == before_local
        assert f.foreign.owner_table.snapshot(f.foreign_id) == before_foreign
        assert core.owner_table.snapshot(f.pending.object_id) == before_output
        assert before_output.state is ObjectState.PENDING
        assert f.local_hold in before_local.submitted_tokens
        assert f.foreign.owner_table.has_retained_reference_for_task(f.foreign_id, f.foreign_hold)
        assert f.registry().snapshot(f.request.lease_id) == inventory and f.registry().has_pending()
        assert f.request.lease_id not in f.target._leases
        assert f.target._leases[f.probe.lease_id].state is protocol.LeaseExecutionState.GRANTED
        for item, payload in zip(f.sources, f.payloads):
            assert f.target.object_store.get(item.object_id) == payload
            assert f.target.object_store.snapshot(item.object_id).pin_count == 0
        assert not f.drops and not f.releases
    finally:
        # Release the one real probe and local handles through fixture cleanup.
        # Leave unresolved consumer inventory intact: this is a routing test,
        # not a forged clean-shutdown/custody acknowledgement.
        f.close()
