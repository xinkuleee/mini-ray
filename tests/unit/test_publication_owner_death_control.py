"""Bounded base membership fences and owner-death cleanup acknowledgements.

Two synchronous tests preserve common invariants from the archived GCS
publication consumer. One uses real membership/fence reducers with two owners;
the other uses one completed Core/Node output and two real child-owner tables.
No runtime constructors, threads, processes, sockets, sleeps or waits run.
Global publication worksets/graphs and the two live-thread cases remain explicit
E migration gaps in the K3 owner-death-consumer-map audit note.
"""

import threading

import pytest

from miniray import control, output_protocol as wire, protocol
from miniray.errors import ProtocolError
from miniray.ids import NodeID, WorkerID
from miniray.output_publication import OutputPublicationConflictError
from miniray.output_publication_journal import OutputPublicationJournalState
from miniray.owner_death_fence_registry import OwnerDeathFenceRegistry
from miniray.resources import ResourceVector
from miniray.trace import MemoryEventSink
from tests.unit.test_common_cleanup_progress import _no_runtime
from tests.unit.test_core_output_publication import _fixture, _close


pytestmark = pytest.mark.unit


def _membership_service():
    """Actual GCS methods and metadata authorities; no GCS/TCP constructor."""
    service = object.__new__(control.GCSLite)
    service._owner_death_control_lock = threading.RLock()
    service.owner_death_fences = OwnerDeathFenceRegistry()
    service.nodes = control.NodeRegistry(scheduling_visible=lambda node_id: node_id in (
        service.owner_death_fences.cleanup_safe_node_ids()
    ))
    service.workers = control.WorkerRegistry(service.nodes)
    service.event_sink = MemoryEventSink()
    service._owner_fence_rpc = lambda *_args: pytest.fail("uninstalled fence transport")
    return service


def test_worker_death_report_replays_without_rpc_and_scoped_fence_progress_preserves_other_owner():
    service = _membership_service()
    node = service.register_node(protocol.RegisterNode(
        NodeID(b"n" * 16), 4101, ("node.invalid", 1234), ResourceVector({"CPU": 1}),
    ))
    assert node.accepted
    requests, deaths = [], []
    for index, worker_id in enumerate((WorkerID(b"a" * 16), WorkerID(b"b" * 16))):
        incarnation = protocol.WorkerIncarnation(
            node.node_id, node.node_pid, node.registration_epoch, worker_id, 5101 + index,
        )
        assert service.register_worker_incarnation(protocol.RegisterWorkerIncarnation(incarnation)).accepted
        requests.append(protocol.ReportWorkerDeath(
            "owner-exit-{}".format(index), incarnation, 1, protocol.WorkerDeathReason.PROCESS_EXIT,
        ))

    calls = []

    def rpc(address, handler, request):
        assert not service._owner_fence_lock()._is_owned()
        assert address == ("node.invalid", 1234)
        assert handler == control.INSTALL_OWNER_DEATH_FENCE_HANDLER
        assert request.scope is protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP
        assert service.get_worker_state(protocol.GetWorkerState(request.owner_worker_id)).death == request.owner_death
        calls.append(request)
        assert len(calls) <= 2
        # A typed Node boundary acknowledgement, not a simulated process exit
        # or a claim that this metadata-only fixture deleted physical replicas.
        return protocol.InstallOwnerDeathFenceReply(request, protocol.OwnerDeathFenceDisposition.FENCED)

    service._owner_fence_rpc = rpc
    for request in requests:
        first = service.report_worker_death(request)
        pending = service.owner_death_fences.pending_for_owner(request.worker_id)
        replay = service.report_worker_death(request)
        assert first.disposition is protocol.WorkerDeathDisposition.APPLIED
        assert replay.disposition is protocol.WorkerDeathDisposition.ALREADY_DEAD
        assert replay.death == first.death
        assert len(pending) == 1 and service.owner_death_fences.pending_for_owner(request.worker_id) == pending
        deaths.append(first.death)
    assert calls == []
    assert service._owner_death_progress_thread is None
    assert service.get_nodes().nodes == ()
    registry = service.owner_death_fences
    first_effect, = registry.pending_for_owner(deaths[0].worker_id)
    second_before = registry.pending_for_owner(deaths[1].worker_id)
    assert service._drive_owner_death_fence(first_effect)
    assert registry.pending_for_owner(deaths[0].worker_id) == ()
    assert registry.pending_for_owner(deaths[1].worker_id) == second_before
    assert calls == [first_effect.request] and service.get_nodes().nodes == ()
    assert service.report_worker_death(requests[0]).death == deaths[0]
    assert registry.pending_for_owner(deaths[0].worker_id) == ()

    drain = protocol.DrainOwnerDeathFences("remaining-owner-fence")
    terminal = service.drain_owner_death_fences(drain)
    assert terminal.clean and terminal.active_fences == 0 and terminal.request_id == drain.request_id
    assert calls == [first_effect.request, second_before[0].request]
    assert not registry.has_active_operations()
    assert tuple(value.node_id for value in service.get_nodes().nodes) == (node.node_id,)
    assert service.drain_owner_death_fences(drain) == terminal
    assert len(calls) == 2 and service._owner_death_progress_thread is None


def test_child_ack_loss_then_invalid_worker_finalize_preserves_exact_owner_cleanup():
    fixture, node, core, pending, task_reply, _calls, _rpc = _fixture(refs=True, stored=False)
    try:
        values = fixture.values
        incarnation = fixture.manifest.header.node_incarnation
        death = protocol.WorkerDeathRecord(
            "completed-output-owner-exit",
            protocol.WorkerIncarnation(
                incarnation.node_id, incarnation.node_pid, incarnation.registration_epoch,
                values.owner, 1901,
            ), 1, 5, protocol.WorkerDeathReason.PROCESS_EXIT,
        )
        request = wire.FinalizeOutputOwnerDeath(fixture.manifest, death)
        journal_before = fixture.journal.snapshot(fixture.id)
        owner_before = core.owner_table.snapshot(pending.object_id)
        handoff_before = fixture.handoffs.query(fixture.id)
        assert journal_before.complete == task_reply.output_publication.complete
        with pytest.raises(OutputPublicationConflictError, match="exact installed owner fence"):
            node._handle_finalize_output_owner_death(request)
        assert fixture.journal.snapshot(fixture.id) == journal_before
        fence = node._handle_install_owner_death_fence(protocol.InstallOwnerDeathFence(
            "completed-output-owner-sweep", death, node.node_id,
            scope=protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP,
        ))
        assert fence.accepted and fence.complete
        assert node._owner_death_fences[values.owner] == death
        assert fixture.journal.snapshot(fixture.id) == journal_before

        releases, finalizations = [], []
        expected = tuple(
            protocol.ReleaseContainedReference(
                transfer.contained_object_id, transfer.contained_owner_worker_id,
                transfer.final_hold if final else transfer.provisional_hold,
            )
            for final in (True, False)
            for transfer in (fixture.manifest.value).transfers
        )
        assert len(expected) == 4

        def rpc(address, handler, message):
            assert not node._state_lock._is_owned()
            assert not fixture.journal._lock._is_owned()
            assert not fixture.adapter._lock._is_owned()
            assert node._owner_death_fences[values.owner] == death
            if handler == "release_contained_reference":
                releases.append(message)
                assert len(releases) <= 5
                result = fixture.release_child(address, message)
                if len(releases) == 1:
                    assert result.released
                    raise TimeoutError("child released; exact ACK lost")
                return result
            assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER
            assert address == node._workers[values.executor].address and message == request
            assert tuple(releases) == (expected[0],) + expected
            for transfer in (fixture.manifest.value).transfers:
                child = fixture.child_owners[transfer.contained_owner_worker_id]
                assert transfer.final_hold not in child.snapshot(transfer.contained_object_id).contained_holds
                assert child.contained_release_was_seen(transfer.contained_object_id, transfer.final_hold)
                assert child.contained_release_was_seen(transfer.contained_object_id, transfer.provisional_hold)
            finalizations.append(message)
            assert len(finalizations) <= 2
            # Worker custody is a typed acknowledgement boundary here; the
            # actual Node adapter, child tables and journal remain authoritative.
            result = wire.FinalizeOutputOwnerDeathReply(message, True)
            if len(finalizations) == 1:
                object.__setattr__(result, "cleaned", 1)
            return result

        node._background_rpc = rpc
        with pytest.raises(TimeoutError, match="exact ACK lost"):
            node._handle_finalize_output_owner_death(request)
        assert releases == [expected[0]] and not finalizations
        assert fixture.adapter._owner_cleanup_acks == {}
        assert fixture.journal.snapshot(fixture.id).result_retained is True
        assert not fixture.adapter.owner_death_finished(fixture.id)
        assert not node._handle_prepare_output_publication(wire.PrepareOutputPublication(
            fixture.manifest, (values.payload),
        )).accepted

        with pytest.raises(ProtocolError, match="owner cleanup flag"):
            node._handle_finalize_output_owner_death(request)
        assert tuple(releases) == (expected[0],) + expected
        assert finalizations == [request]
        assert len(fixture.adapter._owner_cleanup_acks) == 4
        unresolved = fixture.journal.snapshot(fixture.id)
        assert unresolved.complete == journal_before.complete and unresolved.result_retained is True
        assert not fixture.adapter.owner_death_finished(fixture.id)
        assert not fixture.adapter._tickets

        terminal = node._handle_finalize_output_owner_death(request)
        assert terminal.cleaned and finalizations == [request, request]
        assert tuple(releases) == (expected[0],) + expected
        snapshot = fixture.journal.snapshot(fixture.id)
        assert snapshot.state is OutputPublicationJournalState.RETIRED
        assert snapshot.complete == journal_before.complete and not snapshot.result_retained
        assert snapshot.rollback is None and snapshot.rollback_tombstone is None
        assert fixture.adapter.owner_death_finished(fixture.id)
        assert fixture.ledger.available == ResourceVector({"CPU": 1})
        assert node._leases[fixture.id.lease_id].state is protocol.LeaseExecutionState.COMPLETED
        fixture.assert_no_pins_or_bytes()
        assert core.owner_table.snapshot(pending.object_id) == owner_before
        assert fixture.handoffs.query(fixture.id) == handoff_before
        assert node._handle_finalize_output_owner_death(request) == terminal
        assert node._drive_output_publications()
        assert len(releases) == 5 and len(finalizations) == 2
    finally:
        _close(core)
