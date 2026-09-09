"""Pure submitting-Core adoption/GC contracts for one stored output.

The historical filename retains no legacy claim/promotion protocol. Fixtures
compose actual Core handoff, Node journal/store and child-owner reducers
synchronously: one tiny output, one 1 KiB store and two child holds.
All RPC is in-memory; no Core/server constructor, process, thread, timer,
socket or blocking wait is allowed. Faults are exact callback boundaries.
"""

from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import replace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import control, output_protocol as wire, protocol
from miniray.core import _DelayedReadyTask, _OutputAdoptionObligation, _PushRequestState
from miniray.errors import ProtocolError, SystemTaskError
from miniray.ids import JobID, LeaseID, NodeID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationEnvelope, OutputPublicationManifest,
)
from miniray.ownership import ObjectState, OutputOwnerPublicationDisposition
from miniray.recovery import FailureKind, RecoveryAction, TaskState
from miniray.transport import TransportTimeout
from tests.unit._pure_core import close_pure_core
from tests.unit.test_core_output_publication import _fixture, _take_adoption


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("Core adoption contract attempted real infrastructure")

    for kind, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for target in (
        "miniray.core.CoreWorker.__init__", "miniray.node.NodeServer.__init__",
        "miniray.worker.WorkerServer.__init__", "miniray.worker.TCPServer.__init__",
        "miniray.core.rpc_request", "miniray.node.rpc_request",
        "miniray.worker.rpc_request", "miniray.transport.request",
    ):
        monkeypatch.setattr(target, forbidden)


@contextmanager
def _case(*, refs=True, report_complete=True):
    values = _fixture(refs=refs, stored=True, report_complete=report_complete)
    core = values[2]
    try:
        yield values
    finally:
        # Release only real local tokens; do not erase failed obligations or
        # reset accepted counts to make the fixture look clean.
        for object_id in tuple(core._objects):
            if core.owner_table.contains(object_id):
                for token in tuple(core.owner_table.snapshot(object_id).local_tokens):
                    core.owner_table.release_local_reference(object_id, token)
        close_pure_core(core)


def _publish(core, pending, reply, node, fixture):
    return core._publish_reply(
        pending, reply, expected_node_id=node.node_id,
        expected_lease_id=fixture.id.lease_id,
    )


def _delayed_adoption(core):
    return next(item.ready.output_adoption for item in tuple(core._submissions.queue)
                if isinstance(item, _DelayedReadyTask) and item.ready.output_adoption is not None)


def _push_state(fixture, node, core, pending):
    record = node._leases[fixture.id.lease_id]
    push = protocol.PushTask(fixture.id.lease_id, fixture.values.executor, pending.spec)
    return _PushRequestState(
        push, record.grant, core.node_address, record.grant.worker_address,
        round=1, ambiguous=True,
    )


def _assert_published(fixture, core, pending):
    snapshot = core.owner_table.snapshot(pending.object_id)
    assert snapshot.state is ObjectState.READY_STORED
    assert snapshot.output_publication.manifest == fixture.manifest
    assert snapshot.outgoing_contained_edges == frozenset(fixture.manifest.slots[0].edges)
    assert core._objects[pending.object_id].event.is_set()
    assert core._stored_descriptors[pending.object_id] == fixture.values.results[0]
    assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
    assert core._recovery.task_record(pending.task_id).retries_started == 0


def test_adoption_orders_complete_atomic_owner_ready_and_payload_ack(monkeypatch):
    with _case(report_complete=False) as (fixture, node, core, pending, reply, _calls, rpc):
        events = []
        original_complete = fixture.handoffs.record_complete
        original_commit, original_wake = core.owner_table.commit_output_publication, core._wake_object
        original_adopt = fixture.handoffs.adopt

        def complete(witness):
            assert core._state_lock._is_owned()
            assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
            assert not core._objects[pending.object_id].event.is_set()
            events.append("complete")
            return original_complete(witness)

        def commit(plan):
            events.append("owner")
            assert fixture.handoffs.query(fixture.id).complete == reply.output_publication.complete
            receipt = original_commit(plan)
            assert receipt.disposition is OutputOwnerPublicationDisposition.APPLIED
            assert core.owner_table.snapshot(pending.object_id).is_ready
            assert not core._objects[pending.object_id].event.is_set()
            return receipt

        def wake(object_id):
            assert object_id == pending.object_id
            assert core.owner_table.snapshot(object_id).is_ready
            assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
            events.append("wake")
            original_wake(object_id)

        def adopt(proof):
            _assert_published(fixture, core, pending)
            events.append("adopted")
            return original_adopt(proof)

        def observe_rpc(address, handler, request):
            assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
            marker = core._protocol_unresolved[pending.task_key]
            assert isinstance(marker.obligation, _OutputAdoptionObligation)
            assert marker.output_candidate == fixture.id
            assert core._task_finish_barriers[pending.object_id] is pending
            _assert_published(fixture, core, pending)
            events.append("node-retired")
            return rpc(address, handler, request)

        monkeypatch.setattr(fixture.handoffs, "record_complete", complete)
        monkeypatch.setattr(core.owner_table, "commit_output_publication", commit)
        monkeypatch.setattr(core, "_wake_object", wake)
        monkeypatch.setattr(fixture.handoffs, "adopt", adopt)
        monkeypatch.setattr(core, "_rpc", observe_rpc)
        assert _publish(core, pending, reply, node, fixture)
        assert events == ["complete", "owner", "wake", "adopted", "node-retired"]
        assert not core._protocol_unresolved
        assert not fixture.journal.snapshot(fixture.id).retained_result_slots
        assert fixture.store.used_bytes > 0
        assert core._finish_pending_task(pending) and core._accepted_task_count == 0


@pytest.mark.parametrize("lost_effect", ("complete", "owner", "wake", "adopted", "node-retired"))
def test_effect_then_error_replays_exact_output_without_second_owner_cas(monkeypatch, lost_effect):
    with _case(report_complete=False) as (fixture, node, core, pending, reply, _calls, rpc):
        injected, dispositions, requests, wakes = [], [], [], []
        original_complete, original_adopt = fixture.handoffs.record_complete, fixture.handoffs.adopt
        original_commit, original_wake = core.owner_table.commit_output_publication, core._wake_object

        def lose_once(stage):
            if stage == lost_effect and not injected:
                injected.append(stage)
                raise TransportTimeout("effect committed; exact acknowledgement lost")

        def complete(witness):
            result = original_complete(witness)
            lose_once("complete")
            return result

        def commit(plan):
            receipt = original_commit(plan)
            dispositions.append(receipt.disposition)
            lose_once("owner")
            return receipt

        def wake(object_id):
            was_ready = core._objects[object_id].event.is_set()
            original_wake(object_id)
            wakes.append(not was_ready)
            lose_once("wake")

        def adopt(proof):
            result = original_adopt(proof)
            lose_once("adopted")
            return result

        def observe_rpc(address, handler, request):
            assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
            requests.append(request)
            result = rpc(address, handler, request)
            lose_once("node-retired")
            return result

        monkeypatch.setattr(fixture.handoffs, "record_complete", complete)
        monkeypatch.setattr(core.owner_table, "commit_output_publication", commit)
        monkeypatch.setattr(core, "_wake_object", wake)
        monkeypatch.setattr(fixture.handoffs, "adopt", adopt)
        monkeypatch.setattr(core, "_rpc", observe_rpc)
        assert not _publish(core, pending, reply, node, fixture)
        assert injected == [lost_effect]
        marker = core._protocol_unresolved[pending.task_key]
        assert marker.output_candidate == fixture.id
        assert isinstance(marker.obligation, _OutputAdoptionObligation)
        assert marker.obligation.envelope == reply.output_publication
        assert core._output_result_custody[fixture.id] == reply.output_publication
        assert not core._finish_pending_task(pending)
        assert core.owner_table.snapshot(pending.object_id).state is not ObjectState.ERROR
        if lost_effect == "node-retired":
            assert not fixture.journal.snapshot(fixture.id).retained_result_slots
        assert core._execute(pending, pending.spec, output_adoption=_take_adoption(core))
        _assert_published(fixture, core, pending)
        assert dispositions == [OutputOwnerPublicationDisposition.APPLIED]
        assert wakes.count(True) == 1
        assert requests == [requests[0]] * (2 if lost_effect == "node-retired" else 1)
        assert not core._protocol_unresolved and fixture.id not in core._output_result_custody
        assert core._finish_pending_task(pending) and core._accepted_task_count == 0


@pytest.mark.parametrize("wrong_kind", ("rebound", "corrupt", "rejected"))
def test_rebound_payload_ack_preserves_finish_obligation(monkeypatch, wrong_kind):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        requests = []
        def wrong_once(address, handler, request):
            assert handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER
            result = rpc(address, handler, request)
            requests.append(request)
            if len(requests) == 1:
                if wrong_kind == "rebound":
                    changed = wire.AckOutputPublicationAdopted(replace(request.proof, owner_commit_id="another-cas"))
                    return wire.AckOutputPublicationAdoptedReply(changed, True)
                if wrong_kind == "corrupt":
                    object.__setattr__(result, "request", object())
                    return result
                return wire.AckOutputPublicationAdoptedReply(
                    request, False, wire.OutputPublicationRPCErrorKind.INVALID_STATE, "injected rejection",
                )
            return result
        monkeypatch.setattr(core, "_rpc", wrong_once)
        assert not _publish(core, pending, reply, node, fixture)
        assert pending.task_key in core._protocol_unresolved
        assert not core._finish_pending_task(pending)
        _assert_published(fixture, core, pending)
        assert not fixture.journal.snapshot(fixture.id).retained_result_slots
        assert core._execute(pending, pending.spec, output_adoption=_take_adoption(core))
        assert requests == [requests[0]] * 2
        assert core._finish_pending_task(pending)


@pytest.mark.parametrize("fault", ("lost", "rebound", "corrupt"))
def test_owner_complete_report_unknown_ack_preserves_node_outbox(monkeypatch, fault):
    with _case(report_complete=False) as (fixture, node, core, pending, reply, _calls, _rpc):
        original = node._background_rpc
        requests = []
        def unknown_ack(address, handler, request):
            result = original(address, handler, request)
            assert handler == wire.REPORT_OUTPUT_HANDOFF_COMPLETE_HANDLER
            requests.append(request)
            if len(requests) == 1:
                if fault == "lost":
                    raise TransportTimeout("owner Complete applied; ACK lost")
                if fault == "rebound":
                    return wire.OutputHandoffReply(wire.GetOutputHandoff(fixture.id), True, result.snapshot)
                object.__setattr__(result, "request", object())
            return result
        monkeypatch.setattr(node, "_background_rpc", unknown_ack)
        assert not node._drive_output_publications()
        assert fixture.handoffs.query(fixture.id).complete == reply.output_publication.complete
        assert fixture.adapter.pending_terminal_reports() == (reply.output_publication.complete,)
        assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
        assert not core._objects[pending.object_id].event.is_set()
        assert core._accepted_task_count == 1
        assert core._task_finish_barriers[pending.object_id] is pending
        assert node._drive_output_publications()
        assert requests == [requests[0]] * 2
        assert fixture.adapter.pending_terminal_reports() == ()
        assert _publish(core, pending, reply, node, fixture)
        assert core._finish_pending_task(pending)


@pytest.mark.parametrize("field", ("lease", "owner", "job", "executor", "node"))
def test_success_envelope_identity_is_checked_before_any_owner_or_node_effect(monkeypatch, field):
    with _case(refs=False) as (fixture, node, core, pending, reply, calls, _rpc):
        envelope = reply.output_publication
        header = envelope.manifest.header
        if field == "lease":
            header = replace(header, publication_id=replace(header.publication_id, lease_id=LeaseID.random()))
        elif field == "owner":
            header = replace(header, owner_worker_id=WorkerID.random())
        elif field == "job":
            header = replace(header, job_id=JobID.random())
        elif field == "executor":
            header = replace(header, executor_worker_id=WorkerID.random())
        else:
            header = replace(header, node_incarnation=replace(header.node_incarnation, node_id=NodeID.random()))
        manifest = OutputPublicationManifest.create(header, envelope.manifest.slots)
        results = tuple(replace(result, owner_worker_id=header.owner_worker_id,
                                node_id=header.node_incarnation.node_id) for result in envelope.results)
        changed = OutputPublicationEnvelope(manifest, OutputPublicationCompleteWitness.for_manifest(manifest), results)
        damaged = replace(reply)
        object.__setattr__(damaged, "results", results)
        object.__setattr__(damaged, "output_publication", changed)
        before = tuple(core.owner_table.snapshot(output) for output in pending.output_ids)
        monkeypatch.setattr(core, "_rpc", lambda *_: pytest.fail("invalid envelope issued remote effects"))
        with pytest.raises((SystemTaskError, ProtocolError)):
            _publish(core, pending, damaged, node, fixture)
        assert tuple(core.owner_table.snapshot(output) for output in pending.output_ids) == before
        assert calls == [] and not core._protocol_unresolved
        assert not core._stored_descriptors
        assert not any(core._objects[output].event.is_set() for output in pending.output_ids)


def test_live_executor_does_not_mask_completed_output_outcome(monkeypatch):
    with _case() as (fixture, node, core, pending, _reply, _calls, rpc):
        state = _push_state(fixture, node, core, pending)
        seen = []

        def outcome_rpc(address, handler, request):
            if handler == "get_worker_lease_outcome":
                outcome = node._handle_get_worker_lease_outcome(request)
                assert outcome.worker_alive and outcome.completion_status is protocol.TaskReplyStatus.SUCCEEDED
                seen.append(outcome)
                return outcome
            return rpc(address, handler, request)

        monkeypatch.setattr(core, "_rpc", outcome_rpc)
        core._mark_protocol_unresolved(pending, "push-outcome", output_candidate=fixture.id)
        assert core._resolve_ambiguous_push_outcome(pending, state)
        assert len(seen) == 1
        _assert_published(fixture, core, pending)
        assert core._finish_pending_task(pending)


def test_first_complete_record_observes_continuous_push_and_finish_fences(monkeypatch):
    with _case(report_complete=False) as (fixture, node, core, pending, reply, _calls, _rpc):
        state = _push_state(fixture, node, core, pending)
        observed = []
        original = fixture.handoffs.record_complete
        def observe(witness):
            marker = core._protocol_unresolved[pending.task_key]
            assert marker.output_candidate == fixture.id
            assert isinstance(marker.obligation, _OutputAdoptionObligation)
            assert marker.obligation.envelope == reply.output_publication
            assert core._task_finish_barriers[pending.object_id].execution == pending.execution
            observed.append(witness)
            return original(witness)
        monkeypatch.setattr(fixture.handoffs, "record_complete", observe)
        monkeypatch.setattr(core, "_push_task_rpc", lambda *_: reply)
        core._mark_protocol_unresolved(pending, "push-replay", output_candidate=fixture.id)
        assert core._replay_push(pending, state)
        assert observed == [reply.output_publication.complete] and not core._protocol_unresolved
        assert core._finish_pending_task(pending)


def test_attempt_replaced_at_complete_boundary_never_overwrites_successor(monkeypatch):
    with _case(report_complete=False) as (fixture, node, core, pending, reply, _calls, _rpc):
        successor = []
        original = fixture.handoffs.record_complete
        def advance(witness):
            result = original(witness)
            if not successor:
                with core._state_lock:
                    transition = core._recovery.validate_task_failure(
                        pending.task_id, pending.spec.attempt_id, FailureKind.SYSTEM,
                    )
                    assert transition.decision.action is RecoveryAction.RETRY_TASK
                    owner_plan = core.owner_table.validate_advance_task_outputs(
                        pending.execution, transition.decision.attempt_id,
                    )
                    core.owner_table.commit_validated_advance_task_outputs(owner_plan)
                    core._recovery.commit_validated_transition(transition)
                    newer = replace(pending, spec=replace(pending.spec, attempt_id=transition.decision.attempt_id))
                    core._clear_protocol_unresolved(pending)
                    core._install_task_finish_barrier_locked(newer)
                    core._enqueue_reconstruction_task(newer)
                    core._mark_protocol_unresolved(newer, "successor-admitted")
                    successor.append((newer, core._protocol_unresolved[newer.task_key]))
            return result
        monkeypatch.setattr(fixture.handoffs, "record_complete", advance)
        monkeypatch.setattr(core.owner_table, "commit_output_publication", lambda *_: pytest.fail("stale owner CAS"))
        monkeypatch.setattr(core, "_wake_object", lambda *_: pytest.fail("stale result wake"))
        assert _publish(core, pending, reply, node, fixture)
        newer, marker = successor[0]
        assert core._protocol_unresolved[newer.task_key] is marker
        assert not _publish(core, pending, reply, node, fixture)
        assert core._submissions.get_nowait() is newer
        core._submissions.task_done()
        assert not core._stored_descriptors and core._submissions.empty()
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.current_attempt == newer.spec.attempt_id and snapshot.state is ObjectState.PENDING
        assert snapshot.output_publication is None and not core._objects[pending.object_id].event.is_set()
        assert core._task_finish_barriers[pending.object_id] is newer
        assert core._accepted_task_count == 1


def test_publisher_death_at_complete_boundary_keeps_exact_output_takeover(monkeypatch):
    with _case(report_complete=False) as (fixture, node, core, pending, reply, _calls, _rpc):
        incarnation = fixture.manifest.header.node_incarnation
        registry = control.NodeRegistry()
        survivor = NodeID(bytes(value ^ 1 for value in node.node_id.value))
        assert registry.register(survivor, ("survivor.invalid", 2), pending.spec.resources, node_pid=1702)
        assert registry.register(node.node_id, core.node_address, pending.spec.resources, node_pid=incarnation.node_pid)
        assert registry.get(node.node_id).registration_epoch == incarnation.registration_epoch
        before_epoch, before_nodes = registry.live_snapshot()
        core._membership_epoch = before_epoch
        core._installed_cluster_snapshot = protocol.InstallClusterSnapshot(before_epoch, "before-publisher-loss", before_nodes)
        original = fixture.handoffs.record_complete
        observed = []
        def complete_then_death(witness):
            result = original(witness)
            report = registry.report_death(protocol.ReportNodeDeath(
                "publisher-at-complete", node.node_id, incarnation.node_pid,
                incarnation.registration_epoch, -9, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed",
            ))
            assert report.disposition is protocol.NodeDeathDisposition.APPLIED
            epoch, live = registry.live_snapshot()
            core.handle_node_death(report.death, protocol.InstallClusterSnapshot(epoch, "after-publisher-loss", live))
            observed.append(report.death)
            return result
        monkeypatch.setattr(fixture.handoffs, "record_complete", complete_then_death)
        monkeypatch.setattr(core.owner_table, "commit_output_publication", lambda *_: pytest.fail("dead publisher owner CAS"))
        monkeypatch.setattr(core, "_wake_object", lambda *_: pytest.fail("dead publisher normal wake"))
        assert not _publish(core, pending, reply, node, fixture)
        assert len(observed) == 1 and core._dead_nodes[node.node_id] == observed[0]
        marker = core._protocol_unresolved[pending.task_key]
        assert marker.output_candidate == fixture.id and marker.obligation.envelope == reply.output_publication
        assert _delayed_adoption(core) == marker.obligation
        assert core._output_result_custody[fixture.id] == reply.output_publication
        assert not core._finish_pending_task(pending)
        snapshot = core.owner_table.snapshot(pending.object_id)
        assert snapshot.state is ObjectState.PENDING and snapshot.output_publication is None
        assert not core._objects[pending.object_id].event.is_set()
        assert core._recovery.task_record(pending.task_id).retries_started == 0


def test_reverse_gc_orders_children_drop_then_metadata_and_preserves_sources(monkeypatch):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        assert _publish(core, pending, reply, node, fixture)
        assert core._finish_pending_task(pending)
        events = []
        original_borrow = core._borrow_rpc
        original_complete = core.owner_table.complete_output_publication_collection
        def release_child(address, handler, request):
            events.append("child")
            return original_borrow(address, handler, request)
        def observe(address, handler, request):
            assert handler == "drop_object_replica"
            events.append("drop")
            return rpc(address, handler, request)
        def metadata(plan):
            events.append("metadata")
            return original_complete(plan)
        monkeypatch.setattr(core, "_borrow_rpc", release_child)
        monkeypatch.setattr(core, "_rpc", observe)
        monkeypatch.setattr(core.owner_table, "complete_output_publication_collection", metadata)
        assert core.owner_table.release_local_reference(pending.object_id, "outer0")
        core._reference_released(pending.object_id)
        assert events == ["child", "child", "drop", "metadata"]
        assert not core._object_gc_obligations and not core.owner_table.contains(pending.object_id)
        assert not core._stored_descriptors and fixture.store.used_bytes == 0
        assert core._recovery.lineage_for_object(pending.object_id) is None
        fixture.assert_no_pins_or_bytes()
        for transfer in fixture.manifest.slots[0].transfers:
            assert fixture.child_owners[transfer.contained_owner_worker_id].snapshot(
                transfer.contained_object_id).local_tokens == frozenset(("source-live",))


@pytest.mark.parametrize("failed_stage", ("child", "drop", "metadata"))
def test_reverse_gc_ack_loss_retains_exact_obligation_and_skips_finished_effects(monkeypatch, failed_stage):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        assert _publish(core, pending, reply, node, fixture)
        assert core._finish_pending_task(pending)
        stored = pending.object_id
        calls, scheduled, injected = defaultdict(list), [], []
        original_borrow = core._borrow_rpc
        original_complete = core.owner_table.complete_output_publication_collection
        def lose_once(stage):
            if stage == failed_stage and not injected:
                injected.append(stage)
                raise TransportTimeout("exact cleanup ACK lost")
        def release_child(address, handler, request):
            calls["child"].append(request)
            result = original_borrow(address, handler, request)
            lose_once("child")
            return result
        def observe(address, handler, request):
            assert handler == "drop_object_replica"
            calls["drop"].append(request)
            result = rpc(address, handler, request)
            lose_once("drop")
            return result
        def metadata(plan):
            calls["metadata"].append(plan)
            lose_once("metadata")
            return original_complete(plan)
        def queue_retry(mailbox, event, delay):
            assert mailbox is core._reference_mailbox and event.object_id == stored
            assert 0 < delay <= 0.25
            scheduled.append(event)
        monkeypatch.setattr(core, "_borrow_rpc", release_child)
        monkeypatch.setattr(core, "_rpc", observe)
        monkeypatch.setattr(core.owner_table, "complete_output_publication_collection", metadata)
        monkeypatch.setattr(core, "_schedule_reference_event", queue_retry)
        assert core.owner_table.release_local_reference(stored, "outer0")
        if failed_stage == "metadata":
            with pytest.raises(TransportTimeout, match="cleanup ACK"):
                core._reference_released(stored)
        else:
            core._reference_released(stored)
        assert injected == [failed_stage]
        obligation = core._object_gc_obligations[stored]
        plan = obligation.output_plan
        assert core.owner_table.contains(stored)
        if failed_stage == "child":
            assert obligation.pending_edges and obligation.pending_drops
            assert calls["drop"] == calls["metadata"] == []
        elif failed_stage == "drop":
            assert not obligation.pending_edges and obligation.pending_drops
            assert calls["metadata"] == []
        else:
            assert not obligation.pending_edges and not obligation.pending_drops
        assert core._retry_gc_obligations_for_shutdown()
        assert not core.owner_table.contains(stored) and stored not in core._object_gc_obligations
        assert not core._stored_descriptors and fixture.store.used_bytes == 0
        assert core.owner_table.output_publication_collection_receipt(plan) is not None
        fixture.assert_no_pins_or_bytes()
        if failed_stage == "child":
            assert len(calls["child"]) == 3 and calls["child"][0] == calls["child"][2]
        else:
            assert len(calls["child"]) == 2
        for stage in ("drop", "metadata"):
            assert len(calls[stage]) == (2 if stage == failed_stage else 1)
            assert all(value == calls[stage][0] for value in calls[stage])
        assert len(scheduled) == (0 if failed_stage == "metadata" else 1)


def test_wrong_child_ack_cannot_admit_replica_drop(monkeypatch):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        assert _publish(core, pending, reply, node, fixture)
        assert core._finish_pending_task(pending)
        stored = pending.object_id
        calls, released = [], []
        original_borrow = core._borrow_rpc
        def release_child(address, handler, request):
            result = original_borrow(address, handler, request)
            released.append(request)
            return object() if len(released) == 1 else result
        def observe(address, handler, request):
            calls.append(handler)
            return rpc(address, handler, request)
        monkeypatch.setattr(core, "_borrow_rpc", release_child)
        monkeypatch.setattr(core, "_rpc", observe)
        monkeypatch.setattr(core, "_schedule_reference_event", lambda *_: None)
        assert core.owner_table.release_local_reference(stored, "outer0")
        core._reference_released(stored)
        obligation = core._object_gc_obligations[stored]
        assert len(obligation.pending_edges) == 1 and obligation.pending_drops
        assert core.owner_table.contains(stored) and fixture.store.used_bytes > 0
        assert calls == [] and len(released) == 2
        assert core._retry_gc_obligations_for_shutdown()
        assert released == [released[0], released[1], released[0]]
        assert calls == ["drop_object_replica"] and not core.owner_table.contains(stored)
        fixture.assert_no_pins_or_bytes()
