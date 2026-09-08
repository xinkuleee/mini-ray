"""Pure submitting-Core adoption/GC contracts for unified output batches.

The historical filename retains no legacy claim/promotion protocol. Existing
fixtures compose actual Core, Node journal/store, GCS recovery and child-owner
reducers synchronously: two tiny output slots and at most four child holds.
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

from miniray import output_protocol as wire, protocol
from miniray.contained_cycle import ContainedGraphTransactionState
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
from tests.unit.test_core_output_publication import _fixture


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
def _case(*, refs=True):
    values = _fixture(refs=refs)
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


def _stage(handler, request):
    if handler == wire.REPORT_OUTPUT_PUBLICATION_HANDLER:
        if type(request) is wire.ReportOutputPublicationTerminal:
            return "terminal"
        if type(request) is wire.ReportOutputPublicationAdopted:
            return "adopted"
        if type(request) is wire.ReportOutputPublicationSlotCollected:
            return "collected"
    return {
        "commit_contained_graph": "graph",
        "release_contained_graph_container": "graph-release",
        "drop_object_replica": "drop",
        wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER: "node-retired",
    }.get(handler, handler)


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
    for index, object_id in enumerate(pending.output_ids):
        snapshot = core.owner_table.snapshot(object_id)
        assert snapshot.state is (ObjectState.READY_INLINE if index == 0 else ObjectState.READY_STORED)
        assert snapshot.output_publication.manifest == fixture.manifest
        assert snapshot.outgoing_contained_edges == frozenset(fixture.manifest.slots[index].edges)
        assert core._objects[object_id].event.is_set()
    assert core._stored_descriptors[pending.output_ids[1]] == fixture.values.results[1]
    assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
    assert core._recovery.task_record(pending.task_id).retries_started == 0


def test_adoption_orders_terminal_graph_atomic_owner_ready_and_metadata_acks(monkeypatch):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        events = []
        original_commit, original_wake = core.owner_table.commit_output_publication, core._wake_object

        def observe_rpc(address, handler, request):
            stage = _stage(handler, request)
            marker = core._protocol_unresolved[pending.task_key]
            assert isinstance(marker.obligation, _OutputAdoptionObligation)
            assert marker.output_candidate == fixture.id
            if stage in ("terminal", "graph"):
                assert all(core.owner_table.snapshot(output).state is ObjectState.PENDING
                           for output in pending.output_ids)
                assert not any(core._objects[output].event.is_set() for output in pending.output_ids)
            else:
                _assert_published(fixture, core, pending)
            events.append(stage)
            return rpc(address, handler, request)

        def commit(plan):
            events.append("owner")
            assert fixture.graph.snapshot().manifests[0].state is ContainedGraphTransactionState.COMMITTED
            receipt = original_commit(plan)
            assert receipt.disposition is OutputOwnerPublicationDisposition.APPLIED
            assert all(core.owner_table.snapshot(output).is_ready for output in pending.output_ids)
            assert not any(core._objects[output].event.is_set() for output in pending.output_ids)
            return receipt

        def wake(object_id):
            assert all(core.owner_table.snapshot(output).is_ready for output in pending.output_ids)
            assert core._recovery.task_record(pending.task_id).state is TaskState.SUCCEEDED
            events.append("wake:{}".format(object_id.return_index))
            original_wake(object_id)

        monkeypatch.setattr(core, "_rpc", observe_rpc)
        monkeypatch.setattr(core.owner_table, "commit_output_publication", commit)
        monkeypatch.setattr(core, "_wake_object", wake)
        assert _publish(core, pending, reply, node, fixture)
        assert events == ["terminal", "graph", "owner", "wake:0", "wake:1", "adopted", "node-retired"]
        assert not core._protocol_unresolved
        assert not fixture.journal.snapshot(fixture.id).retained_result_slots
        assert fixture.store.used_bytes > 0
        assert core._finish_pending_task(pending) and core._accepted_task_count == 0


@pytest.mark.parametrize("lost_effect", ("terminal", "graph", "owner", "wake", "adopted", "node-retired"))
def test_effect_then_lost_ack_replays_exact_batch_without_second_owner_cas(monkeypatch, lost_effect):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        injected = []
        remote_requests, wakes = defaultdict(list), defaultdict(list)
        dispositions = []
        original_commit, original_wake = core.owner_table.commit_output_publication, core._wake_object

        def lose_once(stage):
            if stage == lost_effect and not injected:
                injected.append(stage)
                raise TransportTimeout("effect committed; exact acknowledgement lost")

        def observe_rpc(address, handler, request):
            stage = _stage(handler, request)
            remote_requests[stage].append(request)
            result = rpc(address, handler, request)
            lose_once(stage)
            return result

        def commit(plan):
            receipt = original_commit(plan)
            dispositions.append(receipt.disposition)
            lose_once("owner")
            return receipt

        def wake(object_id):
            was_ready = core._objects[object_id].event.is_set()
            original_wake(object_id)
            wakes[object_id].append(not was_ready)
            lose_once("wake")

        monkeypatch.setattr(core, "_rpc", observe_rpc)
        monkeypatch.setattr(core.owner_table, "commit_output_publication", commit)
        monkeypatch.setattr(core, "_wake_object", wake)
        assert not _publish(core, pending, reply, node, fixture)
        assert injected == [lost_effect]
        marker = core._protocol_unresolved[pending.task_key]
        assert marker.output_candidate == fixture.id
        assert isinstance(marker.obligation, _OutputAdoptionObligation)
        assert marker.obligation.envelope == reply.output_publication
        assert core._output_result_custody[fixture.id] == reply.output_publication
        assert not core._finish_pending_task(pending)
        assert all(core.owner_table.snapshot(output).state is not ObjectState.ERROR for output in pending.output_ids)
        if lost_effect == "node-retired":
            assert not fixture.journal.snapshot(fixture.id).retained_result_slots
        assert core._execute(pending, pending.spec, output_adoption=_delayed_adoption(core))
        _assert_published(fixture, core, pending)
        assert dispositions == [OutputOwnerPublicationDisposition.APPLIED]
        assert all(transitions.count(True) == 1 for transitions in wakes.values())
        assert set(wakes) == set(pending.output_ids)
        assert all(all(request == requests[0] for request in requests) for requests in remote_requests.values())
        if lost_effect in remote_requests:
            assert len(remote_requests[lost_effect]) == 2
        if lost_effect in ("owner", "wake", "adopted", "node-retired"):
            assert len(remote_requests["terminal"]) == len(remote_requests["graph"]) == 1
        assert not core._protocol_unresolved and fixture.id not in core._output_result_custody
        assert core._finish_pending_task(pending) and core._accepted_task_count == 0


@pytest.mark.parametrize("wrong_stage", ("terminal", "graph", "adopted", "node-retired"))
def test_rebound_remote_ack_preserves_obligation_before_following_effect(monkeypatch, wrong_stage):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        seen, wrong = [], []

        def wrong_once(address, handler, request):
            stage = _stage(handler, request)
            result = rpc(address, handler, request)
            seen.append(stage)
            if stage == wrong_stage and not wrong:
                wrong.append(request)
                if stage == "graph":
                    return protocol.ContainedGraphReply(
                        protocol.PrepareContainedGraph(request.manifest), result.receipt,
                    )
                if stage == "node-retired":
                    changed = wire.AckOutputPublicationAdopted(replace(request.proof, owner_commit_id="another-cas"))
                    return wire.AckOutputPublicationAdoptedReply(changed, True)
                # Rebuilding the reply inside Core must reject even an instance
                # whose frozen wire fields were corrupted after construction.
                object.__setattr__(result, "request", object())
            return result

        monkeypatch.setattr(core, "_rpc", wrong_once)
        assert not _publish(core, pending, reply, node, fixture)
        assert wrong and pending.task_key in core._protocol_unresolved
        assert seen[-1] == wrong_stage
        assert not core._finish_pending_task(pending)
        if wrong_stage in ("terminal", "graph"):
            assert all(core.owner_table.snapshot(output).state is ObjectState.PENDING for output in pending.output_ids)
            assert not any(core._objects[output].event.is_set() for output in pending.output_ids)
        assert core._execute(pending, pending.spec, output_adoption=_delayed_adoption(core))
        _assert_published(fixture, core, pending)
        assert core._finish_pending_task(pending)


@pytest.mark.parametrize("field", ("lease", "owner", "job", "executor", "node"))
def test_success_envelope_identity_is_checked_before_any_owner_or_gcs_effect(monkeypatch, field):
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


def test_first_terminal_report_observes_continuous_push_and_finish_fences(monkeypatch):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        state = _push_state(fixture, node, core, pending)
        observed = []

        def observe(address, handler, request):
            if type(request) is wire.ReportOutputPublicationTerminal:
                marker = core._protocol_unresolved[pending.task_key]
                assert marker.output_candidate == fixture.id
                assert isinstance(marker.obligation, _OutputAdoptionObligation)
                assert marker.obligation.envelope == reply.output_publication
                assert all(core._task_finish_barriers[output].execution == pending.execution
                           for output in pending.output_ids)
                observed.append(request)
            return rpc(address, handler, request)

        monkeypatch.setattr(core, "_rpc", observe)
        monkeypatch.setattr(core, "_push_task_rpc", lambda *_: reply)
        core._mark_protocol_unresolved(pending, "push-replay", output_candidate=fixture.id)
        assert core._replay_push(pending, state)
        assert len(observed) == 1 and not core._protocol_unresolved
        assert core._finish_pending_task(pending)


def test_attempt_replaced_during_graph_ack_never_overwrites_successor(monkeypatch):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        successor = []

        def advance(address, handler, request):
            result = rpc(address, handler, request)
            if handler == "commit_contained_graph" and not successor:
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
                    core._mark_protocol_unresolved(newer, "successor-admitted")
                    successor.append((newer, core._protocol_unresolved[newer.task_key]))
            return result

        monkeypatch.setattr(core, "_rpc", advance)
        monkeypatch.setattr(core.owner_table, "commit_output_publication", lambda *_: pytest.fail("stale owner CAS"))
        monkeypatch.setattr(core, "_wake_object", lambda *_: pytest.fail("stale result wake"))
        assert _publish(core, pending, reply, node, fixture)
        newer, marker = successor[0]
        assert core._protocol_unresolved[newer.task_key] is marker
        assert not _publish(core, pending, reply, node, fixture)
        assert not core._stored_descriptors
        for output in pending.output_ids:
            snapshot = core.owner_table.snapshot(output)
            assert snapshot.current_attempt == newer.spec.attempt_id and snapshot.state is ObjectState.PENDING
            assert snapshot.output_publication is None and not core._objects[output].event.is_set()
        assert not any(isinstance(item, _DelayedReadyTask) for item in tuple(core._submissions.queue))


def test_publisher_death_after_graph_ack_keeps_exact_output_takeover(monkeypatch):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        incarnation = fixture.manifest.header.node_incarnation
        death = protocol.NodeDeathRecord(
            "publisher-after-graph", incarnation.node_id, incarnation.node_pid,
            incarnation.registration_epoch, 1, -9, protocol.NodeDeathReason.PROCESS_EXIT, "confirmed",
        )
        events, frozen = [], []

        def observe(address, handler, request):
            events.append(_stage(handler, request))
            result = rpc(address, handler, request)
            if handler == "commit_contained_graph":
                frozen.extend(fixture.recovery.freeze_node_death(death))
                with core._state_lock:
                    core._dead_nodes[node.node_id] = death
            return result

        monkeypatch.setattr(core, "_rpc", observe)
        monkeypatch.setattr(core.owner_table, "commit_output_publication", lambda *_: pytest.fail("dead publisher owner CAS"))
        monkeypatch.setattr(core, "_wake_object", lambda *_: pytest.fail("dead publisher normal wake"))
        assert not _publish(core, pending, reply, node, fixture)
        assert events == ["terminal", "graph"] and len(frozen) == 1
        marker = core._protocol_unresolved[pending.task_key]
        assert marker.output_candidate == fixture.id and marker.obligation.envelope == reply.output_publication
        assert _delayed_adoption(core) == marker.obligation
        assert core._output_result_custody[fixture.id] == reply.output_publication
        assert not core._finish_pending_task(pending)
        for output in pending.output_ids:
            snapshot = core.owner_table.snapshot(output)
            assert snapshot.state is ObjectState.PENDING and snapshot.output_publication is None
            assert not core._objects[output].event.is_set()
        assert core._recovery.task_record(pending.task_id).retries_started == 0


def test_reverse_gc_orders_children_graph_drop_report_and_metadata_without_touching_sibling(monkeypatch):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        assert _publish(core, pending, reply, node, fixture)
        assert core._finish_pending_task(pending)
        stored, sibling = pending.output_ids[1], pending.output_ids[0]
        before_sibling = core.owner_table.snapshot(sibling)
        events = []
        original_borrow = core._borrow_rpc
        original_complete = core.owner_table.complete_output_publication_collection

        def release_child(address, handler, request):
            events.append("child")
            return original_borrow(address, handler, request)

        def observe(address, handler, request):
            events.append(_stage(handler, request))
            return rpc(address, handler, request)

        def metadata(plan, receipt):
            events.append("metadata")
            return original_complete(plan, receipt)

        monkeypatch.setattr(core, "_borrow_rpc", release_child)
        monkeypatch.setattr(core, "_rpc", observe)
        monkeypatch.setattr(core.owner_table, "complete_output_publication_collection", metadata)
        assert core.owner_table.release_local_reference(stored, "outer1")
        core._reference_released(stored)
        assert events == ["child", "child", "graph-release", "drop", "collected", "metadata"]
        assert stored not in core._object_gc_obligations and not core.owner_table.contains(stored)
        assert stored not in core._stored_descriptors and fixture.store.used_bytes == 0
        assert core.owner_table.snapshot(sibling) == before_sibling
        graph = fixture.graph.snapshot()
        assert set(graph.committed_edges) == set(fixture.manifest.slots[0].edges)
        for transfer in fixture.manifest.slots[0].transfers:
            holds = fixture.child_owners[transfer.contained_owner_worker_id].snapshot(transfer.contained_object_id).contained_holds
            assert transfer.final_hold in holds


@pytest.mark.parametrize("failed_stage", ("child", "graph-release", "drop", "collected", "metadata"))
def test_reverse_gc_ack_loss_retains_exact_obligation_and_skips_finished_effects(monkeypatch, failed_stage):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        assert _publish(core, pending, reply, node, fixture)
        assert core._finish_pending_task(pending)
        stored, sibling = pending.output_ids[1], pending.output_ids[0]
        before_sibling = core.owner_table.snapshot(sibling)
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
            stage = _stage(handler, request)
            calls[stage].append(request)
            result = rpc(address, handler, request)
            lose_once(stage)
            return result

        def metadata(plan, receipt):
            calls["metadata"].append((plan, receipt))
            # Preserve the old metadata failure contract: fail before local
            # collection, after all remote effects have exact acknowledgements.
            lose_once("metadata")
            return original_complete(plan, receipt)

        def queue_retry(mailbox, event, delay):
            assert mailbox is core._reference_mailbox and event.object_id == stored
            assert 0 < delay <= 0.25
            scheduled.append(event)

        monkeypatch.setattr(core, "_borrow_rpc", release_child)
        monkeypatch.setattr(core, "_rpc", observe)
        monkeypatch.setattr(core.owner_table, "complete_output_publication_collection", metadata)
        monkeypatch.setattr(core, "_schedule_reference_event", queue_retry)
        assert core.owner_table.release_local_reference(stored, "outer1")
        if failed_stage == "metadata":
            with pytest.raises(TransportTimeout, match="cleanup ACK"):
                core._reference_released(stored)
        else:
            core._reference_released(stored)
        assert injected == [failed_stage]
        obligation = core._object_gc_obligations[stored]
        plan = obligation.output_plan
        assert core.owner_table.contains(stored)
        assert core.owner_table.snapshot(sibling) == before_sibling
        if failed_stage in ("child", "graph-release"):
            assert obligation.graph_release_receipt is None and obligation.pending_drops
            assert calls["drop"] == calls["collected"] == calls["metadata"] == []
        elif failed_stage == "drop":
            assert obligation.graph_release_receipt is not None and obligation.pending_drops
            assert calls["collected"] == calls["metadata"] == []
        else:
            assert not obligation.pending_edges and not obligation.pending_drops
            assert obligation.output_cleanup_reported is (failed_stage == "metadata")
        if failed_stage == "graph-release":
            assert not obligation.pending_edges and len(calls["child"]) == 2
        assert core._retry_gc_obligations_for_shutdown()
        assert not core.owner_table.contains(stored) and stored not in core._object_gc_obligations
        assert stored not in core._stored_descriptors and fixture.store.used_bytes == 0
        assert core.owner_table.snapshot(sibling) == before_sibling
        assert core.owner_table.output_publication_collection_receipt(plan) is not None
        if failed_stage == "child":
            assert len(calls["child"]) == 3
            assert calls["child"][0] == calls["child"][2]
        else:
            assert len(calls["child"]) == 2
            assert len(calls[failed_stage]) == 2 and calls[failed_stage][0] == calls[failed_stage][1]
        for stage in ("graph-release", "drop", "collected", "metadata"):
            assert len(calls[stage]) == (2 if stage == failed_stage else 1)
        assert len(scheduled) == (0 if failed_stage == "metadata" else 1)


def test_release_graph_wrong_ack_cannot_admit_replica_drop(monkeypatch):
    with _case() as (fixture, node, core, pending, reply, _calls, rpc):
        assert _publish(core, pending, reply, node, fixture)
        assert core._finish_pending_task(pending)
        stored = pending.output_ids[1]
        calls, released = [], []
        original_borrow = core._borrow_rpc

        def release_child(address, handler, request):
            released.append(request)
            return original_borrow(address, handler, request)

        def wrong_graph_ack(address, handler, request):
            calls.append(handler)
            result = rpc(address, handler, request)
            if handler == "release_contained_graph_container":
                return object()
            return result

        monkeypatch.setattr(core, "_borrow_rpc", release_child)
        monkeypatch.setattr(core, "_rpc", wrong_graph_ack)
        monkeypatch.setattr(core, "_schedule_reference_event", lambda *_: None)
        core.owner_table.release_local_reference(stored, "outer1")
        core._reference_released(stored)
        obligation = core._object_gc_obligations[stored]
        assert not obligation.pending_edges and obligation.graph_release_receipt is None
        assert obligation.pending_drops and core.owner_table.contains(stored)
        assert calls == ["release_contained_graph_container"] and len(released) == 2
        monkeypatch.setattr(core, "_rpc", rpc)
        assert core._retry_gc_obligations_for_shutdown()
        assert len(released) == 2 and not core.owner_table.contains(stored)
