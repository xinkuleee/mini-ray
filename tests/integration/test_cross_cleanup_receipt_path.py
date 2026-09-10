"""A real publication rollback supplies an ordinary-GC deletion receipt.

One GCS, one Node, one ordinary Worker (three startup children), one 1 MiB
store, one 8 KiB result, and one logical task with max_retries=1. A spawn-safe
test wrapper calls the real Node entry and injects exactly one ObjectStoreError
AFTER attempt zero has really sealed, but BEFORE its materialization ACK.
The existing rejected-Prepare/failed-Complete protocol must finish publication
rollback before the ordinary system retry seals the same ObjectID at epoch one.

The first generic old-epoch Drop is deliberately sent only then. It must be
ALREADY_DROPPED immediately, with the new physical replica unchanged. Child
observers count every generic invocation, including failures, so an earlier
generic cleanup cannot silently prime the receipt being tested. Observations
do not mutate runtime state or fabricate tombstones, replies, or new handlers.

No kill, Actor, dependency, reconstruction, tracing, extra process, test-owned
thread, or listener. Work shares a 15-second post-init deadline; public reference
close gets three seconds in cleanup and rechecks an earlier timed-out receipt.
Observed PIDs/endpoints are checked after shutdown even if work or close failed.
Run this exact node ID with run_baseline.py --case:
its 30-second execution deadline covers startup through shutdown, followed by
the runner's existing bounded process-tree cleanup grace on failure.
"""

from __future__ import annotations

from dataclasses import replace
import hashlib
import multiprocessing as mp
import os
import socket
import time

import pytest

import miniray as ray
from miniray import api as api_module, node as node_module, output_protocol as wire, protocol
from miniray.api import _get_runtime
from miniray.errors import ObjectStoreError
from miniray.output_publication import OutputPublicationID
from miniray.output_handoff import OutputHandoffPhase
from miniray.core import CoreWorker
from miniray.output_publication_journal import OutputPublicationJournalState, OutputPublicationStage
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.task_outputs import TaskExecution
from miniray.transport import request as rpc_request
from miniray.worker import PUSH_TASK_HANDLER
from tests.integration.test_task_path import _close_reference as _close_local


pytestmark = pytest.mark.multiprocess_smoke
_ACTUAL_NODE_PROCESS_MAIN = api_module._node_process_main
_PAYLOAD = b"R" * (8 * 1024)
_POST_SEAL_ERROR = "cross-cleanup acceptance: real seal finished before materialization ACK"


def _node_process_with_one_post_seal_failure(*args):
    """Patch before Node construction binds its real adapter/server callbacks."""
    node_type = node_module.NodeServer
    actual_seal = node_type._seal_output_publication_replica
    actual_rollback = node_type._drop_output_publication_replica
    actual_generic = node_type._handle_drop_object_replica
    counts = {"seal": 0, "injected": 0, "rollback": 0, "generic": 0, "old": 0, "new": 0}
    facts, violations = {}, []

    def check(condition, detail):
        if not condition and len(violations) < 12:
            violations.append(detail)

    def observe(probe):
        # Observation failures are checked at child exit, not thrown into an
        # RPC where they could become another injected runtime failure.
        try:
            probe()
        except Exception as exc:
            check(False, "observation raised {}: {}".format(type(exc).__name__, exc))

    def seal(node, effect, descriptor, payload):
        counts["seal"] += 1
        attempt = effect.publication_id.attempt_id.attempt_number

        def before_retry():
            old_effect, old_descriptor = facts["old_seal"]
            prior = node._output_publication_journal.snapshot(old_effect.publication_id)
            check(counts["generic"] == 0, "generic cleanup primed the receipt before retry")
            check(counts["rollback"] == 1 and facts.get("physical_rollback") is True,
                  "retry preceded an actual publication physical Drop")
            check(prior.state is OutputPublicationJournalState.RETIRED
                  and prior.rollback_tombstone is not None and prior.complete is None,
                  "retry preceded the exact publication rollback tombstone")
            check(node._output_publications.rollback_reported(old_effect.publication_id),
                  "retry preceded the real owner rollback ACK")
            check(not node._object_store.contains(descriptor.object_id, sealed_only=False),
                  "attempt zero bytes remained at retry admission")
            check(descriptor == old_descriptor and effect.publication_id.attempt_id
                  == old_effect.publication_id.attempt_id.next(),
                  "retry changed logical output, owner, size, or checksum")

        if attempt == 1:
            observe(before_retry)
        result = actual_seal(node, effect, descriptor, payload)

        def sealed():
            stored = node._object_store.snapshot(descriptor.object_id)
            check(stored.sealed and stored.size_bytes == len(payload), "real sealed bytes were not observed")
            check(node._object_store.get(descriptor.object_id) == payload, "sealed bytes differ from the result")
            check(node._sealed_metadata.get(descriptor.object_id) == (
                effect.publication_id.attempt_id, descriptor.owner_worker_id, descriptor.size_bytes, descriptor.checksum,
            ), "real seal metadata does not name this attempt")
            check(len(_PAYLOAD) < len(payload) < 16 * 1024, "result exceeded the small-store test bound")
            check(node._object_store.capacity_bytes == 1024 * 1024, "ObjectStore bound changed")

        observe(sealed)
        if attempt == 0 and counts["injected"] == 0:
            facts["old_seal"] = (effect, descriptor)
            counts["injected"] = 1
            raise ObjectStoreError(_POST_SEAL_ERROR)
        if attempt == 1:
            facts["new_seal"] = (effect, descriptor)
        else:
            check(False, "unexpected physical attempt or repeated attempt-zero seal")
        return result

    def rollback(node, effect, request):
        counts["rollback"] += 1

        def before_drop():
            old_effect, descriptor = facts["old_seal"]
            check(counts["generic"] == 0, "generic Drop preceded publication compensation")
            check(effect.publication_id == old_effect.publication_id
                  and effect.stage is OutputPublicationStage.SLOT_DROP and effect.slot_index == 0,
                  "publication Drop did not compensate the failed materialization")
            check(request == protocol.DropObjectReplica(
                descriptor.object_id, old_effect.publication_id.attempt_id,
                descriptor.owner_worker_id, node.node_id, descriptor.checksum,
            ), "publication Drop changed the physical identity")
            check(node._object_store.contains(request.object_id), "publication Drop had no sealed replica")

        observe(before_drop)
        reply = actual_rollback(node, effect, request)

        def after_drop():
            physical = (reply.status is protocol.DropObjectReplicaStatus.DROPPED
                        and reply.accepted and reply.dropped and reply.error is None
                        and not node._object_store.contains(request.object_id, sealed_only=False)
                        and request.object_id not in node._sealed_metadata
                        and request.object_id not in node._local_replica_write_claims)
            facts["physical_rollback"] = physical
            check(physical, "publication compensation did not remove its physical replica")

        observe(after_drop)
        return reply

    def generic(node, request):
        # Count before the actual call: an earlier rejected/failed generic Drop
        # is not allowed to disappear from the acceptance history.
        counts["generic"] += 1
        attempt = request.producer_attempt_id.attempt_number
        if attempt in (0, 1):
            counts["old" if attempt == 0 else "new"] += 1
        else:
            check(False, "generic Drop named an unexpected attempt")
        before = []

        def before_old_replay():
            check(counts["old"] == 1 and counts["generic"] == 1, "old replay was not the first generic Drop")
            new_effect, descriptor = facts["new_seal"]
            check(request.object_id == descriptor.object_id
                  and request.producer_attempt_id.next() == new_effect.publication_id.attempt_id,
                  "old replay did not run after the same ObjectID was resealed")
            before.append((node._object_store.snapshot(request.object_id),
                           node._object_store.get(request.object_id), node._sealed_metadata.get(request.object_id)))

        if attempt == 0:
            observe(before_old_replay)
        reply = actual_generic(node, request)

        def after_old_replay():
            check(reply.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
                  and reply.accepted and not reply.dropped and reply.error is None,
                  "first generic replay did not consume the publication deletion receipt")
            after = (node._object_store.snapshot(request.object_id),
                     node._object_store.get(request.object_id), node._sealed_metadata.get(request.object_id))
            check(before == [after], "old generic replay changed the new physical replica")

        if attempt == 0:
            observe(after_old_replay)
        return reply

    node_type._seal_output_publication_replica = seal
    node_type._drop_output_publication_replica = rollback
    node_type._handle_drop_object_replica = generic
    try:
        # The original entry owns setsid, child startup, serving, and teardown.
        _ACTUAL_NODE_PROCESS_MAIN(*args)
        assert counts == {"seal": 2, "injected": 1, "rollback": 1, "generic": 2, "old": 1, "new": 1}, counts
        assert not violations, violations
    finally:
        node_type._seal_output_publication_replica = actual_seal
        node_type._drop_output_publication_replica = actual_rollback
        node_type._handle_drop_object_replica = actual_generic


@ray.remote(max_retries=1)
def _stored_result_after_publication_retry():
    return _PAYLOAD


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    assert remaining > 0, "cross-cleanup acceptance exceeded its shared deadline"
    return remaining


def _wait(core, predicate, deadline):
    with core._completion:
        while not predicate():
            core._completion.wait(_remaining(deadline))


def _rpc(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(address, handler, request, connect_timeout=min(0.5, remaining / 2),
                       request_timeout=min(2.0, remaining / 2), deadline=deadline)


def _handoff(owner_address, publication_id, deadline):
    request = wire.GetOutputHandoff(publication_id)
    reply = _rpc(owner_address, wire.GET_OUTPUT_HANDOFF_HANDLER, request, deadline)
    assert type(reply) is wire.OutputHandoffReply and reply.request == request
    assert reply.accepted and reply.snapshot is not None
    return reply.snapshot


def _replica(node, object_id, deadline):
    # No expected-epoch filter: a missing or different replica cannot masquerade
    # as the still-present retry result. This RPC does not reconstruct or pin.
    reply = _rpc(node.node_address, node_module.GET_OBJECT_HANDLER,
                 protocol.GetObject(object_id, node.node_id), deadline)
    assert type(reply) is protocol.GetObjectReply and reply.object_id == object_id and reply.node_id == node.node_id
    return reply


def _pid_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def test_publication_rollback_receipt_replays_after_same_object_retry_seals(monkeypatch):
    context = report = core = reference = original_push = None
    original_entry = api_module._node_process_main
    pids, addresses, cleanup_errors, pushes, replies = set(), set(), [], [], []
    push_count = 0
    rollback_reports = []
    actual_report = CoreWorker.report_output_handoff_rollback

    def observe_rollback(owner, request):
        reply = actual_report(owner, request)
        if core is owner:
            assert len(rollback_reports) < 4
            rollback_reports.append((request, reply))
        return reply

    monkeypatch.setattr(CoreWorker, "report_output_handoff_rollback", observe_rollback)
    try:
        api_module._node_process_main = _node_process_with_one_post_seal_failure
        context = ray.init(num_nodes=1, num_cpus=1, num_workers_per_node=1,
                           inline_threshold=1024, object_store_bytes=1024 * 1024, enable_tracing=False)
        deadline = time.monotonic() + 15.0
        runtime = _get_runtime()
        core = runtime.core_worker
        (node,) = context.nodes
        pids.update((context.gcs_pid, node.node_pid, node.worker_pid))
        addresses.update((context.gcs_address, node.node_address, node.worker_address, runtime.owner_service.address))
        assert len(pids) == 3 and len(addresses) == 4 and os.getpid() not in pids
        assert len(node.worker_ids) == 1 and context.trace_address is None
        original_push = core._push_task_rpc

        def observe_push(address, handler, request):
            nonlocal push_count
            push_count += 1
            if len(pushes) < 8:
                pushes.append((address, handler, request))
            reply = original_push(address, handler, request)
            if len(replies) < 8:
                replies.append((request, reply))
            return reply

        core._push_task_rpc = observe_push
        reference = _stored_result_after_publication_retry.remote()
        assert ray.get(reference, timeout=_remaining(deadline)) == _PAYLOAD
        _wait(core, lambda: reference.object_id not in core._task_finish_barriers, deadline)
        # The supervisor can briefly own the nonblocking rollback ticket. An
        # ambiguous RPC may therefore replay the identical Push, but must not
        # request another lease or execute a third physical attempt.
        assert 2 <= push_count <= 8 and len(pushes) == push_count and replies
        attempts = tuple(request.spec.attempt_id.attempt_number for _address, _handler, request in pushes)
        assert tuple(dict.fromkeys(attempts)) == (0, 1) and attempts == tuple(sorted(attempts))
        first_push = next(request for _address, _handler, request in pushes if request.spec.attempt_id.attempt_number == 0)
        retry_push = next(request for _address, _handler, request in pushes if request.spec.attempt_id.attempt_number == 1)
        assert all(request == (first_push if request.spec.attempt_id.attempt_number == 0 else retry_push)
                   for _address, _handler, request in pushes)
        assert all(address == node.worker_address and handler == PUSH_TASK_HANDLER
                   for address, handler, _request in pushes)
        assert first_push.worker_id == retry_push.worker_id == node.worker_id
        assert first_push.spec.return_ids() == retry_push.spec.return_ids() == (reference.object_id,)
        assert first_push.spec.attempt_id.attempt_number == 0
        assert retry_push.spec.attempt_id == first_push.spec.attempt_id.next()
        assert first_push.lease_id != retry_push.lease_id
        failed_replies = tuple(reply for request, reply in replies if request == first_push)
        successful_replies = tuple(reply for request, reply in replies if request == retry_push)
        assert failed_replies and successful_replies
        assert all(reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR and reply.error is not None
                   and _POST_SEAL_ERROR in reply.error.message and not reply.results
                   and reply.output_publication is None for reply in failed_replies)
        assert all(reply.status is protocol.TaskReplyStatus.SUCCEEDED for reply in successful_replies)

        # Use the first Push identity and the actual owner rollback request.
        # ABORTED alone is not cleanup proof: the request carries the exact
        # Node journal tombstone and the original owner handler validates it.
        old_id = OutputPublicationID(first_push.lease_id, TaskExecution.from_task_spec(first_push.spec))
        rolled_back = _handoff(runtime.owner_service.address, old_id, deadline)
        old_reports = [(request, response) for request, response in rollback_reports
                       if request.manifest.publication_id == old_id]
        assert old_reports
        rollback_request, rollback_reply = old_reports[0]
        assert all(request == rollback_request and response.accepted for request, response in old_reports)
        assert rollback_reply.request == rollback_request
        old_manifest, tombstone = rollback_request.manifest, rollback_request.tombstone
        assert rolled_back.manifest == old_manifest and old_manifest.publication_id == old_id
        assert rolled_back.complete is None and rolled_back.adoption is None
        assert rolled_back.phase is OutputHandoffPhase.ABORTED and tombstone.plan.publication_id == old_id
        assert tombstone.plan.manifest_digest == old_manifest.manifest_digest
        assert tuple((effect.stage, effect.slot_index) for effect in tombstone.plan.effects) == ((OutputPublicationStage.SLOT_DROP, 0),)
        assert tuple(ack.effect for ack in tombstone.acknowledgements) == tombstone.plan.effects
        old_slot = (old_manifest.value)
        assert (old_manifest.publication_id).object_id == reference.object_id and old_slot.tier is protocol.ResultStorage.OBJECT_STORE
        assert not old_slot.transfers and len(_PAYLOAD) < old_slot.size_bytes < 16 * 1024
        assert old_manifest.header.owner_worker_id == core.worker_id and old_manifest.header.executor_worker_id == node.worker_id

        owned = core.owner_table.snapshot(reference.object_id)
        assert owned.state is ObjectState.READY_STORED and owned.current_attempt == retry_push.spec.attempt_id
        assert reference.owner_worker_id == core.worker_id and reference.borrower_token is None
        member = owned.output_publication
        assert member is not None and (member.manifest.value) == old_slot
        assert member.publication_id == OutputPublicationID(retry_push.lease_id, TaskExecution.from_task_spec(retry_push.spec))
        assert member.manifest.header.node_incarnation == old_manifest.header.node_incarnation
        adopted = _handoff(runtime.owner_service.address, member.publication_id, deadline)
        assert adopted.complete is not None and adopted.adoption is not None and adopted.phase is OutputHandoffPhase.ADOPTED
        assert adopted.adoption.complete == adopted.complete and adopted.manifest == member.manifest
        # RecoveryManager returns a live mutable TaskRecord; snapshot it before
        # the late message so equality cannot compare a mutated object to itself.
        record = replace(core._recovery.task_record(reference.object_id.task_id))
        assert record.retries_started == 1 and record.current_attempt == owned.current_attempt
        assert core._recovery.active_recovery(reference.object_id.task_id) is None

        before = _replica(node, reference.object_id, deadline)
        assert before.found and before.sealed and before.producer_attempt_id == retry_push.spec.attempt_id
        assert before.owner_worker_id == core.worker_id and before.size_bytes == old_slot.size_bytes
        assert before.checksum == old_slot.checksum and len(before.data) == old_slot.size_bytes
        assert hashlib.sha256(before.data).hexdigest() == old_slot.checksum
        old_drop = protocol.DropObjectReplica((old_manifest.publication_id).object_id, old_id.attempt_id,
                    old_manifest.header.owner_worker_id, node.node_id, old_slot.checksum)
        reply = _rpc(node.node_address, node_module.DROP_OBJECT_REPLICA_HANDLER, old_drop, deadline)
        assert type(reply) is protocol.DropObjectReplicaReply
        assert protocol.DropObjectReplica(reply.object_id, reply.producer_attempt_id, reply.owner_worker_id,
                                         reply.node_id, reply.checksum) == old_drop
        assert reply.status is protocol.DropObjectReplicaStatus.ALREADY_DROPPED
        assert reply.accepted and not reply.dropped and reply.error is None
        # Check physical truth BEFORE another get could trigger reconstruction.
        assert _replica(node, reference.object_id, deadline) == before
        assert core.owner_table.snapshot(reference.object_id) == owned
        assert core._recovery.task_record(reference.object_id.task_id) == record
        assert ray.get(reference, timeout=_remaining(deadline)) == _PAYLOAD

        _close_local(reference, deadline)
        _wait(core, lambda: core.owner_table.collection_state(reference.object_id) is ObjectCollectionState.COLLECTED, deadline)
        missing = _replica(node, reference.object_id, deadline)
        assert not missing.found and not missing.sealed and missing.data is None and missing.checksum is None
        assert missing.producer_attempt_id is None and missing.owner_worker_id is None and missing.size_bytes is None
        assert not core._protocol_unresolved and not core._task_finish_barriers and not core._object_gc_obligations
        status = _rpc(node.node_address, node_module.SHUTDOWN_STATUS_HANDLER,
                      protocol.ShutdownStatusRequest("inspect-cross-cleanup-only"), deadline)
        assert type(status) is protocol.ShutdownStatus and not status.shutdown_requested and not status.finalized
        assert status.resources_clean and status.child_pids == (node.worker_pid,)
        assert all(_pid_exists(pid) for pid in pids)
    finally:
        api_module._node_process_main = original_entry
        if core is not None and original_push is not None:
            core._push_task_rpc = original_push
        try:
            _close_local(reference, time.monotonic() + 3.0)
        except Exception as exc:
            cleanup_errors.append(exc)
        finally:
            # Also executes after partial startup or a failed test assertion.
            try:
                report = ray.shutdown()
            finally:
                # Collect every observation before asserting so one leaked PID
                # cannot skip the remaining endpoint checks on a failure path.
                surviving_pids = tuple(pid for pid in sorted(pids) if _pid_exists(pid))
                surviving_children = tuple(
                    process.pid for process in mp.active_children() if process.pid in pids
                )
                open_addresses = []
                for address in sorted(addresses):
                    try:
                        with socket.create_connection(address, timeout=0.1):
                            open_addresses.append(address)
                    except OSError:
                        pass
                assert not surviving_pids, surviving_pids
                assert not surviving_children, surviving_children
                assert not open_addresses, open_addresses
                assert not cleanup_errors, cleanup_errors
    assert not cleanup_errors and context is not None and report is not None
    assert not ray.is_initialized() and report.core_stopped
    assert report.gcs_clean and report.gcs_exitcode == 0 and report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized and report.shutdown_ack_clean
    assert not report.forced and not report.gcs_forced and not any(report.worker_forced)
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    # A failed child-side observation must fail the test, even after otherwise
    # successful shutdown; no extra endpoint transports these counters.
    assert report.node_exitcodes == report.worker_exitcodes == (0,)
