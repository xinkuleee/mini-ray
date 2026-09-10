"""Lose the first actual Node adoption ACK, then retire exact reply custody.

One Node/one Worker, three children/four endpoints, one stored 2 KiB Task,
one 1 MiB store. The only fault discards one real accepted transport response;
the owner is already READY. At that exact window an immutable observation
records its finish barrier and payload custody. Real retry must use the same
proof without another Push, and retirement must leave bytes until normal GC.
No fault gate, test thread/listener, process death or trace. <=16 ACK and <=16 Push records,
<=128 condition polls per gate, fifteen seconds of work, three-second closes,
unconditional managed-process/endpoint checks under the exact 30-second runner.
"""

import threading

import pytest

import miniray as ray
from miniray import output_protocol as wire, protocol
from miniray.node import GET_OBJECT_HANDLER, GET_WORKER_LEASE_OUTCOME_HANDLER
from miniray.output_handoff import OutputHandoffPhase
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.transport import TransportTimeout, request as rpc_request
from tests.integration._retained_path_support import cluster, close, remaining, wait_local


pytestmark = pytest.mark.multiprocess_smoke


@ray.remote(max_retries=1)
def _retirement_value():
    return b"A" * 2048


def _query(address, handler, request, deadline):
    return rpc_request(address, handler, request, connect_timeout=min(0.5, remaining(deadline)),
                       request_timeout=min(2.0, remaining(deadline)), deadline=deadline)


def test_lost_actual_adoption_ack_replays_retirement_without_reexecution(monkeypatch):
    with cluster(workers=1) as case:
        core, context, deadline = case.core, case.context, case.deadline
        original_rpc, original_push = core._rpc, core._push_task_rpc
        observations, pushes, lost_windows = [], [], []
        lock = threading.Lock()
        overflow = False

        def inspect_rpc(address, handler, request):
            nonlocal overflow
            reply = original_rpc(address, handler, request)
            lose = False
            if handler == wire.ACK_OUTPUT_PUBLICATION_ADOPTED_HANDLER:
                identity = request.proof.complete.publication_id
                with core._completion:
                    snapshot = core.owner_table.snapshot(((identity.object_id,))[0])
                    window = (snapshot.state, snapshot.current_attempt,
                              ((identity.object_id,))[0] in core._task_finish_barriers,
                              identity in core._output_result_custody,
                              core._output_handoff_table().query(identity).phase)
                with lock:
                    if len(observations) < 16:
                        observations.append((request, reply))
                    else:
                        overflow = True
                    if not lost_windows and type(reply) is wire.AckOutputPublicationAdoptedReply and reply.accepted:
                        lost_windows.append(window)
                        lose = True
            if lose:
                raise TransportTimeout("actual Node adoption ACK discarded after retirement")
            return reply

        def inspect_push(address, handler, request):
            nonlocal overflow
            reply = original_push(address, handler, request)
            with lock:
                if len(pushes) < 16:
                    pushes.append((request, reply))
                else:
                    overflow = True
            return reply

        monkeypatch.setattr(core, "_rpc", inspect_rpc)
        monkeypatch.setattr(core, "_push_task_rpc", inspect_push)
        ref = case.keep(_retirement_value.remote())
        assert ray.get(ref, timeout=remaining(deadline)) == b"A" * 2048
        wait_local(core, lambda: ref.object_id not in core._task_finish_barriers
                   and core._accepted_task_count == 0 and not core._protocol_unresolved, deadline)
        snapshot = core.owner_table.snapshot(ref.object_id)
        assert snapshot.state is ObjectState.READY_STORED
        publication = snapshot.output_publication
        identity = publication.publication_id
        with lock:
            observed, executions, windows = tuple(observations), tuple(pushes), tuple(lost_windows)
        assert not overflow and len(observed) >= 2 and len(executions) == 1
        assert windows == ((ObjectState.READY_STORED, identity.attempt_id, True, True, OutputHandoffPhase.ADOPTED),)
        assert all(request == observed[0][0] and reply == observed[0][1] for request, reply in observed)
        assert all(reply.accepted and reply.request == request for request, reply in observed)
        assert identity not in core._output_result_custody
        assert core._recovery.task_record(ref.object_id.task_id).retries_started == 0
        push, task_reply = executions[0]
        assert push.spec.attempt_id == identity.attempt_id and task_reply.status is protocol.TaskReplyStatus.SUCCEEDED
        outcome = _query(context.node_address, GET_WORKER_LEASE_OUTCOME_HANDLER,
                         protocol.GetWorkerLeaseOutcome(identity.lease_id, identity.task_id, identity.attempt_id,
                                                        publication.manifest.header.executor_worker_id, core.worker_id,
                                                        (ref.object_id,)), deadline)
        assert outcome.found and outcome.state is protocol.LeaseExecutionState.COMPLETED
        assert outcome.output_publication is None and outcome.descriptors == ()
        assert outcome.output_completion == observed[0][0].proof.complete and not outcome.cleanup_pending
        descriptor = snapshot.canonical_stored_result
        get = protocol.GetObject(ref.object_id, context.node_id, identity.attempt_id, core.worker_id,
                                 descriptor.size_bytes, descriptor.checksum)
        before = _query(context.node_address, GET_OBJECT_HANDLER, get, deadline)
        assert before.found and before.sealed and before.data is not None
        close(ref, deadline)
        wait_local(core, lambda: core.owner_table.collection_state(ref.object_id)
                   is ObjectCollectionState.COLLECTED, deadline)
        after = _query(context.node_address, GET_OBJECT_HANDLER, get, deadline)
        assert not after.found and after.data is None
        assert not core.owner_table.contains(ref.object_id)
        assert core._output_handoff_table().query(identity).phase is OutputHandoffPhase.ADOPTED
        assert core._recovery.lineage_for_object(ref.object_id) is None
