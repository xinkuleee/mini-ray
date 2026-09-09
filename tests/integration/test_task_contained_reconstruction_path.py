"""Replay a Ref-containing stored Task result through a foreign dependency.

A Driver put supplies a tiny child. A parent Task submits a Worker-owned Task
that returns that child Ref plus 2 KiB padding. A Driver-owned consumer reads
this stored foreign dependency and itself returns Ref + padding. Dropping the
consumer's only replica causes one whole-function replay: the real foreign
lineage credential advances, the dependency is imported again, and the output
replaces its old contained hold. GC then releases both output incarnations,
foreign dependency lineage, its physical bytes and the original child.

One GCS/Node/two Workers, four children/five endpoints, three TaskIDs/four
executions, one tiny put and a 1 MiB store. No extra task, listener, test thread,
trace or process failure. Original owner methods and outgoing RPCs are passive
observation boundaries (<=96 records); replies and IDs are never fabricated.
<=128 condition polls per gate share fifteen seconds; final closes share three
seconds. Exact 30-second process-tree runner bounds init/shutdown and retries.
"""

import threading

import pytest

import miniray as ray
from miniray import protocol
from miniray.core import CoreWorker
from miniray.node import GET_OBJECT_HANDLER
from miniray.ownership import ObjectCollectionState, ObjectState
from miniray.transport import request as rpc_request
from tests.integration._retained_path_support import cluster, close, remaining, wait_local


pytestmark = pytest.mark.multiprocess_smoke
_PADDING = b"T" * 2048


@ray.remote(num_cpus=1, max_retries=1)
def _foreign_stored_outer(holder):
    return {"ref": holder[0], "padding": _PADDING}


@ray.remote(num_cpus=0, max_retries=0)
def _submit_foreign_outer(holder):
    return _foreign_stored_outer.remote(holder)


@ray.remote(num_cpus=1, max_retries=1)
def _reproduce_stored_outer(container, deadline):
    child = container["ref"]
    assert isinstance(child, ray.ObjectRef) and container["padding"] == _PADDING
    assert ray.get(child, timeout=remaining(deadline)) == 42
    return {"ref": child, "padding": _PADDING, "value": 42}


def test_foreign_task_outer_renews_imports_replaces_edges_and_collects(monkeypatch):
    observations, holder = [], []
    lock = threading.Lock()
    overflow = False

    def record(kind, request, reply):
        nonlocal overflow
        with lock:
            if len(observations) < 96:
                observations.append((kind, request, reply))
            else:
                overflow = True

    # OwnerService captures methods while it is created. Patch those original
    # methods before init, limiting observations to the actual Driver owner.
    def observed_owner_method(name):
        original = getattr(CoreWorker, name)

        def call(self, request):
            reply = original(self, request)
            if holder and self is holder[0]:
                record(name, request, reply)
            return reply

        monkeypatch.setattr(CoreWorker, name, call)

    for name in ("acquire_exported_reference", "release_borrowed_reference",
                 "release_contained_reference"):
        observed_owner_method(name)

    with cluster(workers=2) as case:
        core, context, deadline = case.core, case.context, case.deadline
        holder.append(core)
        original_borrow, original_push = core._borrow_rpc, core._push_task_rpc

        def inspect_borrow(address, handler, request):
            reply = original_borrow(address, handler, request)
            if handler in ("replace_retained_object_for_task", "retain_owned_object_for_task",
                           "release_owned_object_for_task", "release_borrowed_object"):
                record(handler, request, reply)
            return reply

        def inspect_push(address, handler, request):
            reply = original_push(address, handler, request)
            record(handler, request, reply)
            return reply

        monkeypatch.setattr(core, "_borrow_rpc", inspect_borrow)
        monkeypatch.setattr(core, "_push_task_rpc", inspect_push)
        child = case.keep(ray.put(42))
        parent = case.keep(_submit_foreign_outer.remote([child]))
        foreign = case.keep(ray.get(parent, timeout=remaining(deadline)))
        assert isinstance(foreign, ray.ObjectRef) and foreign.borrower_token is not None
        assert foreign.owner_worker_id in context.worker_ids and foreign.owner_worker_id != core.worker_id
        consumer = case.keep(_reproduce_stored_outer.remote(foreign, deadline))
        initial = ray.get(consumer, timeout=remaining(deadline))
        initial_child = case.keep(initial["ref"])
        assert initial["padding"] == _PADDING and initial["value"] == 42
        assert initial_child.object_id == child.object_id and ray.get(initial_child, timeout=remaining(deadline)) == 42
        wait_local(core, lambda: core._accepted_task_count == 0 and not core._protocol_unresolved
                   and consumer.object_id not in core._task_finish_barriers, deadline)
        first = core.owner_table.snapshot(consumer.object_id)
        assert first.state is ObjectState.READY_STORED and first.current_attempt.attempt_number == 0
        first_edge, = first.outgoing_contained_edges
        first_hold = first_edge.incoming_hold(core.worker_id)
        assert first_edge.contained_object_id == child.object_id
        assert first_hold in core.owner_table.snapshot(child.object_id).contained_holds
        foreign_query = protocol.GetOwnedObject(foreign.object_id, foreign.owner_worker_id,
                                                core.worker_id, foreign.borrower_token)
        foreign_state = original_borrow(foreign.owner_address, "get_owned_object", foreign_query)
        assert foreign_state.accepted and foreign_state.state is protocol.OwnedObjectState.READY_STORED
        assert foreign_state.current_attempt.attempt_number == 0
        foreign_descriptor = foreign_state.descriptor
        assert foreign_descriptor is not None and 2048 <= foreign_descriptor.size_bytes < 8192
        assert first.producer_task_spec.args[0] == protocol.RefArg(foreign.object_id, foreign.owner_worker_id)

        # Close every independent source handle. Only the accepted Task lineage
        # and real contained holds may keep the replay's input and child alive.
        for ref in (initial_child, child, foreign, parent):
            close(ref, deadline)
        foreign_key = (foreign.owner_worker_id, foreign.object_id, core.worker_id, foreign.borrower_token)
        wait_local(core, lambda: foreign_key not in core._borrowed_release_obligations
                   and core.owner_table.collection_state(parent.object_id) is ObjectCollectionState.COLLECTED, deadline)
        assert not core.owner_table.snapshot(child.object_id).local_tokens
        assert ray.drop_object(consumer)
        assert core.owner_table.snapshot(consumer.object_id).state is ObjectState.LOST
        rebuilt = ray.get(consumer, timeout=remaining(deadline))
        rebuilt_child = case.keep(rebuilt["ref"])
        assert rebuilt["value"] == 42 and rebuilt["padding"] == _PADDING
        assert rebuilt_child.object_id == child.object_id
        assert ray.get(rebuilt_child, timeout=remaining(deadline)) == 42
        wait_local(core, lambda: core._accepted_task_count == 0 and not core._protocol_unresolved
                   and consumer.object_id not in core._task_finish_barriers, deadline)
        second = core.owner_table.snapshot(consumer.object_id)
        assert second.current_attempt == first.current_attempt.next() and second.state is ObjectState.READY_STORED
        second_edge, = second.outgoing_contained_edges
        second_hold = second_edge.incoming_hold(core.worker_id)
        assert second_edge.container_object_id == first_edge.container_object_id == consumer.object_id
        assert second_edge.contained_object_id == first_edge.contained_object_id == child.object_id
        assert second_hold != first_hold
        child_state = core.owner_table.snapshot(child.object_id)
        assert first_hold not in child_state.contained_holds and second_hold in child_state.contained_holds
        assert core._recovery.task_record(consumer.object_id.task_id).retries_started == 1

        with lock:
            exchanges = tuple(observations)
        replacements = [(index, request, reply) for index, (kind, request, reply) in enumerate(exchanges)
                        if kind == "replace_retained_object_for_task" and request.object_id == foreign.object_id]
        assert replacements
        for _, request, reply in replacements:
            assert type(request) is protocol.ReplaceRetainedObjectForTask
            assert request.expected_hold.task_id == request.replacement_hold.task_id == consumer.object_id.task_id
            assert request.expected_hold.origin_attempt_id == first.current_attempt
            assert request.replacement_hold.origin_attempt_id == second.current_attempt
            assert reply.expected_hold == request.expected_hold and reply.replacement_hold == request.replacement_hold
            assert reply.disposition in (protocol.ReplaceRetainedObjectDisposition.REPLACED,
                                         protocol.ReplaceRetainedObjectDisposition.ALREADY_REPLACED)
        pushes = [(index, request, reply) for index, (kind, request, reply) in enumerate(exchanges)
                  if kind == "push_task" and request.spec.task_id == consumer.object_id.task_id]
        assert len(pushes) == 2 and [request.spec.attempt_id for _, request, _ in pushes] == [first.current_attempt, second.current_attempt]
        assert replacements[0][0] < pushes[1][0]
        assert pushes[0][1].lease_id != pushes[1][1].lease_id
        assert all(request.dependencies == (foreign_descriptor,) for _, request, _ in pushes)
        assert all(reply.status is protocol.TaskReplyStatus.SUCCEEDED for _, _, reply in pushes)
        imports = [(request, reply) for kind, request, reply in exchanges
                   if kind == "acquire_exported_reference" and request.object_id == child.object_id
                   and isinstance(request.source, protocol.ContainedTransferSource)
                   and request.source.hold.container_object_id == foreign.object_id]
        assert len(imports) == 2
        assert len({request.borrower_token for request, _ in imports}) == 2
        assert all(reply.accepted and reply.source == request.source
                   and request.borrower_worker_id in context.worker_ids for request, reply in imports)
        imported_tokens = {request.borrower_token for request, _ in imports}
        released_tokens = {request.borrower_token for kind, request, reply in exchanges
                           if kind == "release_borrowed_reference" and request.object_id == child.object_id and reply.accepted}
        assert imported_tokens <= released_tokens

        close(rebuilt_child, deadline)
        close(consumer, deadline)
        wait_local(core, lambda: all(core.owner_table.collection_state(ref.object_id)
                                    is ObjectCollectionState.COLLECTED for ref in (consumer, child)), deadline)
        wait_local(core, lambda: consumer.object_id not in core._foreign_lineage_collection_receipts, deadline)
        assert consumer.object_id not in core._object_gc_obligations
        assert core._recovery.lineage_for_object(consumer.object_id) is None
        get = protocol.GetObject(foreign.object_id, context.node_id, foreign_descriptor.producer_attempt_id,
                                 foreign.owner_worker_id, foreign_descriptor.size_bytes, foreign_descriptor.checksum)
        # The foreign owner releases its child edge before dropping its own
        # bytes. Driver child GC can therefore precede that remote Drop ACK.
        # Poll only the exact actual replica, under the original work deadline.
        pause = threading.Event()
        for index in range(32):
            absent = rpc_request(context.node_address, GET_OBJECT_HANDLER, get,
                                 connect_timeout=min(0.5, remaining(deadline)),
                                 request_timeout=min(2.0, remaining(deadline)), deadline=deadline)
            if not absent.found:
                break
            if index < 31:
                pause.wait(min(0.05, remaining(deadline)))
        assert not absent.found and absent.data is None
        with lock:
            final_exchanges = tuple(observations)
        releases = [(request, reply) for kind, request, reply in final_exchanges
                    if kind == "release_owned_object_for_task" and request.object_id == foreign.object_id
                    and request.hold == replacements[-1][1].replacement_hold]
        assert releases and all(reply.accepted and reply.hold == request.hold for request, reply in releases)
        assert not overflow
