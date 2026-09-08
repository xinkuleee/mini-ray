"""Pure Worker imports for contained refs inside materialized result bytes.

One consumer, at most two imports of one child, and one 128 KiB in-memory
Node store per case. Stored inputs use actual seal/GetObject handlers;
successful outputs use the unified Node adapter/journal and real child-owner
reducers. The embedded Core boundary returns tracked handles without a
mailbox or real RPC. Early error Complete replies use the existing typed
Worker fixture; this file does not test remote release-ACK convergence.

All retries are explicit and bounded. No runtime constructor, listener,
process, thread, timer or wait runs.
"""

from __future__ import annotations

from dataclasses import replace
import multiprocessing.process
import socket
import threading

import cloudpickle
import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.core import ObjectRef
from miniray.dependency import ContainedRef, encode_task_argument
from miniray.ids import AttemptID, ObjectID, TaskID
from miniray.ownership import ObjectOwnerTable
from miniray.publication_sources import BorrowedContainedSource
from miniray.ref_transfer import current_importer, exporting_references
from miniray.resources import ResourceVector
from miniray.transport import TransportError
from miniray.worker import (
    COMPLETE_WORKER_LEASE_HANDLER, GET_OBJECT_HANDLER, START_WORKER_LEASE_HANDLER,
)
from tests.unit.test_worker_unified_output import (
    _ActualNodePublication, _Fixture, _install_no_runtime,
)


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    _install_no_runtime(monkeypatch)

    def forbidden(*_args, **_kwargs):
        pytest.fail("materialized argument test attempted runtime infrastructure")

    monkeypatch.setattr(socket, "socketpair", forbidden)
    monkeypatch.setattr(threading.Barrier, "wait", forbidden)
    monkeypatch.setattr(multiprocessing.process.BaseProcess, "join", forbidden)


class _ImportedRef(ObjectRef):
    """Use real owner accounting, but no Core finalizer/mailbox/wait."""

    __slots__ = ("table", "borrower", "events", "label", "closes")

    def __init__(self, imports, source, label, borrower):
        super().__init__(imports.child, imports.owner, imports.address)
        self.table = imports.table
        self.borrower = borrower
        self.events = imports.events
        self.label = label
        self.closes = 0
        self._borrower_token = borrower[1]
        self._borrow_source = source

    def close(self):
        self.closes += 1
        assert self.closes == 1, "one imported handle was closed more than once"
        assert self.table.release_borrowed_reference(self.object_id, self.borrower)
        self._closed = True
        self.events.append(("close", self.label))


class _Imports:
    """One child owner and the two existing Core restore entry points."""

    def __init__(self, fixture, monkeypatch):
        self.fixture = fixture
        spec = fixture.push.spec
        self.owner = spec.owner_worker_id
        self.address = ("127.0.0.1", 32801)
        self.child = ObjectID.for_task(TaskID.derive(spec.job_id, spec.task_id, 61))
        self.outer = ObjectID.for_task(TaskID.derive(spec.job_id, spec.task_id, 62))
        self.hold = ContainedReferenceHold(self.outer, self.owner, "materialized-outer-child")
        self.table = ObjectOwnerTable()
        self.table.register(self.child, local_token="child-source-live")
        assert self.table.add_contained_reference(self.child, self.hold)
        self.handles = []
        self.events = []
        self.exported_calls = []
        self.nested_calls = []
        self.core_requests = []
        self.nested_transfer = None

        def embedded_core(job_id):
            assert job_id == spec.job_id
            self.core_requests.append(job_id)
            return self

        monkeypatch.setattr(fixture.worker, "_embedded_core_for", embedded_core)

    def _acquire(self, source, label):
        assert len(self.handles) < 2
        borrower = (self.fixture.worker.worker_id, "materialized-borrow-{}".format(len(self.handles)))
        assert self.table.acquire_exported_reference(self.child, source, borrower)
        handle = _ImportedRef(self, source, label, borrower)
        self.handles.append(handle)
        self.events.append(("acquire", label))
        return handle

    def _restore_borrowed_reference(self, object_id, owner, address, hold):
        assert (object_id, owner, address) == (self.child, self.owner, self.address)
        assert current_importer() is not None
        assert hold in self.table.snapshot(self.child).contained_holds
        self.exported_calls.append((object_id, owner, address, hold))
        return self._acquire(protocol.ContainedTransferSource(hold), "contained")

    def _restore_task_argument_reference(self, transfer, attempt):
        assert transfer == self.nested_transfer
        assert attempt == self.fixture.push.spec.attempt_id
        self.nested_calls.append((transfer, attempt))
        return self._acquire(protocol.TaskHoldSource(transfer.hold), "task")

    def result_bytes(self, *, hold=None):
        """The prior result's actual reducer format, with no Task manifest."""
        hold = self.hold if hold is None else hold
        reference = ObjectRef(self.child, self.owner, self.address)

        def export(value):
            assert value is reference
            return self.child, self.owner, self.address, hold

        with exporting_references(export):
            return cloudpickle.dumps({"child": reference, "again": reference})

    def explicit_nested_argument(self):
        spec = self.fixture.push.spec
        hold = protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.SUBMITTED, self.owner,
            spec.task_id, spec.attempt_id,
        )
        assert self.table.add_submitted_reference(self.child, hold)
        self.nested_transfer = protocol.NestedReferenceTransfer(
            self.child, self.owner, self.address, hold,
        )
        return encode_task_argument(
            {"child": ContainedRef(self.child, self.owner)}, serializer="cloudpickle",
            export_nested_ref=lambda _reference: self.nested_transfer,
        )


def _stored_input(fixture, backend, payload, *, object_id=None):
    assert len(payload) <= 8 * 1024
    spec = fixture.push.spec
    object_id = object_id or ObjectID.for_task(TaskID.derive(spec.job_id, spec.task_id, 63))
    attempt = AttemptID(object_id.task_id, 0)
    seal = protocol.SealObject.from_data(object_id, attempt, spec.owner_worker_id, payload)
    reply = backend.node._handle_seal_object(seal)
    assert reply.sealed and backend.store.get(object_id) == payload
    descriptor = protocol.ObjectStoreDescriptor(
        object_id, spec.owner_worker_id, attempt, fixture.worker.node_id,
        len(payload), seal.checksum,
    )
    return protocol.RefArg(object_id, spec.owner_worker_id), descriptor


def _route_input_reads(fixture, backend, monkeypatch):
    reads = []

    def rpc(address, handler, request, **options):
        if handler == GET_OBJECT_HANDLER:
            assert address == fixture.worker.node_address
            assert len(reads) < 2
            reads.append(request)
            fixture.calls.append((handler, request))
            return backend.node._handle_get_object(request)
        return fixture.rpc(address, handler, request, **options)

    monkeypatch.setattr("miniray.worker.rpc_request", rpc)
    return reads


@pytest.mark.parametrize("storage", ("stored-refarg", "converted-inline"))
def test_materialized_result_imports_once_across_positional_and_keyword_arguments(monkeypatch, storage):
    escaped = []

    def consume(value, *, named):
        assert current_importer() is None
        child = value["child"]
        assert child is value["again"] is named["child"] is named["again"]
        assert not child.closed and isinstance(child.borrow_source, protocol.ContainedTransferSource)
        escaped.append(child)  # A user-global alias does not extend this attempt handle.
        return 17

    f = _Fixture(monkeypatch, consume)
    imports = _Imports(f, monkeypatch)
    backend = _ActualNodePublication(f)
    payload = imports.result_bytes()
    dependencies = ()
    if storage == "stored-refarg":
        argument, descriptor = _stored_input(f, backend, payload, object_id=imports.outer)
        dependencies = (descriptor,)
    else:
        # This is the ready-INLINE conversion made by dependency preparation,
        # not encode_task_argument's explicit nested-reference manifest.
        argument = protocol.InlineArg(payload, serializer="cloudpickle")
        assert argument.nested_refs == ()
    f.push = replace(f.push, spec=replace(f.push.spec, args=(argument,), kwargs=(("named", argument),)),
                     dependencies=dependencies)
    reads = _route_input_reads(f, backend, monkeypatch)

    reply = f.worker._handle_push_task(f.push)

    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert cloudpickle.loads(reply.results[0].inline_data) == 17
    assert len(imports.exported_calls) == len(imports.core_requests) == 1
    assert imports.nested_calls == []
    assert escaped == imports.handles and escaped[0].closed and escaped[0].closes == 1
    assert not imports.table.snapshot(imports.child).borrowed_tokens
    assert len(reads) == (2 if storage == "stored-refarg" else 0)
    assert len(backend.completions) == 1 and f.executions == [True]
    assert current_importer() is None
    before = tuple(f.calls), tuple(imports.core_requests)
    assert f.worker._handle_push_task(f.push) is reply
    assert (tuple(f.calls), tuple(imports.core_requests)) == before
    assert len(imports.handles) == 1 and escaped[0].closes == 1


def test_later_decode_failure_rolls_back_contained_and_task_sources_without_merging(monkeypatch):
    f = _Fixture(monkeypatch, lambda *_args, **_kwargs: pytest.fail("bad call reached user code"))
    imports = _Imports(f, monkeypatch)
    contained = protocol.InlineArg(imports.result_bytes(), serializer="cloudpickle")
    nested = imports.explicit_nested_argument()
    f.push = replace(f.push, spec=replace(
        f.push.spec, args=(contained, nested),
        kwargs=(("bad", protocol.InlineArg(b"not a pickle")),),
    ))

    def complete(request):
        assert request.status is protocol.TaskReplyStatus.SYSTEM_ERROR
        assert all(handle.closed for handle in imports.handles)
        assert current_importer() is None
        return f.complete(request)

    f.on_complete = complete
    reply = f.worker._handle_push_task(f.push)

    assert reply.status is protocol.TaskReplyStatus.SYSTEM_ERROR
    assert f.executions == [] and f.prepares == []
    assert f.handlers == [START_WORKER_LEASE_HANDLER, COMPLETE_WORKER_LEASE_HANDLER]
    assert len(imports.exported_calls) == len(imports.nested_calls) == 1
    first, second = imports.handles
    assert first == second and first is not second
    assert isinstance(first.borrow_source, protocol.ContainedTransferSource)
    assert isinstance(second.borrow_source, protocol.TaskHoldSource)
    assert imports.events == [
        ("acquire", "contained"), ("acquire", "task"),
        ("close", "task"), ("close", "contained"),
    ]
    snapshot = imports.table.snapshot(imports.child)
    assert not snapshot.borrowed_tokens and imports.hold in snapshot.contained_holds
    assert imports.nested_transfer.hold in snapshot.submitted_tokens
    assert not getattr(f.worker, "_prepared_output_replies", {})
    assert current_importer() is None
    assert f.worker._handle_push_task(f.push) is reply and f.executions == []
    assert first.closes == second.closes == 1


def test_user_exception_closes_materialized_child_before_error_complete(monkeypatch):
    escaped = []

    def consume(value):
        assert current_importer() is None and not value["child"].closed
        escaped.append(value["child"])
        raise ValueError("consumer failed after import")

    f = _Fixture(monkeypatch, consume)
    imports = _Imports(f, monkeypatch)
    backend = _ActualNodePublication(f)
    argument, descriptor = _stored_input(f, backend, imports.result_bytes(), object_id=imports.outer)
    f.push = replace(f.push, spec=replace(f.push.spec, args=(argument,)), dependencies=(descriptor,))
    reads = _route_input_reads(f, backend, monkeypatch)

    def complete(request):
        assert request.status is protocol.TaskReplyStatus.APPLICATION_ERROR
        assert escaped and escaped[0].closed and escaped[0].closes == 1
        assert not imports.table.snapshot(imports.child).borrowed_tokens
        return f.complete(request)

    f.on_complete = complete
    reply = f.worker._handle_push_task(f.push)

    assert reply.status is protocol.TaskReplyStatus.APPLICATION_ERROR
    assert "consumer failed after import" in reply.error.message
    assert f.executions == [True] and len(reads) == 1
    assert len(imports.exported_calls) == 1 and imports.nested_calls == []
    assert f.prepares == [] and backend.journal.publication_ids() == ()
    assert not getattr(f.worker, "_prepared_output_replies", {})
    assert current_importer() is None


def test_returned_materialized_child_stays_live_through_promotion_ack_replay(monkeypatch):
    reductions = []

    class Returned:
        def __init__(self, child):
            self.child = child

        def __reduce__(self):
            assert current_importer() is None and not self.child.closed
            reductions.append(True)
            return dict, ((("child", self.child),),)

    def consume(value):
        assert current_importer() is None
        return Returned(value["child"])

    f = _Fixture(monkeypatch, consume)
    imports = _Imports(f, monkeypatch)
    backend = _ActualNodePublication(f, child_tables={imports.owner: imports.table})
    f.push = replace(f.push, spec=replace(f.push.spec, args=(
        protocol.InlineArg(imports.result_bytes(), serializer="cloudpickle"),
    )))
    allow_ack = [False]
    promotions = []
    original_promote = backend.adapter._promote_child

    def promote(address, request):
        (child,) = imports.handles
        assert not child.closed and child.borrower in imports.table.snapshot(imports.child).borrowed_tokens
        reply = original_promote(address, request)
        promotions.append(request)
        assert request.transfer.final_hold in imports.table.snapshot(imports.child).contained_holds
        return reply

    def prepare(request):
        (child,) = imports.handles
        assert not child.closed and f.pending.nested_imports.acquired == (child,)
        assert f.pending.discovery.source_references == (child,)
        reply = backend.prepare(request)
        if not allow_ack[0]:
            raise TransportError("promotions applied before prepare ACK was lost")
        return reply

    def complete(request):
        (child,) = imports.handles
        assert child.closed and child.closes == 1 and f.pending.nested_imports is None
        assert not imports.table.snapshot(imports.child).borrowed_tokens
        return backend.complete(request)

    monkeypatch.setattr(backend.adapter, "_promote_child", promote)
    f.on_prepare, f.on_complete = prepare, complete
    with pytest.raises(RuntimeError, match="exact PushTask replay"):
        f.worker._handle_push_task(f.push)
    pending = f.pending
    outputs = pending.outputs
    (child,) = imports.handles
    assert len(promotions) == 1 and not child.closed
    assert pending.nested_imports.acquired == (child,) and not pending.prepare_acked
    assert backend.journal.snapshot(outputs.manifest.publication_id).complete is None
    assert COMPLETE_WORKER_LEASE_HANDLER not in f.handlers
    assert backend.ledger.available == ResourceVector({"CPU": 0})
    assert len(imports.exported_calls) == 1 and f.executions == reductions == [True]
    assert current_importer() is None

    allow_ack[0] = True
    reply = f.worker._handle_push_task(f.push)

    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert pending.outputs is outputs and reply.output_publication.manifest == outputs.manifest
    transfer = outputs.manifest.slots[0].transfers[0]
    assert isinstance(transfer.source, BorrowedContainedSource)
    assert transfer.source.original_source == protocol.ContainedTransferSource(imports.hold)
    assert transfer.final_hold in imports.table.snapshot(imports.child).contained_holds
    assert transfer.provisional_hold not in imports.table.snapshot(imports.child).contained_holds
    assert not imports.table.snapshot(imports.child).borrowed_tokens
    assert all(request == f.prepares[0] for request in f.prepares)
    assert len(promotions) == len(backend.completions) == 1
    assert child.closes == 1 and pending.nested_imports is None
    assert backend.ledger.available == ResourceVector({"CPU": 1})
    assert len(imports.exported_calls) == 1 and f.executions == reductions == [True]
    assert not f.worker._prepared_output_replies
    count = len(f.calls)
    assert f.worker._handle_push_task(f.push) is reply and len(f.calls) == count
    assert len(imports.exported_calls) == 1 and child.closes == 1


def test_same_child_from_distinct_containers_keeps_distinct_import_capabilities(monkeypatch):
    def consume(value, *, other):
        first, second = value["child"], other["child"]
        assert first == second and first is not second
        assert first.borrow_source != second.borrow_source
        assert not first.closed and not second.closed and current_importer() is None
        return True

    f = _Fixture(monkeypatch, consume)
    imports = _Imports(f, monkeypatch)
    backend = _ActualNodePublication(f)
    other_outer = ObjectID.for_task(TaskID.derive(f.push.spec.job_id, f.push.spec.task_id, 64))
    other_hold = ContainedReferenceHold(other_outer, imports.owner, "other-materialized-child")
    assert imports.table.add_contained_reference(imports.child, other_hold)
    first = protocol.InlineArg(imports.result_bytes(), serializer="cloudpickle")
    second = protocol.InlineArg(imports.result_bytes(hold=other_hold), serializer="cloudpickle")
    f.push = replace(f.push, spec=replace(f.push.spec, args=(first,), kwargs=(("other", second),)))

    reply = f.worker._handle_push_task(f.push)

    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert len(imports.exported_calls) == 2 and imports.nested_calls == []
    assert {call[3] for call in imports.exported_calls} == {imports.hold, other_hold}
    assert all(handle.closed and handle.closes == 1 for handle in imports.handles)
    assert not imports.table.snapshot(imports.child).borrowed_tokens
    assert len(backend.completions) == 1 and f.executions == [True]


def test_plain_inline_and_stored_values_do_not_require_lazy_core_or_worker_owner_server(monkeypatch):
    def consume(value, *, inline):
        assert current_importer() is None
        return value["number"] + inline

    f = _Fixture(monkeypatch, consume)
    del f.worker._server

    def forbidden(*_args, **_kwargs):
        pytest.fail("plain argument unnecessarily required an embedded Core or owner service")

    monkeypatch.setattr(f.worker, "_embedded_core_for", forbidden)
    monkeypatch.setattr(f.worker, "_owner_address", forbidden)
    backend = _ActualNodePublication(f)
    argument, descriptor = _stored_input(f, backend, cloudpickle.dumps({"number": 20}))
    f.push = replace(f.push, spec=replace(
        f.push.spec, args=(argument,),
        kwargs=(("inline", protocol.InlineArg(cloudpickle.dumps(22), serializer="cloudpickle")),),
    ), dependencies=(descriptor,))
    reads = _route_input_reads(f, backend, monkeypatch)

    reply = f.worker._handle_push_task(f.push)

    assert reply.status is protocol.TaskReplyStatus.SUCCEEDED
    assert cloudpickle.loads(reply.results[0].inline_data) == 42
    assert len(reads) == len(backend.completions) == 1
    assert f.executions == [True] and not hasattr(f.worker, "_server")
    assert not getattr(f.worker, "_embedded_core", None)
    assert not f.worker._prepared_output_replies and current_importer() is None
