"""Stored ObjectRef contracts with explicit pure and live-reference modes.

The owner RPC is a logical metadata lookup.  Stored bytes must instead travel
from the descriptor's Node directly to the borrower, with the complete
producer identity and integrity tuple acting as a fence.

All five contracts are synchronous state combinations: one or two threadless
Cores, one borrowed reference and at most 4 KiB payload. Real owner/borrower
reducers run through the existing synchronous release mailbox. Transport and
notifier callbacks model boundaries; no runtime, thread, socket or real wait
is started and no physical store or process-lifecycle claim is made.
"""

from __future__ import annotations

import hashlib
import threading
import time
from contextlib import contextmanager
from dataclasses import replace
from typing import Iterator

import cloudpickle
import pytest

from miniray import protocol
from miniray.core import _HomeRoute
from miniray.core import CoreWorker, ObjectRef
from miniray.contained_edges import ContainedReferenceHold
from miniray.errors import ProtocolError, SystemTaskError
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import ObjectOwnerTable
from miniray.ref_transfer import exporting_references
from miniray.runtime_binding import ExecutionContext, bind_runtime
from miniray.worker import WorkerServer
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_foreign_wait_drop import _BorrowMailbox, _no_runtime as _guard_runtime




@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    failed, _forbidden = _guard_runtime(monkeypatch)
    yield
    assert failed == [False]


def _identity(index: int = 0) -> tuple[ObjectID, AttemptID]:
    job_id = JobID.random()
    task_id = TaskID.derive(job_id, TaskID.for_driver(job_id), index)
    return ObjectID.for_task(task_id), AttemptID(task_id, 0)


def _bare_core(
    *,
    worker_id: WorkerID | None = None,
    node_id: NodeID | None = None,
    node_address: tuple[str, int] = ("127.0.0.1", 27001),
) -> CoreWorker:
    """Build only the owner/borrower half of CoreWorker for pure tests."""

    core = make_pure_core()
    core.worker_id = worker_id or core.worker_id
    core.node_id = node_id or core.node_id
    core.node_address = node_address
    core._home_route = _HomeRoute(core.node_id, core.node_address, core._membership_epoch)
    return core


def _stop(core: CoreWorker) -> None:
    close_pure_core(core)


def _descriptor(
    *,
    object_id: ObjectID | None = None,
    attempt_id: AttemptID | None = None,
    owner_worker_id: WorkerID | None = None,
    node_id: NodeID | None = None,
    payload: bytes = b"stored-payload",
) -> protocol.ObjectStoreDescriptor:
    if object_id is None:
        object_id, generated_attempt = _identity()
        attempt_id = generated_attempt
    elif attempt_id is None:
        attempt_id = AttemptID(object_id.task_id, 0)
    assert attempt_id is not None
    return protocol.ObjectStoreDescriptor(
        object_id=object_id,
        owner_worker_id=owner_worker_id or WorkerID.random(),
        producer_attempt_id=attempt_id,
        node_id=node_id or NodeID.random(),
        size_bytes=len(payload),
        checksum=hashlib.sha256(payload).hexdigest(),
    )


def _owned_reply(
    descriptor: protocol.ObjectStoreDescriptor,
    borrower: WorkerID,
    **changes: object,
) -> protocol.GetOwnedObjectReply:
    values: dict[str, object] = {
        "object_id": descriptor.object_id,
        "owner_worker_id": descriptor.owner_worker_id,
        "borrower_worker_id": borrower,
        "borrower_token": "borrow-token",
        "accepted": True,
        "state": protocol.OwnedObjectState.READY_STORED,
        "current_attempt": descriptor.producer_attempt_id,
        "descriptor": descriptor,
    }
    values.update(changes)
    return protocol.GetOwnedObjectReply(**values)


@pytest.mark.unit
def test_get_owned_object_reply_is_a_strict_state_payload_sum_type() -> None:
    descriptor = _descriptor()
    borrower = WorkerID.random()
    common = dict(
        object_id=descriptor.object_id,
        owner_worker_id=descriptor.owner_worker_id,
        borrower_worker_id=borrower,
        borrower_token="borrow-token",
    )
    error = protocol.RemoteErrorInfo("ValueError", "bad value")

    valid = (
        protocol.GetOwnedObjectReply(
            **common, accepted=False, detail="inactive borrower"
        ),
        protocol.GetOwnedObjectReply(
            **common, accepted=True, state=protocol.OwnedObjectState.PENDING
        ),
        protocol.GetOwnedObjectReply(
            **common, accepted=True, state=protocol.OwnedObjectState.READY_INLINE,
            data=b"inline",
        ),
        protocol.GetOwnedObjectReply(
            **common, accepted=True, state=protocol.OwnedObjectState.READY_STORED,
            descriptor=descriptor,
        ),
        protocol.GetOwnedObjectReply(
            **common, accepted=True, state=protocol.OwnedObjectState.ERROR,
            error=error,
        ),
        protocol.GetOwnedObjectReply(
            **common, accepted=True, state=protocol.OwnedObjectState.LOST
        ),
    )
    assert [(item.data, item.error, item.descriptor) for item in valid] == [
        (None, None, None),
        (None, None, None),
        (b"inline", None, None),
        (None, None, descriptor),
        (None, error, None),
        (None, None, None),
    ]

    other_object, other_attempt = _identity(1)
    wrong_object = _descriptor(
        object_id=other_object,
        attempt_id=other_attempt,
        owner_worker_id=descriptor.owner_worker_id,
    )
    wrong_owner = replace(descriptor, owner_worker_id=WorkerID.random())
    invalid = (
        dict(accepted=False),
        dict(accepted=False, detail="rejected", state=protocol.OwnedObjectState.LOST),
        dict(accepted=True),
        dict(accepted=True, state=protocol.OwnedObjectState.PENDING, data=b"bytes"),
        dict(accepted=True, state=protocol.OwnedObjectState.READY_INLINE),
        dict(
            accepted=True, state=protocol.OwnedObjectState.READY_INLINE,
            data=b"bytes", descriptor=descriptor,
        ),
        dict(accepted=True, state=protocol.OwnedObjectState.READY_STORED),
        dict(
            accepted=True, state=protocol.OwnedObjectState.READY_STORED,
            descriptor=descriptor, data=b"owner must not proxy bytes",
        ),
        dict(
            accepted=True, state=protocol.OwnedObjectState.READY_STORED,
            descriptor=wrong_object,
        ),
        dict(
            accepted=True, state=protocol.OwnedObjectState.READY_STORED,
            descriptor=wrong_owner,
        ),
        dict(accepted=True, state=protocol.OwnedObjectState.ERROR),
        dict(
            accepted=True, state=protocol.OwnedObjectState.ERROR,
            error=error, data=b"bytes",
        ),
        dict(
            accepted=True, state=protocol.OwnedObjectState.LOST,
            descriptor=descriptor,
        ),
        dict(
            accepted=True, state=protocol.OwnedObjectState.PENDING,
            detail="accepted replies cannot reject",
        ),
    )
    for changes in invalid:
        with pytest.raises(ProtocolError):
            protocol.GetOwnedObjectReply(**common, **changes)


@pytest.mark.unit
def test_owner_and_worker_publish_only_canonical_live_stored_metadata() -> None:
    owner = _bare_core()
    object_id, attempt_id = _identity()
    lower_node = NodeID(bytes([1]) * 16)
    higher_node = NodeID(bytes([2]) * 16)
    payload = cloudpickle.dumps({"stored": b"x" * 4096})
    checksum = hashlib.sha256(payload).hexdigest()
    owner.owner_table.register(object_id, current_attempt=attempt_id)
    assert owner.owner_table.publish_stored(object_id, attempt_id, higher_node)
    assert owner.owner_table.add_location(object_id, attempt_id, lower_node)
    owner._stored_descriptors[object_id] = protocol.ResultDescriptor(
        object_id=object_id,
        storage=protocol.ResultStorage.OBJECT_STORE,
        size_bytes=len(payload),
        owner_worker_id=owner.worker_id,
        node_id=higher_node,
        checksum=checksum,
    )
    borrower = WorkerID.random()
    token = (borrower, "borrow-token")
    owner.owner_table.add_borrowed_reference(object_id, token)
    request = protocol.GetOwnedObject(
        object_id, owner.worker_id, borrower, "borrow-token"
    )
    worker = object.__new__(WorkerServer)
    worker.worker_id = owner.worker_id
    worker._embedded_core = owner
    worker._embedded_core_lock = threading.Lock()
    try:
        reply = worker._handle_get_owned_object(request)
        assert reply == protocol.GetOwnedObjectReply(
            object_id=object_id,
            owner_worker_id=owner.worker_id,
            borrower_worker_id=borrower,
                borrower_token="borrow-token",
                accepted=True,
                state=protocol.OwnedObjectState.READY_STORED,
                current_attempt=attempt_id,
                descriptor=protocol.ObjectStoreDescriptor(
                object_id=object_id,
                owner_worker_id=owner.worker_id,
                producer_attempt_id=attempt_id,
                node_id=lower_node,
                size_bytes=len(payload),
                checksum=checksum,
            ),
        )
        assert reply.data is None and reply.error is None and reply.detail is None

        # The owner must not invent a route or proxy bytes when its durable
        # stored-result metadata is incomplete.
        del owner._stored_descriptors[object_id]
        rejected = worker._handle_get_owned_object(request)
        assert not rejected.accepted
        assert "metadata is incomplete" in (rejected.detail or "")
        assert (rejected.state, rejected.data, rejected.error, rejected.descriptor) == (
            None, None, None, None
        )
    finally:
        _stop(owner)


class _RecordingNotifier:
    def __init__(self) -> None:
        self.depth = 0
        self.events: list[str] = []

    @contextmanager
    def blocking_scope(self) -> Iterator[None]:
        assert self.depth == 0
        self.depth = 1
        self.events.append("blocked")
        try:
            yield
        finally:
            self.events.append("unblocked")
            self.depth = 0


@pytest.mark.unit
def test_nested_ref_restore_then_stored_get_uses_owner_metadata_and_node_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner_address = ("127.0.0.1", 27002)
    source_address = ("127.0.0.1", 27003)
    gcs_address = ("127.0.0.1", 27004)
    source_node = NodeID(bytes([3]) * 16)
    borrower_node = NodeID(bytes([4]) * 16)
    owner = _bare_core(worker_id=WorkerID.random(), node_id=source_node)
    borrower = _bare_core(worker_id=WorkerID.random(), node_id=borrower_node)
    borrower.gcs_address = gcs_address
    object_id, attempt_id = _identity()
    value = {"kind": "stored", "blob": b"z" * 4096}
    stored_payload = cloudpickle.dumps(value)
    checksum = hashlib.sha256(stored_payload).hexdigest()
    owner.owner_table.register(object_id, current_attempt=attempt_id)
    assert owner.owner_table.publish_stored(object_id, attempt_id, source_node)
    owner._stored_descriptors[object_id] = protocol.ResultDescriptor(
        object_id=object_id,
        storage=protocol.ResultStorage.OBJECT_STORE,
        size_bytes=len(stored_payload),
        owner_worker_id=owner.worker_id,
        node_id=source_node,
        checksum=checksum,
    )

    # Install one exact owner pin and serialize the supported scoped reducer.
    # This models contained bytes, not a complete outer Task publication.
    hold = ContainedReferenceHold(ObjectID.for_task(TaskID.random()), owner.worker_id, "stored-child-source")
    assert owner.owner_table.add_contained_reference(object_id, hold)
    child = ObjectRef(object_id, owner.worker_id, owner_address)
    with exporting_references(lambda reference: (reference.object_id, owner.worker_id, owner_address, hold)):
        inline_outer = cloudpickle.dumps({"layers": [{"children": (child,)}]})
    release_failures = [False]
    borrower._reference_mailbox = _BorrowMailbox(borrower, release_failures)

    owner_calls: list[tuple[str, object, object]] = []

    def owner_rpc(
        address: object, handler: str, request: object, **_kwargs: object
    ) -> object:
        assert address == owner_address
        if handler == "acquire_borrowed_object":
            reply = owner.acquire_exported_reference(request)
        elif handler == "get_owned_object":
            reply = owner.get_owned_object(request)
        elif handler == "release_borrowed_object":
            reply = owner.release_borrowed_reference(request)
        else:
            raise AssertionError(handler)
        owner_calls.append((handler, request, reply))
        return reply

    notifier = _RecordingNotifier()
    node_calls: list[tuple[object, str, object]] = []

    deadline_calls: list[tuple[str, float, float]] = []

    def data_rpc(
        address: object, handler: str, request: object,
        *, connect_timeout: float, request_timeout: float | None,
        **_transport: object,
    ) -> object:
        assert connect_timeout > 0
        assert request_timeout is not None and request_timeout > 0
        deadline_calls.append((handler, connect_timeout, request_timeout))
        if handler == "get_owned_object":
            prior_gets = sum(
                recorded_handler == "get_owned_object"
                for recorded_handler, _request, _reply in owner_calls
            )
            assert notifier.depth == (0 if prior_gets == 0 else 1)
            assert address == owner_address
            reply = owner.get_owned_object(request)
            owner_calls.append((handler, request, reply))
            return reply
        if handler == "release_borrowed_object":
            assert address == owner_address
            reply = owner.release_borrowed_reference(request)
            owner_calls.append((handler, request, reply))
            return reply
        node_calls.append((address, handler, request))
        if handler == "get_node_address":
            assert notifier.depth == 1
            assert address == gcs_address
            assert request == protocol.GetNodeAddress(source_node)
            return protocol.GetNodeAddressReply(
                source_node, True, address=source_address
            )
        if handler == "get_object":
            assert notifier.depth == 1
            assert address == source_address
            assert request == protocol.GetObject(
                object_id=object_id,
                requester_node_id=borrower_node,
                expected_attempt_id=attempt_id,
                expected_owner_worker_id=owner.worker_id,
                expected_size_bytes=len(stored_payload),
                expected_checksum=checksum,
            )
            return protocol.GetObjectReply(
                object_id=object_id,
                node_id=source_node,
                found=True,
                sealed=True,
                data=stored_payload,
                checksum=checksum,
                producer_attempt_id=attempt_id,
                owner_worker_id=owner.worker_id,
                size_bytes=len(stored_payload),
            )
        raise AssertionError(handler)

    monkeypatch.setattr(borrower, "_borrow_rpc", owner_rpc)
    monkeypatch.setattr(borrower, "_borrow_rpc_with_deadline",
        lambda address, handler, request, remaining: CoreWorker._borrow_rpc_with_deadline(
            borrower, address, handler, request, remaining))
    monkeypatch.setattr("miniray.core.rpc_request", data_rpc)
    restored = borrower._loads_owned_value(inline_outer)
    borrowed = restored["layers"][0]["children"][0]
    try:
        assert isinstance(borrowed, ObjectRef)
        assert borrowed.object_id == object_id
        assert borrowed.owner_worker_id == owner.worker_id
        assert borrowed.owner_address == owner_address
        assert borrowed.borrower_token is not None

        parent_task = TaskID.derive(
            borrower.job_id, borrower.driver_task_id, 11
        )
        context = ExecutionContext(
            borrower.job_id, parent_task, AttemptID(parent_task, 0),
            blocking_notifier=notifier,
        )
        with bind_runtime(borrower, context):
            assert borrower.get(borrowed, timeout=1.0) == value

        assert notifier.events == ["blocked", "unblocked"]
        assert [handler for handler, _, _ in deadline_calls] == [
            "get_owned_object", "get_node_address", "get_object",
            "get_owned_object",
        ]
        assert all(
            connect_timeout + request_timeout <= 1.0
            for _, connect_timeout, request_timeout in deadline_calls
        )
        assert [handler for _, handler, _ in node_calls] == [
            "get_node_address", "get_object"
        ]
        owned_replies = [
            reply
            for handler, _request, reply in owner_calls
            if handler == "get_owned_object"
        ]
        assert len(owned_replies) == 2
        assert all(
            reply.state is protocol.OwnedObjectState.READY_STORED
            and reply.data is None
            and reply.descriptor == _descriptor(
                object_id=object_id, attempt_id=attempt_id,
                owner_worker_id=owner.worker_id, node_id=source_node,
                payload=stored_payload,
            )
            for reply in owned_replies
        )
        borrowed.close(timeout=0)
        assert borrower._reference_mailbox.events.empty()
        assert borrower._reference_mailbox.events.unfinished_tasks == 0
        assert not release_failures[0]
        assert not owner.owner_table.snapshot(object_id).borrowed_tokens
        assert owner.owner_table.release_contained_reference(object_id, hold)
    finally:
        borrowed.close(timeout=0)
        _stop(borrower)
        _stop(owner)


@pytest.mark.parametrize(
    "corruption",
    (
        "wrong_object",
        "wrong_node",
        "wrong_attempt",
        "wrong_owner",
        "wrong_size",
        "wrong_checksum",
        "corrupt_bytes",
        "missing_metadata",
        "missing_replica",
    ),
)
@pytest.mark.unit
def test_borrower_rejects_every_stored_reply_identity_or_integrity_mismatch(
    corruption: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = cloudpickle.dumps({"value": b"v" * 256})
    descriptor = _descriptor(payload=payload)
    core = _bare_core(node_id=NodeID.random())
    source_address = ("127.0.0.1", 27005)
    expected_request = protocol.GetObject(
        object_id=descriptor.object_id,
        requester_node_id=core.node_id,
        expected_attempt_id=descriptor.producer_attempt_id,
        expected_owner_worker_id=descriptor.owner_worker_id,
        expected_size_bytes=descriptor.size_bytes,
        expected_checksum=descriptor.checksum,
    )
    requests: list[protocol.GetObject] = []

    def corrupt_reply() -> protocol.GetObjectReply:
        values: dict[str, object] = dict(
            object_id=descriptor.object_id,
            node_id=descriptor.node_id,
            found=True,
            sealed=True,
            data=payload,
            checksum=descriptor.checksum,
            producer_attempt_id=descriptor.producer_attempt_id,
            owner_worker_id=descriptor.owner_worker_id,
            size_bytes=descriptor.size_bytes,
        )
        if corruption == "wrong_object":
            other_object, other_attempt = _identity(2)
            values.update(object_id=other_object, producer_attempt_id=other_attempt)
        elif corruption == "wrong_node":
            values["node_id"] = NodeID.random()
        elif corruption == "wrong_attempt":
            values["producer_attempt_id"] = descriptor.producer_attempt_id.next()
        elif corruption == "wrong_owner":
            values["owner_worker_id"] = WorkerID.random()
        elif corruption == "wrong_size":
            larger = payload + b"!"
            values.update(
                data=larger, size_bytes=len(larger),
                checksum=hashlib.sha256(larger).hexdigest(),
            )
        elif corruption in ("wrong_checksum", "corrupt_bytes"):
            alternate = bytes([payload[0] ^ 1]) + payload[1:]
            values.update(
                data=alternate, checksum=hashlib.sha256(alternate).hexdigest()
            )
        elif corruption == "missing_metadata":
            values.update(
                producer_attempt_id=None, owner_worker_id=None, size_bytes=None
            )
        elif corruption == "missing_replica":
            return protocol.GetObjectReply(
                descriptor.object_id, descriptor.node_id, False, False,
                error="replica is missing",
            )
        return protocol.GetObjectReply(**values)

    monkeypatch.setattr(
        core, "_resolve_node_address",
        lambda node_id, *, home_route=None: source_address if node_id == descriptor.node_id else None,
    )

    def rpc(address: object, handler: str, request: object) -> object:
        assert address == source_address and handler == "get_object"
        assert isinstance(request, protocol.GetObject)
        requests.append(request)
        return corrupt_reply()

    monkeypatch.setattr(core, "_rpc", rpc)
    try:
        with pytest.raises(SystemTaskError):
            core._fetch_borrowed_stored_object(descriptor)
        assert requests == [expected_request]
    finally:
        _stop(core)


@pytest.mark.unit
def test_stored_fetch_subtracts_resolution_time_and_never_starts_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor = _descriptor(payload=cloudpickle.dumps("stored"))
    core = make_pure_core()
    source_address = ("127.0.0.1", 27006)
    resolutions: list[tuple[NodeID, float | None]] = []
    object_rpcs: list[object] = []

    def resolve(node_id: NodeID, timeout: float | None, *, home_route) -> tuple[str, int]:
        assert home_route == core._home_route
        resolutions.append((node_id, timeout))
        return source_address

    monotonic_values = iter((10.0, 10.6))
    monkeypatch.setattr(
        core, "_resolve_node_address_with_timeout", resolve
    )
    monkeypatch.setattr(
        "miniray.core.time.monotonic", lambda: next(monotonic_values)
    )
    monkeypatch.setattr(
        "miniray.core.rpc_request",
        lambda *args, **kwargs: object_rpcs.append((args, kwargs)),
    )

    with pytest.raises(TimeoutError, match="before timeout"):
        core._fetch_borrowed_stored_object(descriptor, timeout=0.5)

    assert resolutions == [(descriptor.node_id, 0.5)]
    assert object_rpcs == []
