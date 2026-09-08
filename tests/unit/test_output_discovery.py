"""Pure multi-slot discovery: no Core, RPC, pins, threads or processes."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, fields, is_dataclass, replace
from enum import Enum
import hashlib
import pickle
import weakref

import cloudpickle
import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.core import ObjectRef
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_discovery import DiscoveredOutputs, OutputDiscoverySession
from miniray.output_publication import (
    OutputPublicationConflictError, OutputPublicationHeader, OutputPublicationID,
    OutputPublicationNodeIncarnation,
)
from miniray.ref_transfer import (
    current_exporter, exporting_references, importing_references,
)
from miniray.publication_sources import (
    BorrowedContainedSource, OwnedContainedSource, PreparedContainedTransfer,
)
from miniray.task_outputs import (
    TargetExecutionKey, TargetOutputManifest, TaskExecutionKey, TaskOutputManifest,
)


pytestmark = pytest.mark.unit


def _header(count: int = 2, *, selected: tuple[int, ...] | None = None):
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 1)
    full = TaskOutputManifest.for_task(task, count)
    attempt = AttemptID(task, 0)
    execution = (
        TaskExecutionKey(full, attempt) if selected is None
        else TargetExecutionKey(
            TargetOutputManifest(full, tuple(full.output_ids[index] for index in selected)),
            attempt,
        )
    )
    return OutputPublicationHeader(
        OutputPublicationID(LeaseID.random(), execution), job,
        WorkerID.random(), WorkerID.random(),
        OutputPublicationNodeIncarnation(NodeID.random(), 12345, 1),
    )


def _owned(header, *, index=40, route=("127.0.0.1", 31001)):
    task = TaskID.derive(header.job_id, TaskID.for_driver(header.job_id), index)
    return ObjectRef(ObjectID.for_task(task), header.executor_worker_id, route)


def _borrowed(header, *, source=None, index=41):
    task = TaskID.derive(header.job_id, TaskID.for_driver(header.job_id), index)
    handle = ObjectRef(
        ObjectID.for_task(task), WorkerID.random(), ("127.0.0.1", 31002)
    )
    handle._borrower_token = "exact-borrower-token"
    handle._borrow_source = source or protocol.ContainedTransferSource(
        "original-containing-object"
    )
    return handle


def _assert_metadata(value):
    if type(value) in (JobID, TaskID, LeaseID, NodeID, WorkerID):
        assert type(value.value) is bytes and len(value.value) == 16
        return
    assert not isinstance(value, (bytes, bytearray, memoryview, ObjectRef))
    if value is None or isinstance(value, (str, int, Enum)):
        return
    if is_dataclass(value) and not isinstance(value, type):
        for field in fields(value):
            _assert_metadata(getattr(value, field.name))
        return
    assert isinstance(value, tuple)
    for item in value:
        _assert_metadata(item)


class _CountedReduction:
    def __init__(self, value, reductions):
        self.value = value
        self.reductions = reductions

    def __reduce__(self):
        self.reductions.append(self.value)
        return str, (self.value,)


class _Unserializable:
    def __reduce__(self):
        raise ValueError("later slot cannot serialize")


class _EphemeralOwnedReference:
    def __init__(self, header, weak_handles, index=99):
        self.header = header
        self.weak_handles = weak_handles
        self.index = index

    def __reduce__(self):
        reference = _owned(self.header, index=self.index)
        self.weak_handles.append(weakref.ref(reference))
        # Only the exporter, not this reducer wrapper, keeps the created ref.
        return list, ((reference,),)


def test_each_selected_slot_is_serialized_once_before_batch_becomes_observable():
    header = _header(3)
    reductions = []
    session = OutputDiscoverySession(header, inline_threshold=1024)
    observed = []

    class _Observe:
        def __reduce__(self):
            observed.append(session.discovered)
            return int, (7,)

    result = session.discover((
        _CountedReduction("first", reductions), _Observe(),
        _CountedReduction("last", reductions),
    ))
    assert reductions == ["first", "last"]
    assert observed == [None]
    assert session.discovered is result
    assert tuple(cloudpickle.loads(payload) for payload in result.slot_payloads) == (
        "first", 7, "last",
    )
    with pytest.raises(RuntimeError, match="one-shot"):
        session.discover((1, 2, 3))
    assert reductions == ["first", "last"]


def test_per_slot_threshold_equality_does_not_apply_argument_cumulative_budget():
    header = _header(3)
    small = b"x"
    threshold = len(cloudpickle.dumps(small))
    session = OutputDiscoverySession(header, inline_threshold=threshold)
    result = session.discover((small, small, b"y" * 64))
    assert tuple(slot.tier for slot in result.manifest.slots) == (
        protocol.ResultStorage.INLINE, protocol.ResultStorage.INLINE,
        protocol.ResultStorage.OBJECT_STORE,
    )
    assert result.manifest.slots[0].size_bytes == threshold
    assert result.manifest.slots[1].size_bytes == threshold
    assert sum(slot.size_bytes for slot in result.manifest.slots[:2]) > threshold
    assert result.manifest.to_graph_manifest() is None


def test_same_python_ref_memoizes_per_slot_but_siblings_have_independent_holds():
    header = _header()
    child = _owned(header)
    result_session = OutputDiscoverySession(header, inline_threshold=2048)
    result = result_session.discover(([child, child], {"child": child}))
    first, second = result.manifest.slots
    assert len(first.transfers) == len(second.transfers) == 1
    left, right = first.transfers[0], second.transfers[0]
    assert left.contained_object_id == right.contained_object_id == child.object_id
    assert left.final_hold != right.final_hold
    assert left.final_hold.container_object_id == header.publication_id.output_ids[0]
    assert right.final_hold.container_object_id == header.publication_id.output_ids[1]
    assert result_session.source_references == (child, child)
    imports = []

    def restore(*identity):
        imports.append(identity)
        return object()

    with importing_references(restore):
        decoded_first = cloudpickle.loads(result.slot_payloads[0])
        decoded_second = cloudpickle.loads(result.slot_payloads[1])
    assert decoded_first[0] is decoded_first[1]
    assert decoded_first[0] is not decoded_second["child"]
    assert tuple(identity[3] for identity in imports) == (left.final_hold, right.final_hold)
    assert len(result.manifest.to_graph_manifest().ordered_edges) == 2


def test_distinct_python_handles_for_one_child_keep_existing_export_semantics():
    header = _header(1)
    first = _owned(header)
    second = ObjectRef(first.object_id, first.owner_worker_id, first.owner_address)
    assert first == second and first is not second
    session = OutputDiscoverySession(header, inline_threshold=2048)
    result = session.discover(([first, second],))
    transfers = result.manifest.slots[0].transfers
    assert len(transfers) == 2
    assert transfers[0].contained_object_id == transfers[1].contained_object_id
    assert transfers[0].final_hold != transfers[1].final_hold
    assert session.source_references == (first, second)


@pytest.mark.parametrize("source_kind", ("contained", "task"))
def test_owned_and_borrowed_sources_are_exact_metadata_with_no_pin_or_rpc(
    source_kind, monkeypatch,
):
    header = _header()
    upstream_task = TaskID.random()
    source = (
        protocol.ContainedTransferSource("exact-upstream")
        if source_kind == "contained"
        else protocol.TaskHoldSource(protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, header.executor_worker_id,
            upstream_task, AttemptID(upstream_task, 0),
        ))
    )
    owned = _owned(header)
    borrowed = _borrowed(header, source=source)
    route_calls = []
    monkeypatch.setattr(
        ObjectRef, "close", lambda self: pytest.fail("discovery must not close a user handle")
    )
    session = OutputDiscoverySession(
        header, inline_threshold=1,
        owner_address=lambda: route_calls.append("local-route") or ("127.0.0.1", 31003),
    )
    result = session.discover((owned, {"borrowed": borrowed}))
    assert route_calls == ["local-route"]
    owned_transfer = result.manifest.slots[0].transfers[0]
    borrowed_transfer = result.manifest.slots[1].transfers[0]
    assert owned_transfer.source == OwnedContainedSource(header.executor_worker_id)
    assert owned_transfer.contained_owner_address == ("127.0.0.1", 31003)
    assert borrowed_transfer.source == BorrowedContainedSource(
        header.executor_worker_id, borrowed.borrower_token, source,
    )
    assert borrowed_transfer.contained_owner_address == borrowed.owner_address
    for transfer in (owned_transfer, borrowed_transfer):
        assert transfer.provisional_hold.container_owner_worker_id == header.executor_worker_id
        assert transfer.final_hold.container_owner_worker_id == header.owner_worker_id
    session.release_sources_after_promotions()
    assert session.source_references == ()


def test_discovery_metadata_and_payloads_are_separate_and_wire_roundtrip_exact():
    header = _header()
    child = _owned(header)
    session = OutputDiscoverySession(header, inline_threshold=1024)
    result = session.discover(({"child": child}, b"P" * 4096))
    assert tuple(slot.tier for slot in result.manifest.slots) == (
        protocol.ResultStorage.INLINE, protocol.ResultStorage.OBJECT_STORE,
    )
    _assert_metadata(result.manifest)
    _assert_metadata(result.manifest.to_graph_manifest())
    assert result == pickle.loads(pickle.dumps(result))
    for slot, payload in zip(result.manifest.slots, result.slot_payloads):
        assert slot.size_bytes == len(payload)
        assert slot.checksum == hashlib.sha256(payload).hexdigest()
    with pytest.raises(TypeError, match="local custody"):
        cloudpickle.dumps(session)
    with pytest.raises(FrozenInstanceError):
        result.slot_payloads = ()


def test_target_subset_uses_original_slot_indices_and_stable_tokens():
    header = _header(4, selected=(1, 3))
    child = _owned(header)
    sessions = [OutputDiscoverySession(header, inline_threshold=1024) for _ in range(2)]
    first, replay = (session.discover((child, child)) for session in sessions)
    assert first == replay
    assert tuple(slot.object_id.return_index for slot in first.manifest.slots) == (1, 3)
    assert first.manifest.execution.full_output_ids == header.publication_id.full_output_ids
    calls = []
    custom = OutputDiscoverySession(
        header, inline_threshold=1024,
        token_factory=lambda slot, transfer: calls.append((slot, transfer)) or "fixed-token",
    ).discover((child, child))
    assert calls == [(1, 0), (3, 0)]
    left, right = (slot.transfers[0].final_hold for slot in custom.manifest.slots)
    assert left.transfer_token == right.transfer_token == "fixed-token"
    assert left != right
    changed = replace(header, publication_id=replace(
        header.publication_id, lease_id=LeaseID.random()
    ))
    other = OutputDiscoverySession(changed, inline_threshold=1024).discover((child, child))
    assert first.manifest.slots[0].transfers[0].final_hold != other.manifest.slots[0].transfers[0].final_hold


def test_discovery_retains_ephemeral_sources_until_explicit_all_promotions_release():
    header = _header()
    weak_handles = []
    session = OutputDiscoverySession(header, inline_threshold=1024)
    result = session.discover((
        _EphemeralOwnedReference(header, weak_handles, index=99),
        _EphemeralOwnedReference(header, weak_handles, index=100),
    ))
    assert len(weak_handles) == 2
    assert all(reference() is not None for reference in weak_handles)
    assert len(session.source_references) == 2
    assert session.discovered is result
    session.release_sources_after_promotions()
    session.release_sources_after_promotions()
    assert session.discovered is result
    assert session.source_references == ()
    assert all(reference() is None for reference in weak_handles)


def test_failure_in_later_slot_clears_prior_source_custody_and_forbids_retry():
    header = _header()
    weak_handles = []
    session = OutputDiscoverySession(header, inline_threshold=1024)

    def attempt():
        try:
            session.discover((_EphemeralOwnedReference(header, weak_handles), _Unserializable()))
        except ValueError as error:
            assert str(error) == "later slot cannot serialize"
        else:
            pytest.fail("bad later slot unexpectedly serialized")

    attempt()
    assert session.source_references == ()
    assert session.discovered is None
    assert weak_handles[0]() is None
    with pytest.raises(RuntimeError, match="one-shot"):
        session.discover((1, 2))
    session.abort()
    with pytest.raises(RuntimeError, match="complete discovered batch"):
        session.release_sources_after_promotions()


def test_abort_clears_local_custody_without_mutating_the_returned_data_plane_batch():
    header = _header(1)
    weak_handles = []
    session = OutputDiscoverySession(header, inline_threshold=1024)
    result = session.discover((_EphemeralOwnedReference(header, weak_handles),))
    assert weak_handles[0]() is not None
    session.abort()
    session.abort()
    assert session.discovered is None and session.source_references == ()
    assert weak_handles[0]() is None
    assert result.manifest.slots[0].transfers


@pytest.mark.parametrize("invalid", ("closed", "no-route", "no-token", "no-source", "owned-borrower"))
def test_invalid_live_source_fails_before_a_batch_or_remote_effect_exists(invalid):
    header = _header(1)
    child = _borrowed(header)
    if invalid == "closed":
        child._closed = True
    elif invalid == "no-route":
        child._owner_address = None
    elif invalid == "no-token":
        child._borrower_token = None
    elif invalid == "no-source":
        child._borrow_source = None
    else:
        child._owner_worker_id = header.executor_worker_id
    session = OutputDiscoverySession(header, inline_threshold=1024)
    with pytest.raises((ValueError, RuntimeError)):
        session.discover((child,))
    assert session.discovered is None and session.source_references == ()


def test_invalid_owned_credentials_are_rejected_before_route_provider_runs():
    header = _header(1)
    child = _owned(header)
    child._borrower_token = "not-an-owned-handle"
    session = OutputDiscoverySession(
        header, inline_threshold=1024,
        owner_address=lambda: pytest.fail("conflicting ownership reached route lookup"),
    )
    with pytest.raises(ValueError, match="conflicting borrower"):
        session.discover((child,))


def test_discovery_restores_outer_export_scope_after_success_and_failure():
    header = _header(1)
    exporter = lambda _ref: pytest.fail("outer exporter was used inside discovery")
    with exporting_references(exporter):
        OutputDiscoverySession(header, inline_threshold=1024).discover((7,))
        assert current_exporter() is exporter
        with pytest.raises(ValueError, match="later slot"):
            OutputDiscoverySession(header, inline_threshold=1024).discover((_Unserializable(),))
        assert current_exporter() is exporter
    assert current_exporter() is None


def test_late_slot_identity_conflict_aborts_the_complete_batch_before_effects():
    header = _header()
    first = _borrowed(header)
    second = ObjectRef(first.object_id, WorkerID.random(), first.owner_address)
    second._borrower_token = "different-owner-borrow"
    second._borrow_source = protocol.ContainedTransferSource("different-owner-source")
    session = OutputDiscoverySession(header, inline_threshold=1024)
    with pytest.raises(OutputPublicationConflictError, match="conflicting owners"):
        session.discover((first, second))
    assert session.discovered is None
    assert session.source_references == ()


def test_reentrant_reducer_cannot_release_or_restart_discovery_custody():
    header = _header(1)
    session = OutputDiscoverySession(header, inline_threshold=1024)
    rejected = []

    class _Reenter:
        def __reduce__(self):
            for operation in (session.abort, session.release_sources_after_promotions):
                with pytest.raises(RuntimeError):
                    operation()
                rejected.append(operation.__name__)
            with pytest.raises(RuntimeError, match="one-shot"):
                session.discover((0,))
            return int, (7,)

    result = session.discover((_Reenter(),))
    assert cloudpickle.loads(result.slot_payloads[0]) == 7
    assert rejected == ["abort", "release_sources_after_promotions"]
    assert session.discovered is result


@pytest.mark.parametrize("values", ((), (1,), (1, 2, 3), b"not-a-slot-sequence", {0: 1, 1: 2}))
def test_wrong_selected_manifest_shape_never_invokes_any_reducer(values):
    session = OutputDiscoverySession(_header(), inline_threshold=1024)
    with pytest.raises((TypeError, ValueError), match="selected|slots"):
        session.discover(values)
    assert session.discovered is None and session.source_references == ()


@pytest.mark.parametrize("threshold", (True, -1, 1.0, "1024", None))
def test_threshold_requires_nonnegative_integer(threshold):
    with pytest.raises(ValueError, match="inline_threshold"):
        OutputDiscoverySession(_header(), inline_threshold=threshold)


def test_bad_token_or_final_manifest_rejection_clears_all_source_handles():
    header = _header(1)
    child = _owned(header)
    for token in (None, "", 1):
        session = OutputDiscoverySession(
            header, inline_threshold=1024, token_factory=lambda *_args: token
        )
        with pytest.raises(ValueError, match="non-empty string"):
            session.discover((child,))
        assert session.source_references == () and session.discovered is None
    duplicate = ObjectRef(child.object_id, child.owner_worker_id, child.owner_address)
    session = OutputDiscoverySession(
        header, inline_threshold=1024, token_factory=lambda *_args: "duplicate"
    )
    with pytest.raises(OutputPublicationConflictError, match="child hold"):
        session.discover(([child, duplicate],))
    assert session.source_references == () and session.discovered is None


def test_discovered_payload_batch_rejects_partial_or_corrupted_streams():
    result = OutputDiscoverySession(_header(), inline_threshold=1024).discover((1, 2))
    with pytest.raises(ValueError, match="exactly cover"):
        replace(result, slot_payloads=result.slot_payloads[:1])
    with pytest.raises(ValueError, match="does not match"):
        replace(result, slot_payloads=(b"wrong", result.slot_payloads[1]))
    with pytest.raises(TypeError, match="serialized bytes"):
        replace(result, slot_payloads=(bytearray(result.slot_payloads[0]), result.slot_payloads[1]))


def test_discovery_uses_explicit_owner_route_and_embeds_exact_final_hold():
    header = _header(1)
    child = _owned(header, route=None)
    address = ("127.0.0.1", 31009)
    outer = header.publication_id.output_ids[0]
    expected = PreparedContainedTransfer(
        child.object_id, header.executor_worker_id, address,
        OwnedContainedSource(header.executor_worker_id),
        ContainedReferenceHold(outer, header.executor_worker_id, "same-token"),
        ContainedReferenceHold(outer, header.owner_worker_id, "same-token"),
    )
    wire_identity = (
        child.object_id, header.executor_worker_id, address, expected.final_hold,
    )

    def expected_export(reference):
        assert reference is child
        return wire_identity

    # Compare the real ObjectRef reducer's wire shape with an independently
    # specified final-custody identity, without a retired publication session.
    with exporting_references(expected_export):
        payload = cloudpickle.dumps(child)
    session = OutputDiscoverySession(
        header, inline_threshold=1024, owner_address=address,
        token_factory=lambda _slot, _index: "same-token",
    )
    result = session.discover((child,))
    assert result.slot_payloads == (payload,)
    assert result.manifest.slots[0].transfers == (expected,)
    assert session.source_references == (child,)
    assert child.owner_address is None and not child.closed
    assert current_exporter() is None
    imports = []
    restored = object()

    def restore(*identity):
        imports.append(identity)
        return restored

    with importing_references(restore):
        assert cloudpickle.loads(result.slot_payloads[0]) is restored
    assert imports == [wire_identity]
    session.release_sources_after_promotions()
    assert session.source_references == () and session.discovered is result
    assert not child.closed
