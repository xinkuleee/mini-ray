"""Pure single-output discovery: no Core, RPC, pins, threads or processes.

Detached ObjectRefs carry explicit source metadata for serialization tests;
these cases do not claim live borrower admission or remote hold acquisition.
"""

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
from miniray.output_discovery import PreparedOutput, OutputDiscoverySession
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
from miniray.task_outputs import TaskExecution


pytestmark = pytest.mark.unit


def _header():
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 1)
    attempt = AttemptID(task, 0)
    execution = TaskExecution(attempt)
    return OutputPublicationHeader(OutputPublicationID(LeaseID.random(), execution), job, WorkerID.random(), WorkerID.random(), OutputPublicationNodeIncarnation(NodeID.random(), 12345, 1))


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
        ContainedReferenceHold(ObjectID.for_task(TaskID.derive(header.job_id, task, 1)),
                               handle.owner_worker_id, "original-containing-object")
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
        raise ValueError(('later reducer cannot serialize'))


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


@pytest.mark.parametrize('container', (list, tuple))
def test_one_output_serializes_each_reducer_once_before_becoming_observable(container, monkeypatch):
    header = _header()
    reductions = []
    session = OutputDiscoverySession(header, inline_threshold=1024)
    observed = []

    class _Observe:

        def __reduce__(self):
            observed.append(session.discovered)
            return (int, (7,))
    serialized = []
    original_dumps = cloudpickle.dumps

    def dumps_once(value):
        serialized.append(value)
        return original_dumps(value)

    monkeypatch.setattr(cloudpickle, 'dumps', dumps_once)
    value = container((_CountedReduction('first', reductions), _Observe(), _CountedReduction('last', reductions)))
    result = session.discover(value)
    assert reductions == ['first', 'last']
    assert observed == [None]
    assert session.discovered is result
    assert type(result) is PreparedOutput and type(result.payload) is bytes
    assert len(serialized) == 1 and serialized[0] is value
    assert cloudpickle.loads(result.payload) == container(('first', 7, 'last'))
    with pytest.raises(RuntimeError, match='one-shot'):
        session.discover([1, 2, 3])
    assert reductions == ['first', 'last']
    assert len(serialized) == 1


def test_single_value_threshold_equality_and_larger_payload_choose_exact_tier():
    header = _header()
    small = b"x"
    threshold = len(cloudpickle.dumps(small))
    session = OutputDiscoverySession(header, inline_threshold=threshold)
    result = session.discover(small)
    assert (result.manifest.value).tier is protocol.ResultStorage.INLINE
    assert (result.manifest.value).size_bytes == threshold
    larger = OutputDiscoverySession(header, inline_threshold=threshold).discover((b'y' * 64))
    assert (larger.manifest.value).tier is protocol.ResultStorage.OBJECT_STORE
    assert (larger.manifest.value).size_bytes > threshold
    assert (result.manifest.value.edges) == ()


def test_same_python_ref_aliases_share_one_transfer_within_the_single_value():
    header = _header()
    child = _owned(header)
    result_session = OutputDiscoverySession(header, inline_threshold=2048)
    result = result_session.discover((([child, child], {'child': child})))
    first = (result.manifest.value)
    assert len(first.transfers) == 1
    left = first.transfers[0]
    assert left.contained_object_id == child.object_id
    assert left.final_hold.container_object_id == (header.publication_id.object_id)
    assert result_session.source_references == (child,)
    imports = []

    def restore(*identity):
        imports.append(identity)
        return object()

    with importing_references(restore):
        decoded_first, decoded_second = cloudpickle.loads((result.payload))
    assert decoded_first[0] is decoded_first[1]
    assert decoded_first[0] is decoded_second["child"]
    assert tuple(identity[3] for identity in imports) == (left.final_hold,)
    assert (result.manifest.value.edges) == first.edges


def test_distinct_python_handles_for_one_child_keep_existing_export_semantics():
    header = (_header())
    first = _owned(header)
    second = ObjectRef(first.object_id, first.owner_worker_id, first.owner_address)
    assert first == second and first is not second
    session = OutputDiscoverySession(header, inline_threshold=2048)
    result = session.discover(([first, second]))
    transfers = (result.manifest.value).transfers
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
        protocol.ContainedTransferSource(ContainedReferenceHold(
            ObjectID.for_task(upstream_task), WorkerID.random(), "exact-upstream"))
        if source_kind == "contained"
        else protocol.TaskHoldSource(protocol.TaskReferenceHold(
            protocol.TaskReferenceHoldKind.RETAINED, header.executor_worker_id,
            upstream_task, AttemptID(upstream_task, 0),
        ))
    )
    owned = _owned(header)
    borrowed = _borrowed(header, source=source)
    original_source = pickle.loads(pickle.dumps(source))
    route_calls = []
    monkeypatch.setattr(
        ObjectRef, "close", lambda self: pytest.fail("discovery must not close a user handle")
    )
    session = OutputDiscoverySession(
        header, inline_threshold=1,
        owner_address=lambda: route_calls.append("local-route") or ("127.0.0.1", 31003),
    )
    result = session.discover(((owned, {'borrowed': borrowed})))
    assert route_calls == ["local-route"]
    owned_transfer = (result.manifest.value).transfers[0]
    borrowed_transfer = (result.manifest.value).transfers[1]
    assert owned_transfer.source == OwnedContainedSource(header.executor_worker_id)
    assert owned_transfer.contained_owner_address == ("127.0.0.1", 31003)
    assert borrowed_transfer.source == BorrowedContainedSource(
        header.executor_worker_id, borrowed.borrower_token, original_source,
    )
    assert borrowed_transfer.contained_owner_address == borrowed.owner_address
    for transfer in (owned_transfer, borrowed_transfer):
        assert transfer.provisional_hold.container_owner_worker_id == header.executor_worker_id
        assert transfer.final_hold.container_owner_worker_id == header.owner_worker_id
    original_payload = result.payload
    original_manifest = result.manifest
    borrowed._borrower_token = 'changed-after-discovery'
    borrowed._owner_address = ('127.0.0.1', 31004)
    (object.__setattr__(source.hold, 'transfer_token' if source_kind == 'contained' else 'kind', 'changed-after-discovery'))
    assert (result.manifest.value.transfers[1].source) == (BorrowedContainedSource(
        header.executor_worker_id, 'exact-borrower-token', original_source,
    ))
    assert replace(result).manifest == original_manifest and result.payload == original_payload
    session.release_sources_after_promotions()
    assert session.source_references == ()


@pytest.mark.parametrize('stored', (False, True))
def test_discovery_metadata_and_payload_are_separate_and_wire_roundtrip_exact(stored):
    header = _header()
    child = _owned(header)
    session = OutputDiscoverySession(header, inline_threshold=0 if stored else 4096)
    result = session.discover({'child': child, 'padding': b'P' * 32})
    assert result.manifest.value.tier is (protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE)
    _assert_metadata(result.manifest)
    assert result == pickle.loads(pickle.dumps(result))
    assert result.manifest.value.size_bytes == len(result.payload)
    assert result.manifest.value.checksum == hashlib.sha256(result.payload).hexdigest()
    assert tuple(field.name for field in fields(PreparedOutput)) == ('manifest', 'payload')
    with pytest.raises(TypeError, match='local custody'):
        cloudpickle.dumps(session)
    with pytest.raises(FrozenInstanceError):
        result.payload = b'changed'


def test_single_output_tokens_bind_child_order_lease_and_canonical_index():
    header = _header()
    child = _owned(header)
    sessions = [OutputDiscoverySession(header, inline_threshold=1024) for _ in range(2)]
    first, replay = (session.discover(child) for session in sessions)
    assert first == replay
    assert (first.manifest.publication_id.object_id.return_index) == (0)
    assert (first.manifest.execution.object_id) == (header.publication_id.object_id)
    calls = []
    custom = OutputDiscoverySession(
        header, inline_threshold=1024,
        token_factory=(lambda transfer: calls.append(transfer) or 'fixed-token-{}'.format(transfer)),
    ).discover(([_owned(header, index=45), _owned(header, index=46)]))
    assert calls == [(0), (1)]
    left, right = (transfer.final_hold for transfer in (custom.manifest.value).transfers)
    assert (left.transfer_token, right.transfer_token) == ("fixed-token-0", "fixed-token-1")
    assert left != right
    changed = replace(header, publication_id=replace(
        header.publication_id, lease_id=LeaseID.random()
    ))
    other = OutputDiscoverySession(changed, inline_threshold=1024).discover(child)
    assert (first.manifest.value).transfers[0].final_hold != (other.manifest.value).transfers[0].final_hold


def test_discovery_retains_ephemeral_sources_until_explicit_all_promotions_release():
    header = _header()
    weak_handles = []
    session = OutputDiscoverySession(header, inline_threshold=1024)
    result = session.discover(([_EphemeralOwnedReference(header, weak_handles, index=99), _EphemeralOwnedReference(header, weak_handles, index=100)]))
    assert len(weak_handles) == 2
    assert all(reference() is not None for reference in weak_handles)
    assert len(session.source_references) == 2
    assert session.discovered is result
    session.release_sources_after_promotions()
    session.release_sources_after_promotions()
    assert session.discovered is result
    assert session.source_references == ()
    assert all(reference() is None for reference in weak_handles)


def test_late_reducer_failure_in_one_value_clears_sources_and_forbids_retry():
    header = _header()
    weak_handles = []
    session = OutputDiscoverySession(header, inline_threshold=1024)

    def attempt():
        try:
            session.discover(([_EphemeralOwnedReference(header, weak_handles), _Unserializable()]))
        except ValueError as error:
            assert str(error) == ('later reducer cannot serialize')
        else:
            pytest.fail(('bad later reducer unexpectedly serialized'))

    attempt()
    assert session.source_references == ()
    assert session.discovered is None
    assert weak_handles[0]() is None
    with pytest.raises(RuntimeError, match="one-shot"):
        session.discover((1, 2))
    session.abort()
    with pytest.raises(RuntimeError, match=('complete discovered output')):
        session.release_sources_after_promotions()


def test_abort_clears_local_custody_without_mutating_the_prepared_output():
    header = _header()
    weak_handles = []
    session = OutputDiscoverySession(header, inline_threshold=1024)
    result = session.discover(_EphemeralOwnedReference(header, weak_handles))
    assert weak_handles[0]() is not None
    session.abort()
    session.abort()
    assert session.discovered is None and session.source_references == ()
    assert weak_handles[0]() is None
    assert result.manifest.value.transfers


@pytest.mark.parametrize('invalid', ('closed', 'no-route', 'no-token', 'no-source', 'owned-borrower'))
def test_invalid_live_source_fails_before_an_output_or_remote_effect_exists(invalid):
    header = _header()
    child = _borrowed(header)
    if invalid == 'closed':
        child._closed = True
    elif invalid == 'no-route':
        child._owner_address = None
    elif invalid == 'no-token':
        child._borrower_token = None
    elif invalid == 'no-source':
        child._borrow_source = None
    else:
        child._owner_worker_id = header.executor_worker_id
    session = OutputDiscoverySession(header, inline_threshold=1024)
    with pytest.raises((ValueError, RuntimeError)):
        session.discover(child)
    assert session.discovered is None and session.source_references == ()


def test_invalid_owned_credentials_are_rejected_before_route_provider_runs():
    header = (_header())
    child = _owned(header)
    child._borrower_token = "not-an-owned-handle"
    session = OutputDiscoverySession(
        header, inline_threshold=1024,
        owner_address=lambda: pytest.fail("conflicting ownership reached route lookup"),
    )
    with pytest.raises(ValueError, match="conflicting borrower"):
        session.discover(child)


def test_discovery_restores_outer_export_scope_after_success_and_failure():
    header = (_header())
    exporter = lambda _ref: pytest.fail("outer exporter was used inside discovery")
    with exporting_references(exporter):
        OutputDiscoverySession(header, inline_threshold=1024).discover((7))
        assert current_exporter() is exporter
        with pytest.raises(ValueError, match=('later reducer')):
            OutputDiscoverySession(header, inline_threshold=1024).discover((_Unserializable()))
        assert current_exporter() is exporter
    assert current_exporter() is None


def test_conflicting_child_owner_in_one_value_aborts_before_effects():
    header = _header()
    first = _borrowed(header)
    second = ObjectRef(first.object_id, WorkerID.random(), first.owner_address)
    second._borrower_token = "different-owner-borrow"
    second._borrow_source = protocol.ContainedTransferSource(ContainedReferenceHold(
        ObjectID.for_task(TaskID.random()), second.owner_worker_id, "different-owner-source"))
    session = OutputDiscoverySession(header, inline_threshold=1024)
    with pytest.raises(OutputPublicationConflictError, match="conflicting owners"):
        session.discover(([first, second]))
    assert session.discovered is None
    assert session.source_references == ()


def test_reentrant_reducer_cannot_release_or_restart_discovery_custody():
    header = (_header())
    session = OutputDiscoverySession(header, inline_threshold=1024)
    rejected = []

    class _Reenter:
        def __reduce__(self):
            for operation in (session.abort, session.release_sources_after_promotions):
                with pytest.raises(RuntimeError):
                    operation()
                rejected.append(operation.__name__)
            with pytest.raises(RuntimeError, match="one-shot"):
                session.discover((0))
            return int, (7,)

    result = session.discover((_Reenter()))
    assert cloudpickle.loads((result.payload)) == 7
    assert rejected == ["abort", "release_sources_after_promotions"]
    assert session.discovered is result


@pytest.mark.parametrize('invalid', ('missing', 'tuple', 'list', 'bytearray', 'memoryview'))
def test_prepared_output_rejects_payload_containers_without_invoking_reducers(invalid):
    session = OutputDiscoverySession(_header(), inline_threshold=1024)
    result = session.discover(())
    reductions = []
    reducer = _CountedReduction('must-not-serialize', reductions)
    payload = {
        'missing': None, 'tuple': (reducer,), 'list': [reducer],
        'bytearray': bytearray(result.payload), 'memoryview': memoryview(result.payload),
    }[invalid]
    with pytest.raises(TypeError, match='serialized bytes'):
        replace(result, payload=payload)
    assert reductions == []
    assert cloudpickle.loads(result.payload) == ()
    assert session.discovered is result and session.source_references == ()


@pytest.mark.parametrize("threshold", (True, -1, 1.0, "1024", None))
def test_threshold_requires_nonnegative_integer(threshold):
    with pytest.raises(ValueError, match="inline_threshold"):
        OutputDiscoverySession(_header(), inline_threshold=threshold)


def test_bad_token_or_final_manifest_rejection_clears_all_source_handles():
    header = (_header())
    child = _owned(header)
    for token in (None, "", 1):
        session = OutputDiscoverySession(
            header, inline_threshold=1024, token_factory=lambda *_args: token
        )
        with pytest.raises(ValueError, match="non-empty string"):
            session.discover(child)
        assert session.source_references == () and session.discovered is None
    duplicate = ObjectRef(child.object_id, child.owner_worker_id, child.owner_address)
    session = OutputDiscoverySession(
        header, inline_threshold=1024, token_factory=lambda *_args: "duplicate"
    )
    with pytest.raises(OutputPublicationConflictError, match="child hold"):
        session.discover(([child, duplicate]))
    assert session.source_references == () and session.discovered is None


def test_prepared_output_rejects_truncated_corrupted_or_mutable_bytes():
    result = OutputDiscoverySession(_header(), inline_threshold=1024).discover((1, 2))
    for payload in (b'', result.payload[:-1], bytes((result.payload[0] ^ 1,)) + result.payload[1:]):
        with pytest.raises(ValueError, match='does not match'):
            replace(result, payload=payload)
    with pytest.raises(TypeError, match='serialized bytes'):
        replace(result, payload=bytearray(result.payload))
    with pytest.raises(TypeError, match='OutputPublicationManifest'):
        PreparedOutput(result.manifest.value, result.payload)
    assert cloudpickle.loads(result.payload) == (1, 2)
    assert pickle.loads(pickle.dumps(result)) == result
    forged = replace(result)
    object.__setattr__(forged, 'payload', b'corrupted-after-construction')
    with pytest.raises(ValueError, match='does not match'):
        pickle.loads(pickle.dumps(forged))


def test_discovery_uses_explicit_owner_route_and_embeds_exact_final_hold():
    header = (_header())
    child = _owned(header, route=None)
    address = ("127.0.0.1", 31009)
    outer = (header.publication_id.object_id)
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
        token_factory=(lambda _index: 'same-token'),
    )
    result = session.discover(child)
    assert (result.payload) == payload
    assert (result.manifest.value).transfers == (expected,)
    assert session.source_references == (child,)
    assert child.owner_address is None and not child.closed
    assert current_exporter() is None
    imports = []
    restored = object()

    def restore(*identity):
        imports.append(identity)
        return restored

    with importing_references(restore):
        assert cloudpickle.loads((result.payload)) is restored
    assert imports == [wire_identity]
    session.release_sources_after_promotions()
    assert session.source_references == () and session.discovered is result
    assert not child.closed
