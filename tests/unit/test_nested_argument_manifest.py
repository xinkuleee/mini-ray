"""Pure contracts for nested ObjectRefs in Task argument payloads."""

from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from io import BytesIO
import pickle

import pytest

pytestmark = pytest.mark.unit

from miniray.dependency import (
    ArgumentEncodingError, ContainedRef, NestedReferenceImportSession,
    UnresolvedDependencyError, contained_references, decode_inline_argument,
    decode_task_argument, decode_task_arguments,
    encode_task_argument, encode_task_arguments, nested_references,
    top_level_dependencies,
)
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.protocol import (
    FunctionKey, InlineArg, NestedReferenceTransfer, RefArg, TaskReferenceHold,
    StoredArg, TaskReferenceHoldKind, TaskSpec,
)
from miniray.resources import ResourceVector


class _Handle:
    def __init__(self, name: str, closed: list[str]) -> None:
        self.name = name
        self._closed = closed

    def close(self) -> None:
        self._closed.append(self.name)


class _Marker:
    def __init__(self, index: object) -> None:
        self.index = index


class _PersistentPickler(pickle.Pickler):
    def persistent_id(self, value: object) -> object | None:
        if isinstance(value, _Marker):
            return ("miniray-nested-ref-index-v2", value.index)
        return None


class _RecordingUnpickler(pickle.Unpickler):
    def __init__(self, stream: BytesIO) -> None:
        super().__init__(stream)
        self.ids: list[object] = []

    def persistent_load(self, value: object) -> object:
        self.ids.append(value)
        return value


def _payload(value: object) -> bytes:
    stream = BytesIO()
    _PersistentPickler(stream, protocol=pickle.HIGHEST_PROTOCOL).dump(value)
    return stream.getvalue()


def _identity(seed: int) -> tuple[ContainedRef, WorkerID, TaskID]:
    producer = TaskID(bytes([seed]) * 16)
    owner = WorkerID(bytes([seed + 1]) * 16)
    submitter = WorkerID(bytes([seed + 2]) * 16)
    task_id = TaskID(bytes([seed + 3]) * 16)
    return ContainedRef(ObjectID(producer, 0), owner), submitter, task_id


def _transfer(
    reference: ContainedRef,
    submitter: WorkerID,
    task_id: TaskID,
    *,
    kind: TaskReferenceHoldKind = TaskReferenceHoldKind.RETAINED,
) -> NestedReferenceTransfer:
    return NestedReferenceTransfer(
        reference.object_id,
        reference.owner_worker_id,
        ("127.0.0.1", 21000 + reference.object_id.return_index),
        TaskReferenceHold(
            kind, submitter, task_id, AttemptID(task_id, 0)
        ),
    )


def test_typed_manifest_is_frozen_and_validates_owner_route_and_hold() -> None:
    reference, submitter, task_id = _identity(1)
    transfer = _transfer(reference, submitter, task_id)

    with pytest.raises(FrozenInstanceError):
        transfer.owner_address = ("127.0.0.1", 1)  # type: ignore[misc]
    with pytest.raises(ProtocolError, match="owner_address"):
        replace(transfer, owner_address=("", 21000))
    with pytest.raises(ProtocolError, match="origin_attempt_id"):
        replace(transfer.hold, origin_attempt_id=object())
    other_task = TaskID.random()
    with pytest.raises(ProtocolError, match="origin attempt must belong"):
        replace(transfer.hold, origin_attempt_id=AttemptID(other_task, 0))
    with pytest.raises(ProtocolError, match="submitted.*owner"):
        _transfer(
            reference, submitter, task_id,
            kind=TaskReferenceHoldKind.SUBMITTED,
        )
    with pytest.raises(ProtocolError, match="manifest must be unique"):
        InlineArg(b"payload", nested_refs=(transfer, transfer))


def test_top_level_ref_does_not_call_nested_exporter() -> None:
    reference, _, _ = _identity(5)

    encoded = encode_task_argument(
        reference,
        export_nested_ref=lambda _value: pytest.fail(
            "top-level ObjectRef must not be exported as nested"
        ),
    )

    assert encoded == RefArg(reference.object_id, reference.owner_worker_id)


@pytest.mark.parametrize("serializer", ["pickle", "cloudpickle"])
def test_payload_contains_only_indexes_and_duplicate_ref_imports_once(
    serializer: str,
) -> None:
    reference, submitter, task_id = _identity(9)
    transfer = _transfer(reference, submitter, task_id)
    exports: list[object] = []
    imports: list[NestedReferenceTransfer] = []
    handle = object()

    encoded = encode_task_argument(
        {"left": reference, "right": [reference]},
        serializer=serializer,
        export_nested_ref=lambda value: (exports.append(value), transfer)[1],
    )
    assert isinstance(encoded, InlineArg)
    assert encoded.nested_refs == (transfer,)
    assert exports == [reference]
    recorder = _RecordingUnpickler(BytesIO(encoded.data))
    recorder.load()
    assert recorder.ids == [
        ("miniray-nested-ref-index-v2", 0),
        ("miniray-nested-ref-index-v2", 0),
    ]

    decoded = decode_inline_argument(
        encoded,
        import_nested_ref=lambda item: (imports.append(item), handle)[1],
    )
    assert imports == [transfer]
    assert decoded["left"] is handle
    assert decoded["right"][0] is handle


def test_manifest_projection_is_non_gating_and_rejects_cross_argument_conflict() -> None:
    first, submitter, task_id = _identity(13)
    second, _, _ = _identity(17)
    transfers = {
        first.object_id: _transfer(first, submitter, task_id),
        second.object_id: _transfer(second, submitter, task_id),
    }
    encoded = encode_task_arguments(
        ({"ref": first}, [second]),
        export_nested_ref=lambda value: transfers[value.object_id],
    )

    assert top_level_dependencies(encoded) == ()
    assert nested_references(encoded) == (
        transfers[first.object_id], transfers[second.object_id]
    )
    assert contained_references(encoded) == (first.object_id, second.object_id)

    conflicting = replace(
        transfers[first.object_id],
        owner_address=("127.0.0.1", 21999),
    )
    with pytest.raises(ArgumentEncodingError, match="conflicting nested"):
        nested_references(
            (encoded[0], InlineArg(_payload(_Marker(0)), nested_refs=(conflicting,)))
        )


def test_stored_arg_is_a_readiness_edge_but_keeps_nested_refs_non_gating() -> None:
    nested, submitter, task_id = _identity(18)
    transfer = _transfer(nested, submitter, task_id)
    encoded = encode_task_argument(
        {"nested": nested},
        export_nested_ref=lambda _value: transfer,
    )
    assert isinstance(encoded, InlineArg)
    storage_task = TaskID(bytes([22]) * 16)
    storage_id = ObjectID.for_task(storage_task)
    stored = StoredArg(
        storage_id, submitter, encoded.serializer, encoded.nested_refs
    )

    assert top_level_dependencies((stored,)) == (storage_id,)
    assert nested_references((stored,)) == (transfer,)
    assert contained_references((stored,)) == (nested.object_id,)

    imported = object()
    decoded = decode_task_arguments(
        (stored,),
        materialize_ref=lambda *_identity: pytest.fail(
            "StoredArg bytes must not use RefArg value materialization"
        ),
        materialize_stored=lambda object_id, owner: (
            encoded.data
            if (object_id, owner) == (storage_id, submitter)
            else pytest.fail("wrong StoredArg identity")
        ),
        import_nested_ref=lambda item: (
            imported if item == transfer
            else pytest.fail("wrong nested transfer")
        ),
    )
    assert decoded == ({"nested": imported},)


def test_stored_arg_malformed_manifest_rolls_back_acquired_handles() -> None:
    first, submitter, task_id = _identity(19)
    second, _, _ = _identity(23)
    one = _transfer(first, submitter, task_id)
    two = _transfer(second, submitter, task_id)
    storage_id = ObjectID.for_task(TaskID(bytes([27]) * 16))
    stored = StoredArg(
        storage_id, submitter, "pickle", (one, two)
    )
    closed: list[str] = []
    session = NestedReferenceImportSession(
        lambda item: _Handle(
            "one" if item == one else "two", closed
        )
    )

    with pytest.raises(ArgumentEncodingError, match="invalid serialized"):
        decode_task_arguments(
            (stored,),
            materialize_ref=lambda *_identity: pytest.fail(
                "unexpected RefArg materialization"
            ),
            materialize_stored=lambda *_identity: _payload(_Marker(0)),
            import_nested_ref=session,
        )

    assert closed == ["one"]
    assert session.acquired == ()


def test_stored_arg_never_uses_the_already_decoded_ref_value_loader() -> None:
    stored = StoredArg(ObjectID.for_task(TaskID.random()), WorkerID.random())
    user_value = pickle.dumps(7)
    value_loads: list[object] = []

    def materialize_value(*identity: object) -> object:
        value_loads.append(identity)
        return user_value

    with pytest.raises(UnresolvedDependencyError, match="stored by-value"):
        decode_task_argument(stored, materialize_ref=materialize_value)
    assert value_loads == []

    decoded = decode_task_argument(
        stored, materialize_ref=materialize_value,
        materialize_stored=lambda *_identity: pickle.dumps(user_value),
    )
    assert decoded == user_value
    assert value_loads == []


@pytest.mark.parametrize(
    "persistent_id",
    [
        ("miniray-contained-ref-v1", 0),
        ("miniray-nested-ref-index-v2", True),
        ("miniray-nested-ref-index-v2", -1),
        ("miniray-nested-ref-index-v2", 1),
    ],
)
def test_decoder_rejects_old_malformed_or_out_of_range_index(
    persistent_id: tuple[object, object],
) -> None:
    reference, submitter, task_id = _identity(21)
    transfer = _transfer(reference, submitter, task_id)
    stream = BytesIO()

    class Pickler(pickle.Pickler):
        def persistent_id(self, value: object) -> object | None:
            return persistent_id if isinstance(value, _Marker) else None

    Pickler(stream, protocol=pickle.HIGHEST_PROTOCOL).dump(_Marker(0))
    argument = InlineArg(stream.getvalue(), nested_refs=(transfer,))
    with pytest.raises(ArgumentEncodingError, match="invalid serialized"):
        decode_inline_argument(argument, import_nested_ref=lambda _item: object())


def test_decoder_requires_importer_and_complete_first_use_order() -> None:
    first, submitter, task_id = _identity(25)
    second, _, _ = _identity(29)
    one = _transfer(first, submitter, task_id)
    two = _transfer(second, submitter, task_id)

    with pytest.raises(ArgumentEncodingError, match="requires import_nested_ref"):
        decode_inline_argument(InlineArg(_payload(_Marker(0)), nested_refs=(one,)))
    with pytest.raises(ArgumentEncodingError, match="invalid serialized"):
        decode_inline_argument(
            InlineArg(_payload(_Marker(0)), nested_refs=(one, two)),
            import_nested_ref=lambda _item: object(),
        )
    with pytest.raises(ArgumentEncodingError, match="invalid serialized"):
        decode_inline_argument(
            InlineArg(_payload([_Marker(1), _Marker(0)]), nested_refs=(one, two)),
            import_nested_ref=lambda _item: object(),
        )


def test_shared_import_session_rolls_back_prior_argument_in_reverse_order() -> None:
    first, submitter, task_id = _identity(33)
    second, _, _ = _identity(37)
    one = _transfer(first, submitter, task_id)
    two = _transfer(second, submitter, task_id)
    closed: list[str] = []

    def acquire(transfer: NestedReferenceTransfer) -> _Handle:
        if transfer == two:
            raise RuntimeError("second acquire failed")
        return _Handle("one", closed)

    session = NestedReferenceImportSession(acquire)
    with pytest.raises(RuntimeError, match="second acquire failed"):
        decode_task_arguments(
            (
                InlineArg(_payload(_Marker(0)), nested_refs=(one,)),
                InlineArg(_payload(_Marker(0)), nested_refs=(two,)),
            ),
            materialize_ref=lambda *_identity: pytest.fail(
                "nested refs are not materialized dependencies"
            ),
            import_nested_ref=session,
        )
    assert closed == ["one"]
    assert session.acquired == ()


def test_committed_shared_session_releases_each_unique_handle_once() -> None:
    reference, submitter, task_id = _identity(41)
    transfer = _transfer(reference, submitter, task_id)
    argument = encode_task_argument(
        [reference, reference],
        export_nested_ref=lambda _value: transfer,
    )
    closed: list[str] = []
    acquired: list[NestedReferenceTransfer] = []
    session = NestedReferenceImportSession(
        lambda item: (acquired.append(item), _Handle("handle", closed))[1]
    )

    decoded = decode_task_arguments(
        (argument,),
        materialize_ref=lambda *_identity: pytest.fail("unexpected RefArg"),
        import_nested_ref=session,
    )
    session.commit()
    assert decoded[0][0] is decoded[0][1]
    assert acquired == [transfer]
    session.close()
    session.close()
    assert closed == ["handle"]


def test_task_spec_rejects_nested_hold_from_another_task_or_submitter() -> None:
    reference, submitter, task_id = _identity(45)
    transfer = _transfer(reference, submitter, task_id)
    argument = InlineArg(_payload(_Marker(0)), nested_refs=(transfer,))

    def spec(value: InlineArg) -> TaskSpec:
        return TaskSpec(
            JobID(bytes([50]) * 16),
            task_id,
            AttemptID(task_id, 0),
            FunctionKey(JobID(bytes([50]) * 16), "m", "f", "v"),
            (value,),
            1,
            ResourceVector(),
            submitter,
        )

    spec(argument)
    other_task = TaskID(bytes([51]) * 16)
    with pytest.raises(ProtocolError, match="TaskSpec.task_id"):
        spec(
            replace(
                argument,
                nested_refs=(
                    replace(
                        transfer,
                        hold=replace(
                            transfer.hold,
                            task_id=other_task,
                            origin_attempt_id=AttemptID(other_task, 0),
                        ),
                    ),
                ),
            )
        )
    with pytest.raises(ProtocolError, match="submitter"):
        spec(
            replace(
                argument,
                nested_refs=(replace(transfer, hold=replace(transfer.hold, submitting_worker_id=WorkerID(bytes([52]) * 16))),),
            )
        )


@pytest.mark.parametrize("other_role", ["inline_nested", "top_level"])
def test_task_spec_rejects_cross_storage_nested_owner_conflicts(
    other_role: str,
) -> None:
    reference, submitter, task_id = _identity(53)
    transfer = _transfer(reference, submitter, task_id)
    stored = StoredArg(
        ObjectID.for_task(TaskID.random()), submitter,
        "pickle", (transfer,),
    )
    other_owner = WorkerID(bytes([58]) * 16)
    conflict = (
        InlineArg(b"unused", nested_refs=(replace(
            transfer, owner_worker_id=other_owner,
        ),))
        if other_role == "inline_nested"
        else RefArg(reference.object_id, other_owner)
    )
    job_id = JobID(bytes([59]) * 16)
    with pytest.raises(ProtocolError, match="conflicting owners"):
        TaskSpec(
            job_id, task_id, AttemptID(task_id, 0),
            FunctionKey(job_id, "m", "f", "v"), (stored,), 1,
            ResourceVector(), submitter, kwargs=(("other", conflict),),
        )
