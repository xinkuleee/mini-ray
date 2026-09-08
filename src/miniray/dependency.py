"""Task-argument encoding and dependency resolution.

The important distinction in Ray's argument semantics is structural:

* a top-level object reference is an execution dependency and is encoded as a
  :class:`~miniray.protocol.RefArg`;
* a reference contained inside another Python value is data.  The surrounding
  value is encoded as an :class:`~miniray.protocol.InlineArg`, while the nested
  reference is restored as a :class:`ContainedRef` handle at the worker.

No function in this module waits for an object.  Submission can therefore build
an immutable task specification before its dependencies become ready.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import pickle
from typing import Callable, Hashable, Iterable, Sequence, Union

import cloudpickle

from .protocol import (
    ContainedTransferSource, InlineArg, NestedReferenceTransfer, RefArg, StoredArg, TaskArg,
)
from .ref_transfer import ExportedReference, ImportCallback


ObjectIDLike = Hashable
WorkerIDLike = Hashable
_PERSISTENT_REF_TAG = "miniray-nested-ref-index-v2"

ExportNestedReference = Callable[[object], NestedReferenceTransfer]
ImportNestedReference = Callable[[NestedReferenceTransfer], object]
ReleaseImportedReference = Callable[[object], object]


class ArgumentEncodingError(ValueError):
    """A task argument cannot be encoded without changing its semantics."""


class UnresolvedDependencyError(LookupError):
    """A caller tried to decode a top-level reference before resolving it."""


@dataclass(frozen=True)
class ContainedRef:
    """A pickle-safe ObjectRef handle embedded inside an inline value.

    The same value may also be supplied as a top-level task argument.  In that
    position :func:`encode_task_argument` emits ``RefArg`` and the object becomes
    an execution dependency.
    """

    object_id: ObjectIDLike
    owner_worker_id: WorkerIDLike

    def __post_init__(self) -> None:
        _require_hashable(self.object_id, "object_id")
        _require_hashable(self.owner_worker_id, "owner_worker_id")


@dataclass(frozen=True)
class DependencyResolution:
    """Pure result of checking and, if possible, decoding task arguments."""

    ready: bool
    missing: tuple[ObjectIDLike, ...]
    values: tuple[object, ...] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "missing", tuple(self.missing))
        if self.ready == bool(self.missing):
            raise ValueError("ready must be true exactly when missing is empty")
        if not self.ready and self.values is not None:
            raise ValueError("unready arguments cannot expose partial values")
        if self.ready and self.values is None:
            raise ValueError("ready arguments must expose a values tuple")


class _NestedManifestBuilder:
    """Assign first-use indexes without putting owner credentials in pickle."""

    def __init__(
        self, export_nested_ref: ExportNestedReference | None
    ) -> None:
        self._export_nested_ref = export_nested_ref
        self._indexes: dict[tuple[ObjectIDLike, WorkerIDLike], int] = {}
        self._owners: dict[ObjectIDLike, WorkerIDLike] = {}
        self.manifest: list[NestedReferenceTransfer] = []

    def persistent_id(self, value: object) -> object | None:
        reference = _as_reference(value)
        if reference is None:
            return None
        previous_owner = self._owners.setdefault(
            reference.object_id, reference.owner_worker_id
        )
        if previous_owner != reference.owner_worker_id:
            raise ArgumentEncodingError(
                f"object {reference.object_id!r} names conflicting owners"
            )
        key = reference.object_id, reference.owner_worker_id
        index = self._indexes.get(key)
        if index is None:
            if self._export_nested_ref is None:
                raise ArgumentEncodingError(
                    "serializing a nested ObjectRef requires "
                    "export_nested_ref"
                )
            transfer = self._export_nested_ref(value)
            if not isinstance(transfer, NestedReferenceTransfer):
                raise ArgumentEncodingError(
                    "export_nested_ref must return NestedReferenceTransfer"
                )
            if (
                transfer.object_id != reference.object_id
                or transfer.owner_worker_id != reference.owner_worker_id
            ):
                raise ArgumentEncodingError(
                    "nested-reference exporter changed object identity"
                )
            index = len(self.manifest)
            self._indexes[key] = index
            self.manifest.append(transfer)
        return _PERSISTENT_REF_TAG, index


class _ArgumentPickler(pickle.Pickler):
    def __init__(self, file: BytesIO, manifest: _NestedManifestBuilder) -> None:
        super().__init__(file, protocol=pickle.HIGHEST_PROTOCOL)
        self._manifest = manifest

    def persistent_id(self, value: object) -> object | None:
        return self._manifest.persistent_id(value)


class _CloudpickleArgumentPickler(cloudpickle.CloudPickler):
    """Cloudpickle variant that keeps ObjectRefs as small handles."""

    def __init__(self, file: BytesIO, manifest: _NestedManifestBuilder) -> None:
        super().__init__(file, protocol=pickle.HIGHEST_PROTOCOL)
        self._manifest = manifest

    def persistent_id(self, value: object) -> object | None:
        return self._manifest.persistent_id(value)


class NestedReferenceImportSession:
    """Transactional, de-duplicating importer for one complete Task call.

    A Worker should share one session across every positional and keyword
    argument, commit only after all arguments decode, and call ``close`` after
    the physical attempt.  ``rollback`` closes already-acquired handles in
    reverse order while preserving the decode exception that caused it.
    Result payloads have contained-hold reducers rather than Task manifests;
    their handles join this same attempt custody through ``resolve_exported``.
    """

    def __init__(
        self,
        import_nested_ref: ImportNestedReference,
        *,
        release_nested_ref: ReleaseImportedReference | None = None,
        import_exported_ref: ImportCallback | None = None,
    ) -> None:
        if not callable(import_nested_ref):
            raise TypeError("import_nested_ref must be callable")
        if release_nested_ref is not None and not callable(release_nested_ref):
            raise TypeError("release_nested_ref must be callable or None")
        if import_exported_ref is not None and not callable(import_exported_ref):
            raise TypeError("import_exported_ref must be callable or None")
        self._import_nested_ref = import_nested_ref
        self._import_exported_ref = import_exported_ref
        self._release_nested_ref = (
            release_nested_ref or _close_imported_reference
        )
        self._values: dict[NestedReferenceTransfer, object] = {}
        self._exported_values: dict[ExportedReference, object] = {}
        self._acquired: list[object] = []
        self._committed = False
        self._rolled_back = False
        self._closed = False

    def resolve(self, transfer: NestedReferenceTransfer) -> object:
        if not isinstance(transfer, NestedReferenceTransfer):
            raise TypeError("transfer must be a NestedReferenceTransfer")
        if self._committed or self._rolled_back or self._closed:
            raise RuntimeError("nested-reference import session is not open")
        try:
            return self._values[transfer]
        except KeyError:
            pass
        value = self._import_nested_ref(transfer)
        self._values[transfer] = value
        self._acquired.append(value)
        return value

    def resolve_exported(self, object_id, owner_worker_id, owner_address, hold) -> object:
        """Import a child of a materialized dependency via its original hold.

        This is not a nested Task manifest edge. Its containing result stays
        alive through the consumer's top-level dependency hold; the original
        contained source authorizes a fresh borrower before user code sees it.
        The callback owns ambiguous Acquire/Release RPC obligations. This
        session owns the local handles and their reverse-order release.
        """
        if self._committed or self._rolled_back or self._closed:
            raise RuntimeError("nested-reference import session is not open")
        if self._import_exported_ref is None:
            raise ArgumentEncodingError("contained result requires import_exported_ref")
        # Normalize the wire spelling without turning a contained capability
        # into a TaskHoldSource or collapsing distinct container identities.
        source = ContainedTransferSource(hold)
        key = (object_id, owner_worker_id, owner_address, source.hold)
        if key in self._exported_values:
            return self._exported_values[key]
        value = self._import_exported_ref(*key)
        self._exported_values[key] = value
        self._acquired.append(value)
        return value

    @property
    def acquired(self) -> tuple[object, ...]:
        return tuple(self._acquired)

    def commit(self) -> tuple[object, ...]:
        if self._rolled_back or self._closed:
            raise RuntimeError("cannot commit a closed import session")
        self._committed = True
        return self.acquired

    def rollback(self) -> None:
        if self._committed or self._rolled_back or self._closed:
            return
        self._rolled_back = True
        self._release_all()

    def close(self) -> None:
        """Release every imported attempt handle exactly once."""

        if self._closed:
            return
        self._closed = True
        self._release_all()

    def _release_all(self) -> None:
        while self._acquired:
            value = self._acquired.pop()
            try:
                self._release_nested_ref(value)
            except Exception:
                # The original decode/attempt outcome is more useful.  A real
                # ObjectRef release remains conservatively live on ambiguity.
                pass
        self._values.clear()
        self._exported_values.clear()


class _ArgumentUnpickler(pickle.Unpickler):
    def __init__(
        self,
        file: BytesIO,
        manifest: tuple[NestedReferenceTransfer, ...],
        importer: NestedReferenceImportSession | None,
    ) -> None:
        super().__init__(file)
        self._manifest = manifest
        self._importer = importer
        self._first_use: list[int] = []
        self._seen: set[int] = set()

    def persistent_load(self, persistent_id: object) -> object:
        if (
            not isinstance(persistent_id, tuple)
            or len(persistent_id) != 2
            or persistent_id[0] != _PERSISTENT_REF_TAG
        ):
            raise pickle.UnpicklingError(
                f"unsupported mini-Ray persistent ID: {persistent_id!r}"
            )
        index = persistent_id[1]
        if isinstance(index, bool) or not isinstance(index, int):
            raise pickle.UnpicklingError(
                "nested-reference manifest index must be an integer"
            )
        if index < 0 or index >= len(self._manifest):
            raise pickle.UnpicklingError(
                "nested-reference manifest index is out of range"
            )
        if index not in self._seen:
            expected = len(self._first_use)
            if index != expected:
                raise pickle.UnpicklingError(
                    "nested-reference manifest indexes are not in first-use order"
                )
            self._seen.add(index)
            self._first_use.append(index)
        if self._importer is None:
            raise pickle.UnpicklingError(
                "nested ObjectRef payload requires an importer"
            )
        return self._importer.resolve(self._manifest[index])

    def validate_complete(self) -> None:
        if self._first_use != list(range(len(self._manifest))):
            raise pickle.UnpicklingError(
                "inline argument did not reference its complete nested manifest"
            )


def encode_task_argument(
    value: object,
    *,
    serializer: str = "pickle",
    export_nested_ref: ExportNestedReference | None = None,
) -> TaskArg:
    """Encode one user argument without resolving any ObjectRef."""

    reference = _as_reference(value)
    if reference is not None:
        return RefArg(reference.object_id, reference.owner_worker_id)
    if serializer not in {"pickle", "cloudpickle"}:
        raise ArgumentEncodingError(f"unsupported argument serializer: {serializer!r}")

    stream = BytesIO()
    manifest = _NestedManifestBuilder(export_nested_ref)
    try:
        pickler = (
            _ArgumentPickler(stream, manifest)
            if serializer == "pickle"
            else _CloudpickleArgumentPickler(stream, manifest)
        )
        pickler.dump(value)
    except (pickle.PickleError, TypeError, AttributeError) as exc:
        raise ArgumentEncodingError(
            f"task argument of type {type(value).__name__} is not serializable"
        ) from exc
    return InlineArg(
        stream.getvalue(),
        serializer=serializer,
        nested_refs=tuple(manifest.manifest),
    )


def encode_task_arguments(
    values: Iterable[object],
    *,
    serializer: str = "pickle",
    export_nested_ref: ExportNestedReference | None = None,
) -> tuple[TaskArg, ...]:
    """Encode positional arguments, preserving their top-level boundary."""

    return tuple(
        encode_task_argument(
            value, serializer=serializer, export_nested_ref=export_nested_ref
        )
        for value in values
    )


def decode_inline_argument(
    argument: InlineArg,
    *,
    import_nested_ref: Union[
        ImportNestedReference, NestedReferenceImportSession, None
    ] = None,
) -> object:
    """Decode an inline value and import every nested public ObjectRef.

    Passing a callback creates and commits a one-argument import transaction.
    A Worker may instead pass a shared :class:`NestedReferenceImportSession`
    across all arguments and commit it after the complete call decodes.
    """

    if not isinstance(argument, InlineArg):
        raise TypeError("argument must be an InlineArg")
    if argument.serializer not in {"pickle", "cloudpickle"}:
        raise ArgumentEncodingError(
            f"unsupported argument serializer: {argument.serializer!r}"
        )
    session: NestedReferenceImportSession | None
    owns_session = False
    if isinstance(import_nested_ref, NestedReferenceImportSession):
        session = import_nested_ref
    elif import_nested_ref is None:
        session = None
    else:
        session = NestedReferenceImportSession(import_nested_ref)
        owns_session = True
    if argument.nested_refs and session is None:
        raise ArgumentEncodingError(
            "nested ObjectRef payload requires import_nested_ref"
        )

    stream = BytesIO(argument.data)
    try:
        unpickler = _ArgumentUnpickler(
            stream, tuple(argument.nested_refs), session
        )
        value = unpickler.load()
        if stream.read(1):
            raise pickle.UnpicklingError(
                "inline argument contains trailing serialized data"
            )
        unpickler.validate_complete()
    except (pickle.PickleError, EOFError, AttributeError, TypeError, ValueError) as exc:
        if session is not None:
            session.rollback()
        raise ArgumentEncodingError("invalid serialized task argument") from exc
    except BaseException:
        if session is not None:
            session.rollback()
        raise
    if owns_session:
        assert session is not None
        session.commit()
    return value


def decode_task_argument(
    argument: TaskArg,
    *,
    materialize_ref: Callable[[ObjectIDLike, WorkerIDLike], object] | None = None,
    materialize_stored: (
        Callable[[ObjectIDLike, WorkerIDLike], bytes] | None
    ) = None,
    import_nested_ref: Union[
        ImportNestedReference, NestedReferenceImportSession, None
    ] = None,
) -> object:
    """Decode one worker argument.

    ``materialize_ref`` returns the decoded value of a top-level ``RefArg``.
    ``materialize_stored`` instead returns an undecoded argument byte stream.
    These contracts must not be mixed: a user's value may itself be bytes.
    Neither loader is called for a nested ``ContainedRef``.
    """

    if isinstance(argument, InlineArg):
        return decode_inline_argument(
            argument, import_nested_ref=import_nested_ref
        )
    if isinstance(argument, StoredArg):
        if materialize_stored is None:
            raise UnresolvedDependencyError(
                "stored by-value dependency is unresolved: {!r}".format(
                    argument.object_id
                )
            )
        payload = materialize_stored(
            argument.object_id, argument.owner_worker_id
        )
        if not isinstance(payload, bytes):
            raise ArgumentEncodingError(
                "StoredArg materializer must return serialized bytes"
            )
        return decode_inline_argument(
            InlineArg(
                payload, serializer=argument.serializer,
                nested_refs=argument.nested_refs,
            ),
            import_nested_ref=import_nested_ref,
        )
    if isinstance(argument, RefArg):
        if materialize_ref is None:
            raise UnresolvedDependencyError(
                f"top-level object dependency is unresolved: {argument.object_id!r}"
            )
        return materialize_ref(argument.object_id, argument.owner_worker_id)
    raise TypeError(
        "argument must be an InlineArg, RefArg, or StoredArg"
    )


def decode_task_arguments(
    arguments: Sequence[TaskArg],
    *,
    materialize_ref: Callable[[ObjectIDLike, WorkerIDLike], object],
    materialize_stored: (
        Callable[[ObjectIDLike, WorkerIDLike], bytes] | None
    ) = None,
    import_nested_ref: Union[
        ImportNestedReference, NestedReferenceImportSession, None
    ] = None,
) -> tuple[object, ...]:
    """Decode a dependency-ready argument sequence."""

    owns_session = (
        import_nested_ref is not None
        and not isinstance(import_nested_ref, NestedReferenceImportSession)
    )
    session = (
        NestedReferenceImportSession(import_nested_ref)
        if owns_session
        else import_nested_ref
    )
    try:
        values = tuple(
            decode_task_argument(
                argument,
                materialize_ref=materialize_ref,
                materialize_stored=materialize_stored,
                import_nested_ref=session,
            )
            for argument in arguments
        )
    except BaseException:
        if isinstance(session, NestedReferenceImportSession):
            session.rollback()
        raise
    if owns_session:
        assert isinstance(session, NestedReferenceImportSession)
        session.commit()
    return values


def top_level_dependencies(arguments: Iterable[TaskArg]) -> tuple[ObjectIDLike, ...]:
    """Return only execution dependencies, in first-appearance order."""

    return tuple(ref.object_id for ref in top_level_references(arguments))


def top_level_references(arguments: Iterable[TaskArg]) -> tuple[ContainedRef, ...]:
    """Return execution-dependency handles, preserving their owner identity."""

    refs = tuple(
        ContainedRef(argument.object_id, argument.owner_worker_id)
        for argument in arguments
        if isinstance(argument, (RefArg, StoredArg))
    )
    _validate_reference_owners(refs)
    result: list[ContainedRef] = []
    seen: set[ObjectIDLike] = set()
    for ref in refs:
        if ref.object_id not in seen:
            seen.add(ref.object_id)
            result.append(ref)
    return tuple(result)


def contained_references(arguments: Iterable[TaskArg]) -> tuple[ObjectIDLike, ...]:
    """Return nested handles, which are lifetime edges but not dependencies."""

    return tuple(reference.object_id for reference in nested_references(arguments))


def nested_references(
    arguments: Iterable[TaskArg],
) -> tuple[NestedReferenceTransfer, ...]:
    """Return unique nested transfer descriptors in first-appearance order."""

    result: list[NestedReferenceTransfer] = []
    by_object: dict[ObjectIDLike, NestedReferenceTransfer] = {}
    for argument in arguments:
        if not isinstance(argument, (InlineArg, StoredArg)):
            continue
        for reference in argument.nested_refs:
            previous = by_object.get(reference.object_id)
            if previous is None:
                by_object[reference.object_id] = reference
                result.append(reference)
            elif previous != reference:
                raise ArgumentEncodingError(
                    f"object {reference.object_id!r} names conflicting "
                    "nested transfers"
                )
    return tuple(result)


def resolve_task_arguments(
    arguments: Sequence[TaskArg],
    *,
    is_ready: Callable[[ObjectIDLike], bool],
    materialize_ref: Callable[[ObjectIDLike, WorkerIDLike], object],
    materialize_stored: (
        Callable[[ObjectIDLike, WorkerIDLike], bytes] | None
    ) = None,
    import_nested_ref: Union[
        ImportNestedReference, NestedReferenceImportSession, None
    ] = None,
) -> DependencyResolution:
    """Check the gate and expose no values until all refs are ready.

    ``materialize_ref`` should be an idempotent local lookup. If it raises, no
    resolution (and therefore no partial argument tuple) escapes.
    """

    dependencies = top_level_dependencies(arguments)
    missing = tuple(object_id for object_id in dependencies if not is_ready(object_id))
    if missing:
        return DependencyResolution(False, missing)
    return DependencyResolution(
        True,
        (),
        decode_task_arguments(
            arguments,
            materialize_ref=materialize_ref,
            materialize_stored=materialize_stored,
            import_nested_ref=import_nested_ref,
        ),
    )


# Short aliases make the module pleasant to use from a future CoreWorker.
encode_argument = encode_task_argument
encode_arguments = encode_task_arguments
decode_argument = decode_task_argument
decode_arguments = decode_task_arguments
dependency_ids = top_level_dependencies


def _as_reference(value: object) -> ContainedRef | None:
    if isinstance(value, ContainedRef):
        return value
    if isinstance(value, RefArg):
        return ContainedRef(value.object_id, value.owner_worker_id)
    # Future public ObjectRef implementations can participate without making
    # this low-level module depend on the API layer.  Requiring both attributes
    # avoids treating ordinary objects with an ``object_id`` field as refs.
    if (
        type(value).__name__ in {"ObjectRef", "ObjectRefHandle"}
        and hasattr(value, "object_id")
        and hasattr(value, "owner_worker_id")
    ):
        return ContainedRef(value.object_id, value.owner_worker_id)  # type: ignore[attr-defined]
    return None


def _validate_reference_owners(references: Iterable[ContainedRef]) -> None:
    owners: dict[ObjectIDLike, WorkerIDLike] = {}
    for reference in references:
        old_owner = owners.setdefault(reference.object_id, reference.owner_worker_id)
        if old_owner != reference.owner_worker_id:
            raise ArgumentEncodingError(
                f"object {reference.object_id!r} names conflicting owners"
            )


def _deduplicate(values: Iterable[ObjectIDLike]) -> tuple[ObjectIDLike, ...]:
    result: list[ObjectIDLike] = []
    seen: set[ObjectIDLike] = set()
    for value in values:
        _require_hashable(value, "object_id")
        if value not in seen:
            seen.add(value)
            result.append(value)
    return tuple(result)


def _require_hashable(value: object, label: str) -> None:
    try:
        hash(value)
    except TypeError as exc:
        raise TypeError(f"{label} must be hashable") from exc


def _close_imported_reference(value: object) -> None:
    close = getattr(value, "close", None)
    if callable(close):
        close()
