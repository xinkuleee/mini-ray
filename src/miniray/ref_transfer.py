"""Scoped cloudpickle hooks for exporting and restoring logical ObjectRefs.

The ordinary ObjectRef reducer stays side-effect free. Result publication
installs an exporter; ``CoreWorker.get`` and Worker argument materialization
install scoped importers. This keeps reference-accounting RPCs out of generic
pickle internals while preserving nested logical handles across both paths.
"""

from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from typing import Callable, Iterator, Optional, Tuple, Union

from .contained_edges import (
    ContainedReferenceEdge,
    ContainedReferenceHold,
    IncomingContainedReferenceHold,
)
from .ids import ObjectID, WorkerID
from .publication_sources import (
    BorrowedContainedSource, ContainedPublicationSource, OwnedContainedSource,
)
from .transport import Address


ContainedPin = Union[IncomingContainedReferenceHold, str]
ExportedReference = Tuple[ObjectID, WorkerID, Address, ContainedPin]
ExportCallback = Callable[[object], ExportedReference]
ImportCallback = Callable[[ObjectID, WorkerID, Address, ContainedPin], object]
PinCallback = Callable[[ObjectID, ContainedPin], object]
UnpinCallback = Callable[[ObjectID, ContainedPin], object]
OwnerAddressProvider = Callable[[], Optional[Address]]

_local = threading.local()


def _pin_transfer_token(hold: ContainedPin) -> str:
    token = hold if isinstance(hold, str) else hold.transfer_token
    if not isinstance(token, str) or not token:
        raise TypeError("contained pin transfer token must be a non-empty string")
    return token


def current_exporter() -> Optional[ExportCallback]:
    return getattr(_local, "exporter", None)


def current_importer() -> Optional[ImportCallback]:
    return getattr(_local, "importer", None)


@contextmanager
def exporting_references(callback: ExportCallback) -> Iterator[None]:
    """Install one result-publication exporter on the current thread."""

    previous = current_exporter()
    _local.exporter = callback
    try:
        yield
    finally:
        if previous is None:
            try:
                del _local.exporter
            except AttributeError:
                pass
        else:
            _local.exporter = previous


@contextmanager
def importing_references(callback: ImportCallback) -> Iterator[None]:
    """Install one borrower-registration importer on the current thread."""

    previous = current_importer()
    _local.importer = callback
    try:
        yield
    finally:
        if previous is None:
            try:
                del _local.importer
            except AttributeError:
                pass
        else:
            _local.importer = previous


def restore_exported_reference(
    object_id: ObjectID,
    owner_worker_id: WorkerID,
    owner_address: Address,
    hold: ContainedPin,
) -> object:
    """Cloudpickle reconstruction hook requiring an explicit receiver.

    Returning an unaccounted detached handle here would open a lifetime gap:
    the sender could release its last local handle before the receiver tells
    the owner it borrowed the object.  The active importer performs that ACK
    synchronously before exposing the reconstructed Python object.
    """

    importer = current_importer()
    if importer is None:
        raise RuntimeError(
            "exported mini-Ray ObjectRef requires a scoped Core/Worker importer"
        )
    return importer(
        object_id, owner_worker_id, owner_address, hold
    )


def discover_contained_reference(
    reference: object,
    executor_worker_id: WorkerID,
    owner_address: Union[Address, OwnerAddressProvider, None],
    *,
    use_reference_owner_address: bool = False,
) -> Tuple[ObjectID, WorkerID, Address, ContainedPublicationSource]:
    """Copy a live handle's custody source without acquiring any hold.

    This is the shared discovery boundary for single and multi-slot result
    publication.  It neither creates tokens nor invokes owner/Node callbacks.
    A provided owner-address callback is only the executor's local route
    provider.  Foreign handles always retain their own exact owner route and
    the source that authorized their existing borrower credential.

    Callers may opt into using a local ObjectRef's route when no explicit
    executor route is supplied, but only after its identity and absence of
    borrower credentials have been validated.
    """

    object_id = getattr(reference, "object_id", None)
    owner_worker_id = getattr(reference, "owner_worker_id", None)
    if bool(getattr(reference, "closed", False)):
        raise RuntimeError("cannot export a closed ObjectRef")
    if not isinstance(object_id, ObjectID):
        raise TypeError("exported reference must contain an ObjectID")
    borrower_token = getattr(reference, "borrower_token", None)
    borrow_source = getattr(reference, "borrow_source", None)
    if owner_worker_id == executor_worker_id:
        if borrower_token is not None or borrow_source is not None:
            raise ValueError(
                "executor-owned ObjectRef has conflicting borrower credentials"
            )
        if owner_address is None and use_reference_owner_address:
            owner_address = getattr(reference, "owner_address", None)
        address = owner_address() if callable(owner_address) else owner_address
        if address is None:
            raise RuntimeError(
                "stored ObjectRef export requires a bound owner endpoint"
            )
        source = OwnedContainedSource(owner_worker_id)
    else:
        address = getattr(reference, "owner_address", None)
        if address is None:
            raise ValueError("foreign ObjectRef is detached from its owner endpoint")
        if borrower_token is None or borrow_source is None:
            raise ValueError(
                "foreign ObjectRef is detached from its live borrower credential"
            )
        source = BorrowedContainedSource(
            executor_worker_id, borrower_token, borrow_source
        )
    return object_id, owner_worker_id, address, source


class ReferenceExportSession:
    """Transactional handoff pins for one task's result serialization.

    Reducers may run before cloudpickle discovers a later unserializable value.
    Every pin is therefore rolled back unless the caller commits after all
    result descriptors have been constructed.
    """

    def __init__(
        self,
        owner_worker_id: WorkerID,
        owner_address: Union[Address, OwnerAddressProvider, None],
        *,
        pin: PinCallback,
        unpin: UnpinCallback,
        container_object_id: Optional[ObjectID] = None,
        container_owner_worker_id: Optional[WorkerID] = None,
    ) -> None:
        if (container_object_id is None) != (
            container_owner_worker_id is None
        ):
            raise ValueError(
                "container object and owner identities must appear together"
            )
        if container_object_id is not None and not isinstance(
            container_object_id, ObjectID
        ):
            raise TypeError("container_object_id must be an ObjectID or None")
        if container_owner_worker_id is not None and not isinstance(
            container_owner_worker_id, WorkerID
        ):
            raise TypeError(
                "container_owner_worker_id must be a WorkerID or None"
            )
        self.owner_worker_id = owner_worker_id
        self.container_object_id = container_object_id
        self.container_owner_worker_id = container_owner_worker_id
        self._owner_address = owner_address
        self._pin = pin
        self._unpin = unpin
        self._pins: list[tuple[ObjectID, ContainedPin]] = []
        self._exports: list[ExportedReference] = []
        self._committed = False
        self._committed_container_id: Optional[ObjectID] = None
        self._committed_edges: tuple[ContainedReferenceEdge, ...] = ()

    @property
    def exported_count(self) -> int:
        return len(self._pins)

    def export(self, reference: object) -> ExportedReference:
        object_id = getattr(reference, "object_id", None)
        owner_worker_id = getattr(reference, "owner_worker_id", None)
        closed = bool(getattr(reference, "closed", False))
        if closed:
            raise RuntimeError("cannot export a closed ObjectRef")
        if not isinstance(object_id, ObjectID):
            raise TypeError("exported reference must contain an ObjectID")
        if owner_worker_id != self.owner_worker_id:
            raise ValueError(
                "this slice can export only ObjectRefs owned by this Worker"
            )
        owner_address = (
            self._owner_address()
            if callable(self._owner_address)
            else self._owner_address
        )
        if owner_address is None:
            raise RuntimeError(
                "exporting an ObjectRef requires a bound Worker owner endpoint"
            )
        transfer_token = "transfer:{}".format(uuid.uuid4().hex)
        hold: ContainedPin = (
            ContainedReferenceHold(
                self.container_object_id,
                self.container_owner_worker_id,
                transfer_token,
            )
            if self.container_object_id is not None
            and self.container_owner_worker_id is not None
            else transfer_token
        )
        self._pin(object_id, hold)
        self._pins.append((object_id, hold))
        exported = (
            object_id,
            self.owner_worker_id,
            owner_address,
            hold,
        )
        self._exports.append(exported)
        return exported

    def commit(
        self, container_object_id: Optional[ObjectID] = None
    ) -> tuple[ContainedReferenceEdge, ...]:
        """Commit pins and optionally bind them to one outer ObjectID.

        Existing runtime callers omit ``container_object_id`` and therefore do
        not yet install outgoing edges.  The pure-model slice accepts it and
        returns immutable release obligations for the outer owner to persist.
        """

        if container_object_id is None:
            container_object_id = self.container_object_id
        if container_object_id is not None and not isinstance(
            container_object_id, ObjectID
        ):
            raise TypeError("container_object_id must be an ObjectID or None")
        if self._committed:
            if container_object_id != self._committed_container_id:
                raise RuntimeError(
                    "reference export session was committed for another container"
                )
            return self._committed_edges
        edges = self.edges_for_container(container_object_id)
        self._committed = True
        self._committed_container_id = container_object_id
        self._committed_edges = edges
        return edges

    def edges_for_container(
        self, container_object_id: Optional[ObjectID]
    ) -> tuple[ContainedReferenceEdge, ...]:
        """Build edges without committing, so envelope validation may fail safely."""

        if container_object_id is None:
            container_object_id = self.container_object_id
        if container_object_id is not None and not isinstance(
            container_object_id, ObjectID
        ):
            raise TypeError("container_object_id must be an ObjectID or None")
        return (
            tuple(
                ContainedReferenceEdge(
                    container_object_id, object_id, owner_worker_id,
                    owner_address, _pin_transfer_token(hold),
                )
                for object_id, owner_worker_id, owner_address, hold
                in self._exports
            )
            if container_object_id is not None
            else ()
        )

    @property
    def committed_edges(self) -> tuple[ContainedReferenceEdge, ...]:
        """Edges returned at commit; empty means runtime wiring is absent."""

        if not self._committed:
            raise RuntimeError("reference export session is not committed")
        return self._committed_edges

    def rollback(self) -> None:
        """Hand every pin to the owner's durable release authority.

        Runtime callbacks must persist the exact identity before returning.
        We still attempt every pin so one transient failure cannot
        strand the rest of a multi-reference rollback.  The publication error
        remains primary; durable retry and clean-shutdown gating belong to the
        owner/Core callback, not this short-lived serialization scope.
        """

        failures: list[tuple[ObjectID, ContainedPin]] = []
        while self._pins:
            object_id, hold = self._pins.pop()
            try:
                self._unpin(object_id, hold)
            except Exception:
                # A runtime callback must either return after durable handoff
                # or raise without claiming it; generic library callbacks may
                # fail before handoff, so retain that pin for explicit replay.
                failures.append((object_id, hold))
        # Generic library callbacks have no durable owner.  Preserve failed
        # identities for an explicit rollback replay instead of pretending the
        # pins vanished.  The Worker callback persists into Core before any
        # fallible release and normally leaves this list empty.
        self._pins.extend(reversed(failures))
        if not failures:
            self._exports.clear()

    def __enter__(self) -> "ReferenceExportSession":
        self._scope = exporting_references(self.export)
        self._scope.__enter__()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            if exc_type is not None or not self._committed:
                self.rollback()
        finally:
            self._scope.__exit__(exc_type, exc, tb)


__all__ = [
    "ExportedReference",
    "ReferenceExportSession",
    "current_exporter",
    "discover_contained_reference",
    "exporting_references",
    "importing_references",
    "restore_exported_reference",
]
