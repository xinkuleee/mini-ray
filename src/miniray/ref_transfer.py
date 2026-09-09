"""Scoped cloudpickle hooks for exporting and restoring logical ObjectRefs.

The ordinary ObjectRef reducer stays side-effect free. Result publication
installs an exporter; ``CoreWorker.get`` and Worker argument materialization
install scoped importers. This keeps reference-accounting RPCs out of generic
pickle internals while preserving nested logical handles across both paths.
"""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Callable, Iterator, Optional, Tuple, Union

from .contained_edges import ContainedReferenceHold
from .ids import ObjectID, WorkerID
from .publication_sources import (
    BorrowedContainedSource, ContainedPublicationSource, OwnedContainedSource,
)
from .transport import Address


ContainedPin = ContainedReferenceHold
ExportedReference = Tuple[ObjectID, WorkerID, Address, ContainedPin]
ExportCallback = Callable[[object], ExportedReference]
ImportCallback = Callable[[ObjectID, WorkerID, Address, ContainedPin], object]
PinCallback = Callable[[ObjectID, ContainedPin], object]
UnpinCallback = Callable[[ObjectID, ContainedPin], object]
OwnerAddressProvider = Callable[[], Optional[Address]]

_local = threading.local()




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




__all__ = [
    "ExportedReference",
    "current_exporter",
    "discover_contained_reference",
    "exporting_references",
    "importing_references",
    "restore_exported_reference",
]
