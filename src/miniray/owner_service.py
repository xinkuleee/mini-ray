"""Driver owner-protocol endpoint backed by one authoritative CoreWorker.

This module is intentionally only a transport adapter.  Object state, tokens,
lineage, locations, and shutdown admission all remain owned by ``CoreWorker``;
the service merely gives remote borrowers a loopback endpoint from which to
invoke that existing authority.
"""

from __future__ import annotations

from dataclasses import replace
from types import MappingProxyType
from typing import Callable, Mapping, Protocol

from . import protocol
from .ownership import (
    ConflictingBorrowerTokenError,
    InvalidObjectTransitionError,
    ObjectCollectionInProgressError,
    OwnershipError,
    ReleasedBorrowerTokenError,
    StoredContainedReferenceDisposition,
)
from .trace import EventSink, NonOwningEventSink
from .transport import Address, LOOPBACK_HOST, TCPServer


ACQUIRE_BORROWED_OBJECT_HANDLER = "acquire_borrowed_object"
RELEASE_BORROWED_OBJECT_HANDLER = "release_borrowed_object"
GET_OWNED_OBJECT_HANDLER = "get_owned_object"
REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER = (
    "request_owned_object_reconstruction"
)
REQUEST_DROP_OWNED_OBJECT_HANDLER = "request_drop_owned_object"
RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER = "retain_owned_object_for_task"
GET_RETAINED_OWNED_OBJECT_HANDLER = "get_retained_owned_object"
REPORT_RETAINED_OBJECT_LOCATION_HANDLER = "report_retained_object_location"
REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER = protocol.REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER
RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER = "release_owned_object_for_task"
REPLACE_RETAINED_OBJECT_FOR_TASK_HANDLER = (
    "replace_retained_object_for_task"
)
RELEASE_CONTAINED_REFERENCE_HANDLER = "release_contained_reference"
INSTALL_ACTOR_STATE_HANDLER = "install_actor_state"
PREPARE_STORED_CONTAINED_PIN_HANDLER = "prepare_stored_contained_pin"
PROMOTE_STORED_CONTAINED_PIN_HANDLER = "promote_stored_contained_pin"


class OwnerAuthority(Protocol):
    """The existing Core surface exposed by the transport adapter."""

    def acquire_exported_reference(self, request: object) -> object: ...
    def release_borrowed_reference(self, request: object) -> object: ...
    def get_owned_object(self, request: object) -> object: ...
    def request_owned_object_reconstruction(self, request: object) -> object: ...
    def request_drop_owned_object(self, request: object) -> object: ...
    def retain_owned_object_for_task(self, request: object) -> object: ...
    def get_retained_owned_object(self, request: object) -> object: ...
    def report_retained_object_location(self, request: object) -> object: ...
    def report_abandoned_dependency_replica(self, request: object) -> object: ...
    def release_owned_object_for_task(self, request: object) -> object: ...
    def replace_retained_object_for_task(self, request: object) -> object: ...
    def release_contained_reference(self, request: object) -> object: ...
    def install_actor_state(self, request: object) -> object: ...
    def prepare_stored_contained_pin(self, request: object) -> object: ...
    def promote_stored_contained_pin(self, request: object) -> object: ...
    event_sink: EventSink


class StoredContainedPinAuthority(Protocol):
    """Narrow pure owner-table surface used by the optional adapter."""

    def prepare_stored_contained_reference(
        self, transfer: object, *, authority_worker_id: object
    ) -> StoredContainedReferenceDisposition: ...

    def promote_stored_contained_reference(
        self, transfer: object, *, authority_worker_id: object
    ) -> StoredContainedReferenceDisposition: ...


def _stored_pin_error_kind(
    error: BaseException,
) -> protocol.StoredPublicationRPCErrorKind:
    if isinstance(
        error,
        (ConflictingBorrowerTokenError, InvalidObjectTransitionError,
         ReleasedBorrowerTokenError),
    ):
        return protocol.StoredPublicationRPCErrorKind.CONFLICT
    if isinstance(error, ObjectCollectionInProgressError):
        return protocol.StoredPublicationRPCErrorKind.INVALID_STATE
    if isinstance(error, (TypeError, ValueError, protocol.ProtocolError)):
        return protocol.StoredPublicationRPCErrorKind.INVALID_REQUEST
    if isinstance(error, OwnershipError):
        return protocol.StoredPublicationRPCErrorKind.INVALID_STATE
    return protocol.StoredPublicationRPCErrorKind.INTERNAL


class StoredContainedPinOwnerAdapter:
    """Typed transport facade over owner-table prepare/promote methods."""

    def __init__(self, authority: StoredContainedPinAuthority) -> None:
        self._authority = authority

    @property
    def handlers(self) -> Mapping[str, Callable[[object], object]]:
        return MappingProxyType({
            PREPARE_STORED_CONTAINED_PIN_HANDLER: self.prepare,
            PROMOTE_STORED_CONTAINED_PIN_HANDLER: self.promote,
        })

    def prepare(self, request: object) -> protocol.StoredContainedPinReply:
        if not isinstance(request, protocol.PrepareStoredContainedPin):
            raise TypeError(
                "stored pin prepare expects PrepareStoredContainedPin"
            )
        try:
            disposition = (
                self._authority.prepare_stored_contained_reference(
                    request.transfer,
                    authority_worker_id=request.authority_worker_id,
                )
            )
        except Exception as exc:
            return protocol.StoredContainedPinReply(
                request, error_kind=_stored_pin_error_kind(exc),
                error=str(exc) or type(exc).__name__,
            )
        return protocol.StoredContainedPinReply(
            request, disposition=disposition
        )

    def promote(self, request: object) -> protocol.StoredContainedPinReply:
        if not isinstance(request, protocol.PromoteStoredContainedPin):
            raise TypeError(
                "stored pin promotion expects PromoteStoredContainedPin"
            )
        try:
            disposition = (
                self._authority.promote_stored_contained_reference(
                    request.transfer,
                    authority_worker_id=request.authority_worker_id,
                )
            )
        except Exception as exc:
            return protocol.StoredContainedPinReply(
                request, error_kind=_stored_pin_error_kind(exc),
                error=str(exc) or type(exc).__name__,
            )
        return protocol.StoredContainedPinReply(
            request, disposition=disposition
        )


class OwnerService:
    """A stateless loopback TCP facade over one Driver CoreWorker."""

    def __init__(
        self,
        core: OwnerAuthority,
        *,
        host: str = LOOPBACK_HOST,
        port: int = 0,
        request_timeout: float = 5.0,
    ) -> None:
        self._core = core
        sink = getattr(core, "event_sink", None)
        owner_sink = (
            NonOwningEventSink(sink, component="owner_service")
            if isinstance(sink, EventSink)
            else None
        )
        handlers: dict[str, Callable[[object], object]] = {
                ACQUIRE_BORROWED_OBJECT_HANDLER:
                    core.acquire_exported_reference,
                RELEASE_BORROWED_OBJECT_HANDLER:
                    core.release_borrowed_reference,
                GET_OWNED_OBJECT_HANDLER: core.get_owned_object,
                REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER:
                    core.request_owned_object_reconstruction,
                REQUEST_DROP_OWNED_OBJECT_HANDLER:
                    core.request_drop_owned_object,
                RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER:
                    core.retain_owned_object_for_task,
                GET_RETAINED_OWNED_OBJECT_HANDLER:
                    core.get_retained_owned_object,
                REPORT_RETAINED_OBJECT_LOCATION_HANDLER:
                    core.report_retained_object_location,
                RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER:
                    core.release_owned_object_for_task,
                REPLACE_RETAINED_OBJECT_FOR_TASK_HANDLER:
                    core.replace_retained_object_for_task,
                RELEASE_CONTAINED_REFERENCE_HANDLER:
                    core.release_contained_reference,
                INSTALL_ACTOR_STATE_HANDLER: core.install_actor_state,
        }
        # Phase C1 makes these operations optional until CoreWorker adopts the
        # new business methods.  Absence means no advertised RPC surface; it
        # must never make an otherwise valid OwnerService fail construction.
        prepare = getattr(core, "prepare_stored_contained_pin", None)
        promote = getattr(core, "promote_stored_contained_pin", None)
        if callable(prepare) and callable(promote):
            handlers.update({
                PREPARE_STORED_CONTAINED_PIN_HANDLER: prepare,
                PROMOTE_STORED_CONTAINED_PIN_HANDLER: promote,
            })
        if callable(getattr(core, "report_abandoned_dependency_replica", None)):
            handlers[REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER] = self._handle_report_abandoned_dependency_replica
        self._server = TCPServer(
            handlers,
            host=host,
            port=port,
            request_timeout=request_timeout,
            event_sink=owner_sink,
            trace_component="owner_service",
        )

    def _handle_report_abandoned_dependency_replica(self, request: object) -> object:
        if type(request) is not protocol.ReportAbandonedDependencyReplica:
            raise TypeError("abandoned replica report requires its exact typed request")
        request = replace(request)
        core = self._core
        if core is None or not callable(getattr(core, "report_abandoned_dependency_replica", None)):
            return protocol.ReportAbandonedDependencyReplicaReply(
                request, protocol.RetainedLocationReportStatus.REJECTED, "object owner CoreWorker is not available",
            )
        if request.descriptor.owner_worker_id != getattr(core, "worker_id", None):
            return protocol.ReportAbandonedDependencyReplicaReply(
                request, protocol.RetainedLocationReportStatus.REJECTED, "request targets a different object owner",
            )
        return core.report_abandoned_dependency_replica(request)

    @property
    def address(self) -> Address:
        return self._server.address

    @property
    def is_running(self) -> bool:
        return self._server.is_running

    def start(self) -> Address:
        return self._server.start()

    def stop(self) -> None:
        self._server.stop()


__all__ = [
    "ACQUIRE_BORROWED_OBJECT_HANDLER",
    "GET_OWNED_OBJECT_HANDLER",
    "GET_RETAINED_OWNED_OBJECT_HANDLER",
    "INSTALL_ACTOR_STATE_HANDLER",
    "PREPARE_STORED_CONTAINED_PIN_HANDLER",
    "PROMOTE_STORED_CONTAINED_PIN_HANDLER",
    "OwnerAuthority",
    "OwnerService",
    "StoredContainedPinAuthority",
    "StoredContainedPinOwnerAdapter",
    "REQUEST_OWNED_OBJECT_RECONSTRUCTION_HANDLER",
    "REQUEST_DROP_OWNED_OBJECT_HANDLER",
    "RELEASE_BORROWED_OBJECT_HANDLER",
    "RELEASE_CONTAINED_REFERENCE_HANDLER",
    "RELEASE_OWNED_OBJECT_FOR_TASK_HANDLER",
    "REPLACE_RETAINED_OBJECT_FOR_TASK_HANDLER",
    "REPORT_RETAINED_OBJECT_LOCATION_HANDLER",
    "REPORT_ABANDONED_DEPENDENCY_REPLICA_HANDLER",
    "RETAIN_OWNED_OBJECT_FOR_TASK_HANDLER",
]
