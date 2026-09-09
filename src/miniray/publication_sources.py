"""Tier-neutral source capabilities shared by output publication and child owners.

These immutable values contain identity only: no journal, result payload,
network operation or owner mutation. Fingerprints retain their original domain
and framing so moving the definitions cannot rebind a publication manifest.
Protocol types are imported only while validating a value; importing this leaf
module must not eagerly import the protocol or publication authorities.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

from .contained_edges import (
    ContainedReferenceEdge, ContainedReferenceHold,
    OwnerAddress,
)
from .ids import NodeID, ObjectID, WorkerID

if TYPE_CHECKING:
    from .protocol import BorrowSource, NodeDeathRecord


def _validate_address(value: object) -> OwnerAddress:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or not isinstance(value[0], str)
        or not value[0]
        or isinstance(value[1], bool)
        or not isinstance(value[1], int)
        or not 1 <= value[1] <= 65535
    ):
        raise ValueError("owner address must be a bound (host, port) tuple")
    return value[0], value[1]


def _require_positive_int(value: object, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{label} must be a positive integer")


@dataclass(frozen=True, order=True)
class PublicationNodeIncarnation:
    """Physical publishing-Node identity used by the GCS death fence."""

    node_id: NodeID
    node_pid: int
    registration_epoch: int

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            raise TypeError("node_id must be a NodeID")
        _require_positive_int(self.node_pid, "node_pid")
        _require_positive_int(
            self.registration_epoch, "registration_epoch"
        )

    @classmethod
    def from_death(
        cls, death: NodeDeathRecord
    ) -> "PublicationNodeIncarnation":
        from .protocol import NodeDeathRecord

        if not isinstance(death, NodeDeathRecord):
            raise TypeError("death must be a NodeDeathRecord")
        return cls(
            death.node_id, death.node_pid, death.registration_epoch
        )


@dataclass(frozen=True, order=True)
class OwnedContainedSource:
    """A child ObjectRef directly owned by its serializer."""

    owner_worker_id: WorkerID

    def __post_init__(self) -> None:
        if not isinstance(self.owner_worker_id, WorkerID):
            raise TypeError("owner_worker_id must be a WorkerID")


@dataclass(frozen=True)
class BorrowedContainedSource:
    """Complete live borrower proof used to derive a contained pin.

    ``original_source`` is the immutable binding recorded when the borrower was
    acquired.  A token by itself is deliberately insufficient authority.
    """

    borrower_worker_id: WorkerID
    borrower_token: str
    original_source: BorrowSource

    def __post_init__(self) -> None:
        from .protocol import ContainedTransferSource, TaskHoldSource

        if not isinstance(self.borrower_worker_id, WorkerID):
            raise TypeError("borrower_worker_id must be a WorkerID")
        if not isinstance(self.borrower_token, str) or not self.borrower_token:
            raise ValueError("borrower_token must be a non-empty string")
        if not isinstance(
            self.original_source, (ContainedTransferSource, TaskHoldSource)
        ):
            raise TypeError(
                "original_source must be a contained or task-hold source"
            )
        if (type(self.original_source) is ContainedTransferSource
                and type(self.original_source.hold) is not ContainedReferenceHold):
            raise TypeError("contained original_source requires a ContainedReferenceHold")

    @property
    def owner_table_token(self) -> tuple[WorkerID, str]:
        return self.borrower_worker_id, self.borrower_token


ContainedPublicationSource = Union[
    OwnedContainedSource, BorrowedContainedSource
]


@dataclass(frozen=True)
class PreparedContainedTransfer:
    """One ordered child transfer and both custody identities."""

    contained_object_id: ObjectID
    contained_owner_worker_id: WorkerID
    contained_owner_address: OwnerAddress
    source: ContainedPublicationSource
    provisional_hold: ContainedReferenceHold
    final_hold: ContainedReferenceHold

    def __post_init__(self) -> None:
        if not isinstance(self.contained_object_id, ObjectID):
            raise TypeError("contained_object_id must be an ObjectID")
        if not isinstance(self.contained_owner_worker_id, WorkerID):
            raise TypeError("contained_owner_worker_id must be a WorkerID")
        object.__setattr__(
            self, "contained_owner_address",
            _validate_address(self.contained_owner_address),
        )
        if not isinstance(
            self.source, (OwnedContainedSource, BorrowedContainedSource)
        ):
            raise TypeError("source must be an owned or borrowed source")
        if isinstance(self.source, OwnedContainedSource) and (
            self.source.owner_worker_id != self.contained_owner_worker_id
        ):
            raise ValueError("owned source must name the contained owner")
        if not isinstance(self.provisional_hold, ContainedReferenceHold):
            raise TypeError("provisional_hold must be a ContainedReferenceHold")
        if not isinstance(self.final_hold, ContainedReferenceHold):
            raise TypeError("final_hold must be a ContainedReferenceHold")
        if self.provisional_hold.container_object_id != self.final_hold.container_object_id:
            raise ValueError(
                "provisional and final holds must share outer"
            )
        same_owner = (
            self.provisional_hold.container_owner_worker_id
            == self.final_hold.container_owner_worker_id
        )
        if same_owner and self.provisional_hold == self.final_hold:
            raise ValueError(
                "provisional and final holds must express a custody change"
            )
        expected_provisional = (
            "provisional:{}".format(self.final_hold.transfer_token)
            if same_owner else self.final_hold.transfer_token
        )
        if self.provisional_hold.transfer_token != expected_provisional:
            raise ValueError(
                "provisional and final holds must share outer and token namespace"
            )

    @property
    def edge(self) -> ContainedReferenceEdge:
        return ContainedReferenceEdge(
            self.final_hold.container_object_id,
            self.contained_object_id,
            self.contained_owner_worker_id,
            self.contained_owner_address,
            self.final_hold.transfer_token,
        )


def prepared_contained_transfer_fingerprint(
    transfer: PreparedContainedTransfer,
) -> bytes:
    """Return a stable digest of one complete child-source capability.

    An outgoing edge does not authorize a borrowed child. Manifests bind the
    original borrower source, both custody holds, and the child-owner route in
    a single deterministic fingerprint.  Runtime borrower tokens and contained
    transfer tokens are strings by protocol contract; refusing opaque Python
    objects here keeps the wire identity independent from ``repr`` or pickle.
    """

    from .protocol import ContainedTransferSource, TaskHoldSource

    if not isinstance(transfer, PreparedContainedTransfer):
        raise TypeError(
            "transfer must be a PreparedContainedTransfer"
        )

    digest = hashlib.sha256()
    digest.update(b"miniray-prepared-contained-transfer-v1\0")

    def framed(value: bytes) -> None:
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)

    def string(value: object, label: str) -> bytes:
        if not isinstance(value, str) or not value:
            raise TypeError(f"{label} must be a non-empty string")
        return value.encode("utf-8")

    def object_id(value: ObjectID) -> None:
        framed(bytes(value.task_id))
        framed(value.return_index.to_bytes(8, "big"))

    def contained_hold(value: ContainedReferenceHold) -> None:
        if type(value) is not ContainedReferenceHold:
            raise TypeError("contained hold must be a ContainedReferenceHold")
        object_id(value.container_object_id)
        framed(bytes(value.container_owner_worker_id))
        framed(string(value.transfer_token, "contained transfer token"))

    object_id(transfer.contained_object_id)
    framed(bytes(transfer.contained_owner_worker_id))
    host, port = transfer.contained_owner_address
    framed(host.encode("utf-8"))
    framed(port.to_bytes(8, "big"))

    source = transfer.source
    if isinstance(source, OwnedContainedSource):
        framed(b"owned")
        framed(bytes(source.owner_worker_id))
    else:
        framed(b"borrowed")
        framed(bytes(source.borrower_worker_id))
        framed(string(source.borrower_token, "borrower_token"))
        original = source.original_source
        if isinstance(original, ContainedTransferSource):
            framed(b"contained")
            original_hold = original.hold
            if type(original_hold) is not ContainedReferenceHold:
                raise TypeError(
                    "contained source requires a ContainedReferenceHold"
                )
            framed(b"typed")
            contained_hold(original_hold)
        elif isinstance(original, TaskHoldSource):
            framed(b"task")
            task_hold = original.hold
            framed(task_hold.kind.value.encode("utf-8"))
            framed(bytes(task_hold.submitting_worker_id))
            framed(bytes(task_hold.task_id))
            framed(bytes(task_hold.origin_attempt_id.task_id))
            framed(task_hold.origin_attempt_id.attempt_number.to_bytes(8, "big"))
        else:
            raise TypeError("original_source must be a contained or task-hold source")

    contained_hold(transfer.provisional_hold)
    contained_hold(transfer.final_hold)
    return digest.digest()


__all__ = [
    "PublicationNodeIncarnation", "OwnedContainedSource", "BorrowedContainedSource",
    "ContainedPublicationSource", "PreparedContainedTransfer",
    "prepared_contained_transfer_fingerprint",
]
