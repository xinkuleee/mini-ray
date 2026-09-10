"""Local put custody and exact outstanding effects, without runtime authority.

Core owns locking, owner CAS and effect dispatch. These records retain only
the put's prepared sources, sent requests and actual peer receipts. They never
accept a Core, perform RPC, or derive success from the expected effect list.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from . import protocol
from .ids import AttemptID
from .publication_sources import PreparedContainedTransfer
from .put_handoff import PutPrepared
from .enhanced_publication import PutPublication, OwnerAbortReceipt

if TYPE_CHECKING:
    from .core import _HomeRoute


class PutChoice(Enum):
    OPEN = "OPEN"
    ABORTED = "ABORTED"
    COMMITTED = "COMMITTED"


@dataclass
class ChildTransferProgress:
    """Request presence is sent intent; receipt presence records actual ACK."""

    transfer: PreparedContainedTransfer
    prepare_request: protocol.PrepareStoredContainedPin | None = None
    prepare_receipt: protocol.StoredContainedPinReply | None = None
    promotion_request: protocol.PromoteStoredContainedPin | None = None
    promotion_receipt: protocol.StoredContainedPinReply | None = None
    releases: dict[protocol.ReleaseContainedReference,
                   protocol.ReleaseContainedReferenceReply | protocol.WorkerDeathRecord] = field(default_factory=dict)

    @property
    def has_sent_effect(self) -> bool:
        return self.prepare_request is not None or self.promotion_request is not None


@dataclass
class MaterializationWork:
    """One exact Node intent; a retained Drop prevents another Seal."""

    route: _HomeRoute
    seal_request: protocol.SealObject
    seal_receipt: protocol.SealObjectReply | None = None
    drop_request: protocol.DropObjectReplica | None = None
    drop_receipt: protocol.DropObjectReplicaReply | protocol.NodeDeathRecord | None = None

    @property
    def absence_fenced(self) -> bool:
        reply = self.seal_receipt
        return reply is not None and not reply.sealed and reply.absence_fenced


@dataclass
class PutHandoff:
    """One local choice and driver; prepared keeps every actual source alive."""

    prepared: PutPrepared
    attempt: AttemptID
    publication: PutPublication | None = None
    abort_receipt: OwnerAbortReceipt | None = None
    children: tuple[ChildTransferProgress, ...] = field(init=False)
    materialization: MaterializationWork | None = None
    choice: PutChoice = PutChoice.OPEN
    driving: bool = True

    def __post_init__(self) -> None:
        self.children = tuple(ChildTransferProgress(transfer)
                              for transfer in self.prepared.manifest.transfers)
