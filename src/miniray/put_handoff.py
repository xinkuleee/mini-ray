"""Pure single-value discovery for owner-created put objects.

Discovery serializes once, captures complete child-source metadata and retains
the actual Python handles. It performs no reference, store or RPC effects.
The Core must register the complete manifest before those effects, and retain
PutPrepared until final child handoff or exact compensation has completed.
No Task, lease, execution-success record or producer lineage is invented.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, fields, replace

import cloudpickle

from . import protocol
from .contained_edges import (
    ContainedReferenceEdge, ContainedReferenceHold,
)
from .ids import AttemptID, ObjectID, TaskID, WorkerID
from .publication_sources import (
    BorrowedContainedSource, OwnedContainedSource, PreparedContainedTransfer,
    prepared_contained_transfer_fingerprint,
)
from .ref_transfer import discover_contained_reference, exporting_references
from .transport import Address


_METADATA_TYPES = (
    TaskID, WorkerID, ObjectID, AttemptID, ContainedReferenceHold,
    protocol.TaskReferenceHold,
    protocol.ContainedTransferSource, protocol.TaskHoldSource,
    OwnedContainedSource, BorrowedContainedSource, PreparedContainedTransfer,
)


def _copy_metadata(value):
    """Rebuild every nested identity through its original validation.

    Frozen dataclasses can still be altered through object.__setattr__. Exact
    metadata types prevent arbitrary payload attributes or subclass behavior
    from entering an owner manifest through a shallow dataclass replacement.
    """
    if type(value) in (str, int, bytes, protocol.TaskReferenceHoldKind):
        return value
    if type(value) is tuple:
        return tuple(_copy_metadata(item) for item in value)
    if type(value) in _METADATA_TYPES:
        return type(value)(**{
            item.name: _copy_metadata(getattr(value, item.name))
            for item in fields(value)
        })
    raise TypeError("put manifest contains an invalid metadata type")


@dataclass(frozen=True)
class PutManifest:
    """Complete put identity and cleanup targets, without result bytes."""

    object_id: ObjectID
    owner_worker_id: WorkerID
    tier: protocol.ResultStorage
    size_bytes: int
    checksum: str
    transfers: tuple[PreparedContainedTransfer, ...] = ()

    def __post_init__(self):
        if type(self.object_id) is not ObjectID or self.object_id.return_index != 0:
            raise ValueError("put requires one object at return_index zero")
        if type(self.owner_worker_id) is not WorkerID:
            raise TypeError("put owner must be a WorkerID")
        if type(self.tier) is not protocol.ResultStorage:
            raise TypeError("put tier must be a ResultStorage")
        if type(self.size_bytes) is not int or self.size_bytes < 0:
            raise ValueError("put size_bytes must be a non-negative integer")
        if (type(self.checksum) is not str or len(self.checksum) != 64
                or any(c not in "0123456789abcdef" for c in self.checksum)):
            raise ValueError("put checksum must be a lowercase SHA-256 hex digest")
        if type(self.transfers) is not tuple:
            raise TypeError("put transfers must be an ordered tuple")
        object_id = _copy_metadata(self.object_id)
        owner = _copy_metadata(self.owner_worker_id)
        transfers = _copy_metadata(self.transfers)
        final_holds = set()
        child_routes = {}
        for transfer in transfers:
            if type(transfer) is not PreparedContainedTransfer:
                raise TypeError("put transfer must be a PreparedContainedTransfer")
            prepared_contained_transfer_fingerprint(transfer)
            if any(hold.container_object_id != object_id
                   or hold.container_owner_worker_id != owner
                   for hold in (transfer.provisional_hold, transfer.final_hold)):
                raise ValueError("put child holds must name this outer and owner")
            source = transfer.source
            if type(source) is OwnedContainedSource:
                if source.owner_worker_id != owner:
                    raise ValueError("put owned source must belong to the put owner")
            elif source.borrower_worker_id != owner:
                raise ValueError("put borrowed source must name the put owner")
            elif transfer.contained_owner_worker_id == owner:
                raise ValueError("put cannot borrow its own directly owned child")
            route = transfer.contained_owner_worker_id, transfer.contained_owner_address
            if child_routes.setdefault(transfer.contained_object_id, route) != route:
                raise ValueError("put child has conflicting owner credentials")
            if transfer.final_hold in final_holds:
                raise ValueError("put transfer tokens must be unique")
            final_holds.add(transfer.final_hold)
        object.__setattr__(self, "object_id", object_id)
        object.__setattr__(self, "owner_worker_id", owner)
        object.__setattr__(self, "transfers", transfers)

    @property
    def edges(self) -> tuple[ContainedReferenceEdge, ...]:
        return tuple(transfer.edge for transfer in self.transfers)

    def __reduce__(self):
        return type(self), (
            self.object_id, self.owner_worker_id, self.tier, self.size_bytes,
            self.checksum, self.transfers,
        )


@dataclass(frozen=True)
class PutPrepared:
    """Local bytes plus source custody; this value must never cross RPC.

    Strong references prevent implicit handle finalization. They cannot prevent
    explicit user close; child admission must still validate the live source.
    """

    manifest: PutManifest
    payload: bytes
    sources: tuple[object, ...] = field(repr=False, compare=False)

    def __post_init__(self):
        if type(self.manifest) is not PutManifest:
            raise TypeError("prepared put requires a PutManifest")
        manifest = replace(self.manifest)
        if type(self.payload) is not bytes:
            raise TypeError("prepared put payload must be bytes")
        if (len(self.payload) != manifest.size_bytes
                or hashlib.sha256(self.payload).hexdigest() != manifest.checksum):
            raise ValueError("prepared put payload disagrees with its manifest")
        if type(self.sources) is not tuple or len(self.sources) != len(manifest.transfers):
            raise ValueError("prepared put must retain every source handle")
        object.__setattr__(self, "manifest", manifest)

    def __reduce__(self):
        raise TypeError("prepared put retains local custody and cannot be serialized")


def discover_put(
    value: object, object_id: ObjectID, owner_worker_id: WorkerID,
    owner_address: Address | None, inline_threshold: int,
) -> PutPrepared:
    """Serialize one value once, returning a complete effect-free handoff.

    Replays reuse this result. Distinct Python handles have distinct transfers;
    repeated aliases retain a single handle/hold and normal pickle aliasing.
    """
    if type(object_id) is not ObjectID or object_id.return_index != 0:
        raise ValueError("put requires one object at return_index zero")
    if type(owner_worker_id) is not WorkerID:
        raise TypeError("put owner must be a WorkerID")
    object_id = _copy_metadata(object_id)
    owner_worker_id = _copy_metadata(owner_worker_id)
    if type(inline_threshold) is not int or inline_threshold < 0:
        raise ValueError("inline_threshold must be a non-negative integer")
    sources, transfers, exports = [], [], {}

    def export(reference):
        previous = exports.get(id(reference))
        if previous is not None:
            return previous
        child, child_owner, address, source = discover_contained_reference(
            reference, owner_worker_id, owner_address,
        )
        token = "put:{}:{}:transfer:{}".format(
            owner_worker_id.hex, object_id.hex, len(transfers),
        )
        transfer = _copy_metadata(PreparedContainedTransfer(
            child, child_owner, address, source,
            ContainedReferenceHold(object_id, owner_worker_id, "provisional:" + token),
            ContainedReferenceHold(object_id, owner_worker_id, token),
        ))
        # Keep reducer-created temporary refs before returning their pickle tuple.
        sources.append(reference)
        transfers.append(transfer)
        exported = (transfer.contained_object_id, transfer.contained_owner_worker_id,
                    transfer.contained_owner_address, transfer.final_hold)
        exports[id(reference)] = exported
        return exported

    try:
        with exporting_references(export):
            payload = cloudpickle.dumps(value)
        manifest = PutManifest(
            object_id, owner_worker_id,
            protocol.ResultStorage.INLINE if len(payload) <= inline_threshold
            else protocol.ResultStorage.OBJECT_STORE,
            len(payload), hashlib.sha256(payload).hexdigest(), tuple(transfers),
        )
        return PutPrepared(manifest, payload, tuple(sources))
    except BaseException:
        # No child or Node effect exists, even when serialization failed late.
        sources.clear()
        exports.clear()
        transfers.clear()
        raise


__all__ = ["PutManifest", "PutPrepared", "discover_put"]
