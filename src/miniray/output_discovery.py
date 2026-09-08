"""Zero-effect serialization of one complete selected-output batch.

Discovery has one job: serialize each selected return once, identify its
contained-reference custody, and choose its storage tier.  It never acquires
pins, performs RPC, commits a graph, or manufactures a Complete witness.
Only the metadata manifest may reach GCS; ``slot_payloads`` are a separate
Worker-to-Node data-plane value.

Tier selection is per slot, not cumulative across the return batch: equality
with ``inline_threshold`` stays INLINE.  This preserves the existing Worker
result policy; the cumulative budget for *task arguments* is a different rule.

Python pickle memoization defines aliasing within a slot.  Repeated occurrences
of one Python ObjectRef produce one transfer and restore one handle.  Distinct
Python handles for the same logical child remain distinct transfers, matching
the existing single-slot exporter.  Every slot starts a new memo and names its
own container hold, so collecting one sibling cannot release another's child.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from typing import Callable, Optional, Tuple, Union

import cloudpickle

from .contained_edges import ContainedReferenceHold
from .ids import ObjectID
from .output_publication import (
    OutputPublicationHeader, OutputPublicationManifest, OutputSlotManifest,
)
from .protocol import ResultStorage
from .ref_transfer import (
    ExportedReference, OwnerAddressProvider, discover_contained_reference,
    exporting_references,
)
from .publication_sources import PreparedContainedTransfer
from .transport import Address


# The first index is the original ObjectID.return_index, including targeted
# subsets; the second is the reducer-discovery ordinal within that slot.
OutputTransferTokenFactory = Callable[[int, int], str]


@dataclass(frozen=True)
class DiscoveredOutputs:
    """A complete metadata batch and its ordered, once-serialized streams.

    This is a local/data-plane artifact, not a control-plane message.  It does
    not retain the original user values or source handles; the discovery
    session owns those temporary handles until promotion is acknowledged.
    """

    manifest: OutputPublicationManifest
    slot_payloads: Tuple[bytes, ...]

    def __post_init__(self) -> None:
        if type(self.manifest) is not OutputPublicationManifest:
            raise TypeError("manifest must be an OutputPublicationManifest")
        manifest = replace(self.manifest)
        if type(self.slot_payloads) not in (tuple, list):
            raise TypeError("slot_payloads must be an ordered tuple or list")
        payloads = tuple(self.slot_payloads)
        if len(payloads) != len(manifest.slots):
            raise ValueError("slot payloads must exactly cover the selected outputs")
        for slot, payload in zip(manifest.slots, payloads):
            if type(payload) is not bytes:
                raise TypeError("slot payload must be serialized bytes")
            if (
                len(payload) != slot.size_bytes
                or hashlib.sha256(payload).hexdigest() != slot.checksum
            ):
                raise ValueError("serialized payload does not match its slot manifest")
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "slot_payloads", payloads)

    def __reduce__(self):
        return type(self), (self.manifest, self.slot_payloads)


class OutputDiscoverySession:
    """One-shot Worker-local discovery and source-handle custody.

    ``discover`` returns only after every selected slot has serialized and the
    complete immutable manifest validates.  Its first error aborts the local
    session, clears all retained handles, and cannot be retried on that session:
    user reducers may have side effects, so serialization is never silently
    rerun.  Publication/RPC retries reuse the returned bytes and manifest.

    The caller must retain this session through the all-promotions ACK barrier
    and then call ``release_sources_after_promotions`` explicitly.  The method
    records a caller-owned lifecycle boundary, not an independently verified
    remote acknowledgement.  Aborting only releases local strong references;
    any Node-journal compensation after effects belongs to the coordinator.
    """

    def __init__(
        self, header: OutputPublicationHeader, *, inline_threshold: int,
        owner_address: Union[Address, OwnerAddressProvider, None] = None,
        token_factory: Optional[OutputTransferTokenFactory] = None,
    ) -> None:
        if type(header) is not OutputPublicationHeader:
            raise TypeError("header must be an OutputPublicationHeader")
        if type(inline_threshold) is not int or inline_threshold < 0:
            raise ValueError("inline_threshold must be a non-negative integer")
        if token_factory is not None and not callable(token_factory):
            raise TypeError("token_factory must be callable or None")
        self.header = replace(header)
        self.inline_threshold = inline_threshold
        self._owner_address = owner_address
        self._token_factory = token_factory
        self._token_namespace = self.header.publication_id.graph_transaction_id
        self._source_references: list[object] = []
        self._discovered: Optional[DiscoveredOutputs] = None
        self._state = "NEW"

    @property
    def source_references(self) -> Tuple[object, ...]:
        """Strong handles, in output/reducer order, without any new owner pin."""

        return tuple(self._source_references)

    @property
    def discovered(self) -> Optional[DiscoveredOutputs]:
        """Only a complete validated batch is observable through this property."""

        return self._discovered

    def _token(self, object_id: ObjectID, transfer_index: int) -> str:
        token = (
            self._token_factory(object_id.return_index, transfer_index)
            if self._token_factory is not None
            else "{}:slot:{}:transfer:{}".format(
                self._token_namespace, object_id.return_index, transfer_index
            )
        )
        if type(token) is not str or not token:
            raise ValueError("output transfer token factory must return a non-empty string")
        return token

    def _export(
        self, reference: object, object_id: ObjectID,
        transfers: list[PreparedContainedTransfer],
    ) -> ExportedReference:
        if self._state != "DISCOVERING":
            raise RuntimeError("output reference export is outside discovery")
        child, owner, address, source = discover_contained_reference(
            reference, self.header.executor_worker_id, self._owner_address,
            use_reference_owner_address=True,
        )
        token = self._token(object_id, len(transfers))
        # A nested submitter can execute its own retry on the surviving Worker.
        # The WorkerID then stays equal, but provisional custody and the final
        # outer lifetime must still be distinct references. Keep the final wire
        # token stable and domain-separate only the provisional token.
        provisional_token = (
            "provisional:{}".format(token)
            if self.header.executor_worker_id == self.header.owner_worker_id else token
        )
        transfer = PreparedContainedTransfer(
            child, owner, address, source,
            ContainedReferenceHold(object_id, self.header.executor_worker_id, provisional_token),
            ContainedReferenceHold(object_id, self.header.owner_worker_id, token),
        )
        # Keep the source before returning its reducer tuple.  A temporary
        # ObjectRef produced by user __reduce__ may otherwise disappear before
        # the following slot or the eventual all-promotions acknowledgement.
        self._source_references.append(reference)
        transfers.append(transfer)
        return child, owner, address, transfer.final_hold

    def discover(self, selected_values: Union[tuple, list]) -> DiscoveredOutputs:
        if self._state != "NEW":
            raise RuntimeError("output discovery is one-shot; reuse the serialized batch")
        self._state = "DISCOVERING"
        try:
            if type(selected_values) not in (tuple, list):
                raise TypeError("selected_values must be an ordered tuple or list")
            values = tuple(selected_values)
            output_ids = self.header.publication_id.output_ids
            if len(values) != len(output_ids):
                raise ValueError("values must exactly cover the selected output slots")
            slots = []
            payloads = []
            for object_id, value in zip(output_ids, values):
                transfers: list[PreparedContainedTransfer] = []
                with exporting_references(
                    lambda reference: self._export(reference, object_id, transfers)
                ):
                    payload = cloudpickle.dumps(value)
                tier = (
                    ResultStorage.INLINE
                    if len(payload) <= self.inline_threshold
                    else ResultStorage.OBJECT_STORE
                )
                slots.append(OutputSlotManifest(
                    object_id, tier, len(payload), hashlib.sha256(payload).hexdigest(),
                    tuple(transfers),
                ))
                payloads.append(payload)
            manifest = OutputPublicationManifest.create(self.header, tuple(slots))
            result = DiscoveredOutputs(manifest, tuple(payloads))
        except BaseException:
            self._source_references.clear()
            self._discovered = None
            self._state = "ABORTED"
            raise
        self._discovered = result
        self._state = "DISCOVERED"
        return result

    def release_sources_after_promotions(self) -> None:
        """Called only after all selected slots' final holds are ACKed."""

        if self._state == "RELEASED":
            return
        if self._state != "DISCOVERED":
            raise RuntimeError("a complete discovered batch is required before release")
        self._source_references.clear()
        self._state = "RELEASED"

    def abort(self) -> None:
        """Discard local discovery custody; never issue remote compensation."""

        if self._state == "DISCOVERING":
            raise RuntimeError("cannot abort reentrantly from a user reducer")
        if self._state == "RELEASED":
            return
        self._source_references.clear()
        self._discovered = None
        self._state = "ABORTED"

    def __reduce__(self):
        raise TypeError("output discovery session is local custody, not a wire value")


__all__ = [
    "DiscoveredOutputs", "OutputDiscoverySession", "OutputTransferTokenFactory",
]
