"""Serialize one task result once and discover its contained-reference custody.

The result may be any supported Python value, including a tuple or list; it is
one ObjectRef. Discovery selects INLINE or STORED, emits one metadata manifest
and retains source handles until their real handoff. It performs no RPC or
reference mutation. Payload bytes travel separately from control metadata.
Pickle memoization preserves aliases within that single object.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from typing import Callable, Optional, Tuple, Union

import cloudpickle

from .contained_edges import ContainedReferenceHold
from .ids import ObjectID
from .output_publication import (
    OutputPublicationHeader, OutputPublicationManifest, OutputValue,
)
from .protocol import ResultStorage
from .ref_transfer import (
    ExportedReference, OwnerAddressProvider, discover_contained_reference,
    exporting_references,
)
from .publication_sources import PreparedContainedTransfer
from .transport import Address


# Only the child reducer-discovery ordinal varies; the outer index is canonical zero.
OutputTransferTokenFactory = Callable[[int], str]


@dataclass(frozen=True)
class PreparedOutput:
    """Once-serialized local output; source handles stay in its session."""
    manifest: OutputPublicationManifest
    payload: bytes

    def __post_init__(self) -> None:
        if type(self.manifest) is not OutputPublicationManifest:
            raise TypeError("manifest must be an OutputPublicationManifest")
        manifest = replace(self.manifest)
        if type(self.payload) is not bytes:
            raise TypeError("payload must be serialized bytes")
        value = manifest.value
        if len(self.payload) != value.size_bytes or hashlib.sha256(self.payload).hexdigest() != value.checksum:
            raise ValueError("serialized payload does not match its value manifest")
        object.__setattr__(self, "manifest", manifest)

    def __reduce__(self):
        return type(self), (self.manifest, self.payload)


class OutputDiscoverySession:
    """One-shot Worker-local discovery and source-handle custody.

    ``discover`` returns only after the complete value has serialized and its
    immutable manifest validates.  Its first error aborts the local
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
        self._token_namespace = self.header.publication_id.transaction_id
        self._source_references: list[object] = []
        self._discovered: Optional[PreparedOutput] = None
        self._state = "NEW"

    @property
    def source_references(self) -> Tuple[object, ...]:
        """Strong handles, in reducer order, without any new owner pin."""

        return tuple(self._source_references)

    @property
    def discovered(self) -> Optional[PreparedOutput]:
        """Only a complete validated output is observable through this property."""

        return self._discovered

    def _token(self, object_id: ObjectID, transfer_index: int) -> str:
        token = (
            self._token_factory(transfer_index)
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
        # the rest of the value or the eventual all-promotions acknowledgement.
        self._source_references.append(reference)
        transfers.append(transfer)
        return child, owner, address, transfer.final_hold

    def discover(self, value: object) -> PreparedOutput:
        if self._state != "NEW":
            raise RuntimeError("output discovery is one-shot; reuse the serialized output")
        self._state = "DISCOVERING"
        try:
            object_id = self.header.publication_id.object_id
            transfers: list[PreparedContainedTransfer] = []
            with exporting_references(lambda reference: self._export(reference, object_id, transfers)):
                payload = cloudpickle.dumps(value)
            tier = ResultStorage.INLINE if len(payload) <= self.inline_threshold else ResultStorage.OBJECT_STORE
            output = OutputValue(tier, len(payload), hashlib.sha256(payload).hexdigest(), tuple(transfers))
            manifest = OutputPublicationManifest.create(self.header, output)
            result = PreparedOutput(manifest, payload)
        except BaseException:
            self._source_references.clear()
            self._discovered = None
            self._state = "ABORTED"
            raise
        self._discovered = result
        self._state = "DISCOVERED"
        return result

    def release_sources_after_promotions(self) -> None:
        """Called only after all child final holds for this output are ACKed."""

        if self._state == "RELEASED":
            return
        if self._state != "DISCOVERED":
            raise RuntimeError("a complete discovered output is required before release")
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
    "PreparedOutput", "OutputDiscoverySession", "OutputTransferTokenFactory",
]
