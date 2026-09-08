"""Historical pickle names for shared source capabilities, not a runtime.

New code imports :mod:`miniray.publication_sources`.  These same-object exports
keep previously serialized source/Node identities readable; the retired
single-return journal, transaction and recovery types are intentionally absent.
"""

from .publication_sources import (
    BorrowedContainedSource, ContainedPublicationSource, OwnedContainedSource,
    PreparedContainedTransfer, PublicationNodeIncarnation,
    prepared_contained_transfer_fingerprint,
)

StoredPublicationNodeIncarnation = PublicationNodeIncarnation

__all__ = [
    "BorrowedContainedSource", "ContainedPublicationSource", "OwnedContainedSource",
    "PreparedContainedTransfer", "StoredPublicationNodeIncarnation",
    "prepared_contained_transfer_fingerprint",
]
