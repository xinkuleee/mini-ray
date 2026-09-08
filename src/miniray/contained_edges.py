"""Pure metadata for ObjectRefs contained by another logical object.

A contained-reference edge has two owners with different responsibilities:
the container owner retains this outgoing edge, while the contained-object
owner retains the matching transfer token as an incoming pin.  Collecting the
container must atomically return every outgoing edge so a future runtime layer
can release those remote pins without losing obligations.  This module performs
no RPC and never deletes physical object-store bytes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Hashable, Tuple, Union

from .ids import AttemptID, NodeID, ObjectID, WorkerID


OwnerAddress = Tuple[str, int]


def _validate_owner_address(value: object) -> OwnerAddress:
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or not isinstance(value[0], str)
        or not value[0]
        or isinstance(value[1], bool)
        or not isinstance(value[1], int)
        or not 1 <= value[1] <= 65535
    ):
        raise ValueError(
            "contained owner address must be a bound (host, port) tuple"
        )
    return value[0], value[1]


@dataclass(frozen=True, order=True)
class ContainedReferenceHold:
    """One incoming pin owned by a concrete container incarnation.

    ``container_object_id`` identifies the outer logical object whose
    metadata owns the matching outgoing edge.  ``container_owner_worker_id``
    identifies the physical Worker incarnation responsible for eventually
    releasing that edge.  Keeping both fields in the owner-table key lets an
    authoritative Worker-death fact retire exactly that Worker's abandoned
    pins without confusing equal transfer tokens from another container.
    """

    container_object_id: ObjectID
    container_owner_worker_id: WorkerID
    transfer_token: str

    def __post_init__(self) -> None:
        if not isinstance(self.container_object_id, ObjectID):
            raise TypeError("container_object_id must be an ObjectID")
        if not isinstance(self.container_owner_worker_id, WorkerID):
            raise TypeError(
                "container_owner_worker_id must be a WorkerID"
            )
        if not isinstance(self.transfer_token, str) or not self.transfer_token:
            raise ValueError("transfer_token must be a non-empty string")


@dataclass(frozen=True)
class LegacyContainedReferenceHold:
    """Compatibility identity for callers that know only a raw token.

    Historical mini-Ray pin APIs did not identify the outer object or the
    Worker incarnation that owned it.  Such pins remain valid lifetime
    reasons, but no Worker-death reducer may guess their owner.  Runtime paths
    can migrate incrementally by passing :class:`ContainedReferenceHold`; the
    raw-token projection remains deliberately isolated in this variant.
    """

    transfer_token: Hashable

    def __post_init__(self) -> None:
        try:
            hash(self.transfer_token)
        except TypeError as exc:
            raise TypeError("transfer_token must be hashable") from exc


IncomingContainedReferenceHold = Union[
    ContainedReferenceHold, LegacyContainedReferenceHold
]


@dataclass(frozen=True, order=True)
class ContainedReferenceEdge:
    """One durable container-to-contained-object lifetime edge."""

    container_object_id: ObjectID
    contained_object_id: ObjectID
    contained_owner_worker_id: WorkerID
    contained_owner_address: OwnerAddress
    transfer_token: str

    def __post_init__(self) -> None:
        if not isinstance(self.container_object_id, ObjectID):
            raise TypeError("container_object_id must be an ObjectID")
        if not isinstance(self.contained_object_id, ObjectID):
            raise TypeError("contained_object_id must be an ObjectID")
        if not isinstance(self.contained_owner_worker_id, WorkerID):
            raise TypeError("contained_owner_worker_id must be a WorkerID")
        object.__setattr__(
            self,
            "contained_owner_address",
            _validate_owner_address(self.contained_owner_address),
        )
        if not isinstance(self.transfer_token, str) or not self.transfer_token:
            raise ValueError("transfer_token must be a non-empty string")

    def incoming_hold(
        self, container_owner_worker_id: WorkerID
    ) -> ContainedReferenceHold:
        """Bind this outgoing edge to its responsible owner incarnation.

        The contained-object owner does not need the container's network
        address for lifetime accounting; it needs the stable logical outer ID,
        the physical owner incarnation that may die, and the transfer token.
        """

        return ContainedReferenceHold(
            self.container_object_id,
            container_owner_worker_id,
            self.transfer_token,
        )


@dataclass(frozen=True, order=True)
class LineageReferenceEdge:
    """One producer-output obligation retaining a local dependency lineage.

    The dependency owner installs ``token`` as an incoming LINEAGE reference.
    When the producer output is collected, its frozen metadata returns this edge
    to the composition layer, which may later release that exact token.  This
    pure value performs no RPC and does not mutate either owner table.
    """

    producer_object_id: ObjectID
    dependency_object_id: ObjectID
    token: str

    def __post_init__(self) -> None:
        if not isinstance(self.producer_object_id, ObjectID):
            raise TypeError("producer_object_id must be an ObjectID")
        if not isinstance(self.dependency_object_id, ObjectID):
            raise TypeError("dependency_object_id must be an ObjectID")
        if not isinstance(self.token, str) or not self.token:
            raise ValueError("lineage token must be a non-empty string")


@dataclass(frozen=True)
class ObjectMetadataCollection:
    """Atomic owner-table collection result and retained release work."""

    object_id: ObjectID
    collected: bool
    contained_releases: Tuple[ContainedReferenceEdge, ...] = ()
    lineage_releases: Tuple[LineageReferenceEdge, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise TypeError("collection object_id must be an ObjectID")
        if not isinstance(self.collected, bool):
            raise TypeError("collected must be a bool")
        releases = tuple(self.contained_releases)
        object.__setattr__(self, "contained_releases", releases)
        lineage_releases = tuple(self.lineage_releases)
        object.__setattr__(self, "lineage_releases", lineage_releases)
        if any(
            not isinstance(edge, ContainedReferenceEdge) for edge in releases
        ):
            raise TypeError(
                "contained_releases must contain ContainedReferenceEdge values"
            )
        if any(
            not isinstance(edge, LineageReferenceEdge)
            for edge in lineage_releases
        ):
            raise TypeError(
                "lineage_releases must contain LineageReferenceEdge values"
            )
        if not self.collected and (releases or lineage_releases):
            raise ValueError(
                "an uncollected object cannot emit reference releases"
            )
        if any(edge.container_object_id != self.object_id for edge in releases):
            raise ValueError(
                "every contained release must belong to the collected object"
            )
        if len(releases) != len(set(releases)):
            raise ValueError("contained releases must be unique")
        if any(
            edge.producer_object_id != self.object_id
            for edge in lineage_releases
        ):
            raise ValueError(
                "every lineage release must belong to the collected producer"
            )
        if len(lineage_releases) != len(set(lineage_releases)):
            raise ValueError("lineage releases must be unique")


@dataclass(frozen=True)
class ObjectMetadataCollectionPlan:
    """Immutable owner claim used by the runtime's two-phase collector.

    The plan is the teaching equivalent of Ray's object-directory/lineage GC
    barrier.  Once it exists, the logical owner has moved from ``ACTIVE`` to
    ``COLLECTING``: later location reports and attempt advances cannot change
    the replica epoch underneath already-issued drop messages.  The runtime
    persists this value before performing any RPC and must present the same
    ``collection_id`` when it commits ``COLLECTED``.

    ``canonical_*`` is populated only for object-store-backed results.  The
    producer specification is deliberately typed as ``object`` here because
    ``protocol`` imports this module to describe contained edges.
    """

    object_id: ObjectID
    collection_id: str
    producer_attempt_id: AttemptID | None
    locations: Tuple[NodeID, ...]
    producer_task_spec: object | None
    canonical_size_bytes: int | None = None
    canonical_checksum: str | None = None
    contained_releases: Tuple[ContainedReferenceEdge, ...] = ()
    lineage_releases: Tuple[LineageReferenceEdge, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise TypeError("collection object_id must be an ObjectID")
        if not isinstance(self.collection_id, str) or not self.collection_id:
            raise ValueError("collection_id must be a non-empty string")
        if self.producer_attempt_id is not None:
            if not isinstance(self.producer_attempt_id, AttemptID):
                raise TypeError(
                    "producer_attempt_id must be an AttemptID or None"
                )
            if self.producer_attempt_id.task_id != self.object_id.task_id:
                raise ValueError(
                    "collection attempt must belong to collection object"
                )
        locations = tuple(self.locations)
        object.__setattr__(self, "locations", locations)
        if any(not isinstance(location, NodeID) for location in locations):
            raise TypeError("collection locations must contain NodeID values")
        if len(locations) != len(set(locations)):
            raise ValueError("collection locations must be unique")
        if tuple(sorted(locations)) != locations:
            raise ValueError("collection locations must be canonical and sorted")
        size = self.canonical_size_bytes
        checksum = self.canonical_checksum
        if (size is None) != (checksum is None):
            raise ValueError(
                "stored collection size and checksum must appear together"
            )
        if size is not None and (
            isinstance(size, bool) or not isinstance(size, int) or size < 0
        ):
            raise ValueError(
                "canonical_size_bytes must be a non-negative integer"
            )
        if checksum is not None:
            if not isinstance(checksum, str) or len(checksum) != 64:
                raise ValueError(
                    "canonical_checksum must be a SHA-256 hex digest"
                )
            try:
                int(checksum, 16)
            except ValueError as exc:
                raise ValueError(
                    "canonical_checksum must be a SHA-256 hex digest"
                ) from exc
        ObjectMetadataCollection(
            self.object_id, collected=True,
            contained_releases=tuple(self.contained_releases),
            lineage_releases=tuple(self.lineage_releases),
        )
        object.__setattr__(self, "contained_releases", tuple(self.contained_releases))
        object.__setattr__(self, "lineage_releases", tuple(self.lineage_releases))


__all__ = [
    "ContainedReferenceHold",
    "ContainedReferenceEdge",
    "IncomingContainedReferenceHold",
    "LegacyContainedReferenceHold",
    "LineageReferenceEdge",
    "ObjectMetadataCollection",
    "ObjectMetadataCollectionPlan",
    "OwnerAddress",
]
