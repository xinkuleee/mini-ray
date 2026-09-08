"""A metadata-only, Ray-style locality hint for a new worker-lease first hop.

The Core supplies known, routable replica locations from its existing metadata.
This policy counts bytes, not resource availability: a busy or resource-infeasible
data node can still be the first node asked. Its NodeManager remains responsible
for Hybrid scheduling, spillback, dependency localization, and the final grant.

No placement or ownership authority lives here. The result neither reserves a
resource nor proves that a replica still exists. Missing locality leaves the
ordinary home-route fallback intact; PG routing and frozen lease replays bypass
this policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from .ids import NodeID, ObjectID


@dataclass(frozen=True)
class ObjectLocality:
    """Known byte size and replica nodes for one logical dependency.

    Callers must project one current object version before building this hint;
    attempt/owner validation belongs to their existing metadata authority.
    Locations are copied, deduplicated, and sorted without changing the input.
    """

    object_id: ObjectID
    size_bytes: int
    locations: tuple[NodeID, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, ObjectID):
            raise ValueError("object_id must be an ObjectID")
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes < 0
        ):
            raise ValueError("size_bytes must be a non-negative integer")
        try:
            locations = tuple(self.locations)
        except TypeError as exc:
            raise ValueError("locations must be an iterable of NodeIDs") from exc
        if any(not isinstance(node_id, NodeID) for node_id in locations):
            raise ValueError("locations must contain only NodeIDs")
        object.__setattr__(
            self, "locations", tuple(sorted(set(locations), key=lambda node: node.hex))
        )


def preferred_lease_node(
    objects: Iterable[ObjectLocality], *, fallback_node_id: NodeID
) -> Optional[NodeID]:
    """Prefer the node holding the most distinct dependency bytes, if any.

    Repeated records for the same object merge their known replica locations.
    Each object contributes its size at most once per node; contradictory sizes
    are rejected instead of silently changing the score. Positive ties prefer
    the fallback node, then the smallest NodeID, independent of input order.

    ``None`` means there is no positive locality evidence. The caller chooses
    its normal first hop; this function does not claim a task is infeasible.
    """

    if not isinstance(fallback_node_id, NodeID):
        raise ValueError("fallback_node_id must be a NodeID")
    by_object: dict[ObjectID, tuple[int, set[NodeID]]] = {}
    for item in objects:
        if not isinstance(item, ObjectLocality):
            raise ValueError("objects must contain ObjectLocality values")
        previous = by_object.get(item.object_id)
        if previous is None:
            by_object[item.object_id] = (item.size_bytes, set(item.locations))
        elif previous[0] != item.size_bytes:
            raise ValueError("one ObjectID has conflicting size_bytes")
        else:
            previous[1].update(item.locations)

    bytes_local: dict[NodeID, int] = {}
    for size_bytes, locations in by_object.values():
        for node_id in locations:
            bytes_local[node_id] = bytes_local.get(node_id, 0) + size_bytes
    best_bytes = max(bytes_local.values(), default=0)
    if best_bytes == 0:
        return None
    return min(
        (node_id for node_id, size in bytes_local.items() if size == best_bytes),
        key=lambda node_id: (node_id != fallback_node_id, node_id.hex),
    )
