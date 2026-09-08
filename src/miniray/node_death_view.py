"""The existing cluster-install barrier, retained for Worker-side owners.

The Driver observes managed process exits and obtains the GCS records. Only
after every survivor has installed the same snapshot does it publish this
byte-free certificate. A Node retains one cumulative view; embedded Cores read
it from their local Node using their existing coordinator, including after
lazy construction. This is not another failure detector or membership service.
"""

from __future__ import annotations

from dataclasses import dataclass, fields, replace
from typing import Optional, Tuple

from . import protocol
from .ids import NodeID
from .resources import ResourceVector


PUBLISH_NODE_DEATH_VIEW = "publish_installed_node_deaths"
GET_NODE_DEATH_VIEW = "get_installed_node_deaths"


def _node_id(value):
    if type(value) is not NodeID or type(value.value) is not bytes:
        raise TypeError("node-death view requires an exact immutable NodeID")
    return NodeID(bytes(value))


def _snapshot(value):
    if type(value) is not protocol.InstallClusterSnapshot:
        raise TypeError("node-death view requires an installed cluster snapshot")
    nodes = []
    for node in value.nodes:
        if type(node) is not protocol.NodeInfo:
            raise TypeError("node-death view requires exact live NodeInfo values")
        if type(node.total_resources) is not ResourceVector or type(node.available_resources) is not ResourceVector:
            raise TypeError("certified Node resources must be exact ResourceVectors")
        nodes.append(replace(node, node_id=_node_id(node.node_id),
            total_resources=ResourceVector(node.total_resources.to_dict()),
            available_resources=ResourceVector(node.available_resources.to_dict())))
    snapshot = protocol.InstallClusterSnapshot(value.membership_epoch, value.snapshot_id, tuple(nodes))
    if tuple(node.node_id for node in nodes) != tuple(sorted(node.node_id for node in nodes)):
        raise ValueError("survivor snapshot must have canonical Node order")
    return snapshot


class _WireValue:
    def __reduce__(self):
        # Re-enter deep validation on pickle reconstruction, not just on send.
        return type(self), tuple(getattr(self, field.name) for field in fields(self))


@dataclass(frozen=True)
class InstalledNodeDeathView(_WireValue):
    snapshot: protocol.InstallClusterSnapshot
    survivor_acks: Tuple[protocol.InstallClusterSnapshotReply, ...]
    deaths: Tuple[protocol.NodeDeathRecord, ...]

    def __post_init__(self):
        snapshot = _snapshot(self.snapshot)
        acks = []
        for ack in self.survivor_acks:
            if type(ack) is not protocol.InstallClusterSnapshotReply:
                raise TypeError("survivor barrier requires exact snapshot ACKs")
            ack = replace(ack, node_id=_node_id(ack.node_id))
            if (ack.installed is not True or ack.error is not None
                    or ack.membership_epoch != snapshot.membership_epoch
                    or ack.snapshot_id != snapshot.snapshot_id):
                raise ValueError("survivor ACK does not prove this installed snapshot")
            acks.append(ack)
        if tuple(ack.node_id for ack in acks) != tuple(node.node_id for node in snapshot.nodes):
            raise ValueError("barrier must contain one ordered ACK for every survivor")
        deaths = []
        for death in self.deaths:
            if type(death) is not protocol.NodeDeathRecord:
                raise TypeError("node-death view cannot use Worker or inferred death")
            death = replace(death, node_id=_node_id(death.node_id))
            if death.death_epoch > snapshot.membership_epoch:
                raise ValueError("snapshot predates a committed Node death")
            deaths.append(death)
        if not deaths:
            raise ValueError("a certified death view must contain a death fact")
        if tuple(death.death_epoch for death in deaths) != tuple(sorted({death.death_epoch for death in deaths})):
            raise ValueError("Node deaths must be unique and ordered by death epoch")
        if len({death.node_id for death in deaths}) != len(deaths):
            raise ValueError("NodeID cannot have two death records")
        if len({death.detection_id for death in deaths}) != len(deaths):
            raise ValueError("Node death detection ID cannot be rebound")
        if {node.node_id for node in snapshot.nodes} & {death.node_id for death in deaths}:
            raise ValueError("certified survivor view resurrects a dead Node")
        object.__setattr__(self, "snapshot", snapshot)
        object.__setattr__(self, "survivor_acks", tuple(acks))
        object.__setattr__(self, "deaths", tuple(deaths))

    def validate_successor(self, previous: Optional[InstalledNodeDeathView]):
        if previous is None:
            return
        previous = replace(previous)
        old, new = previous.snapshot, self.snapshot
        if new.membership_epoch < old.membership_epoch:
            raise ValueError("certified death view regressed its membership epoch")
        if new.membership_epoch == old.membership_epoch and new != old:
            raise ValueError("one membership epoch cannot name different snapshots")
        by_node = {death.node_id: death for death in self.deaths}
        if any(by_node.get(death.node_id) != death for death in previous.deaths):
            raise ValueError("certified view lost or rebound an earlier death fact")
        old_nodes = {node.node_id: node for node in old.nodes}
        for node in new.nodes:
            before = old_nodes.get(node.node_id)
            if before is None or (before.node_pid, before.registration_epoch, before.address, before.total_resources) != (
                    node.node_pid, node.registration_epoch, node.address, node.total_resources):
                raise ValueError("death-only update changed a surviving Node incarnation")
        removed = set(old_nodes) - {node.node_id for node in new.nodes}
        for node_id in removed:
            death = by_node.get(node_id)
            node = old_nodes[node_id]
            if death is None or (death.node_pid, death.registration_epoch) != (node.node_pid, node.registration_epoch):
                raise ValueError("removed Node lacks its exact committed death fact")
            if death.death_epoch <= old.membership_epoch:
                raise ValueError("newly removed Node death predates its certified live view")


@dataclass(frozen=True)
class PublishInstalledNodeDeaths(_WireValue):
    node_id: NodeID
    view: InstalledNodeDeathView

    def __post_init__(self):
        node_id = _node_id(self.node_id)
        if type(self.view) is not InstalledNodeDeathView:
            raise TypeError("published death view requires its full certificate")
        view = replace(self.view)
        if node_id not in tuple(node.node_id for node in view.snapshot.nodes):
            raise ValueError("death view publication must target a survivor")
        object.__setattr__(self, "node_id", node_id)
        object.__setattr__(self, "view", view)


@dataclass(frozen=True)
class PublishInstalledNodeDeathsReply(_WireValue):
    request: PublishInstalledNodeDeaths
    accepted: bool
    error: Optional[str] = None

    def __post_init__(self):
        if type(self.request) is not PublishInstalledNodeDeaths or type(self.accepted) is not bool:
            raise TypeError("death view ACK requires the exact request and a bool")
        if (self.accepted and self.error is not None
                or not self.accepted and (type(self.error) is not str or not self.error)):
            raise ValueError("death view ACK has inconsistent acceptance/error")
        object.__setattr__(self, "request", replace(self.request))


@dataclass(frozen=True)
class GetInstalledNodeDeaths(_WireValue):
    node_id: NodeID

    def __post_init__(self):
        object.__setattr__(self, "node_id", _node_id(self.node_id))


@dataclass(frozen=True)
class GetInstalledNodeDeathsReply(_WireValue):
    request: GetInstalledNodeDeaths
    view: Optional[InstalledNodeDeathView] = None

    def __post_init__(self):
        if type(self.request) is not GetInstalledNodeDeaths:
            raise TypeError("death view query reply must echo its exact request")
        request = replace(self.request)
        if self.view is not None:
            if type(self.view) is not InstalledNodeDeathView:
                raise TypeError("death view query reply contains an invalid certificate")
            view = replace(self.view)
            if request.node_id not in tuple(node.node_id for node in view.snapshot.nodes):
                raise ValueError("death view query did not come from a survivor")
            object.__setattr__(self, "view", view)
        object.__setattr__(self, "request", request)
