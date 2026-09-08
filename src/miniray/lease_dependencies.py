"""Node custody for dependencies localized before a Worker lease exists.

The registry records witnessed replicas, not newly-created replicas and not
execution permission. A failed consumer cannot delete a shared object merely
because it pulled it. Its owner takes custody through the same handoff used
after a successful grant, then acknowledges this exact inventory.

Callers compose registry operations with the Node state/object/lease locks.
There is no networking, timer, object-store access or second cleanup backend
here; this module owns only the small request-scoped state transition.
"""

from dataclasses import dataclass, field, replace

from . import protocol
from .ids import LeaseID, NodeID, ObjectID


class DependencyCustodyConflict(ValueError):
    """An immutable request or witnessed-replica identity changed."""


@dataclass
class _Entry:
    request: protocol.RequestWorkerLease
    descriptors: dict[ObjectID, protocol.ObjectStoreDescriptor]
    acknowledged: protocol.LeaseDependencyInventory | None = None
    pending: dict[ObjectID, protocol.ObjectStoreDescriptor] | None = None
    submitter_death: protocol.WorkerDeathRecord | None = None
    owner_receipts: dict[ObjectID, object] = field(default_factory=dict)
    abandoned_complete: bool = False
    handoff_cursor: int = 0


class LeaseDependencyCustody:
    """One Node's monotonically accumulated dependency inventories."""

    def __init__(self, node_id: NodeID):
        if type(node_id) is not NodeID:
            raise TypeError("dependency custody registry requires a NodeID")
        self.node_id = NodeID(bytes(node_id))
        self._entries: dict[LeaseID, _Entry] = {}

    def bind(self, request: protocol.RequestWorkerLease) -> None:
        request = protocol.revalidate_worker_lease_request(request)
        previous = self._entries.get(request.lease_id)
        if previous is not None:
            if previous.request != request:
                raise DependencyCustodyConflict("lease dependency request identity changed")
            return
        self._entries[request.lease_id] = _Entry(request, {}, pending={})

    def begin(self, request: protocol.RequestWorkerLease, descriptor: protocol.ObjectStoreDescriptor) -> None:
        """Retain identity before an operation may expose physical bytes."""
        self.bind(request)
        entry = self._entries[request.lease_id]
        checked = protocol.LeaseDependencyInventory(entry.request, self.node_id, (descriptor,)).descriptors[0]
        if entry.acknowledged is not None or entry.abandoned_complete:
            if entry.descriptors.get(checked.object_id) != checked:
                raise DependencyCustodyConflict("acknowledged inventory cannot begin a new replica")
            return
        previous = entry.pending.setdefault(checked.object_id, checked)
        if previous != checked:
            raise DependencyCustodyConflict("localization candidate identity changed")

    def discard_unsealed(self, request: protocol.RequestWorkerLease, descriptor: protocol.ObjectStoreDescriptor) -> None:
        """Caller observed that this exact operation left no sealed replica."""
        entry = self._entries[request.lease_id]
        if entry.request != request or entry.pending.get(descriptor.object_id) != descriptor:
            raise DependencyCustodyConflict("unsealed candidate identity changed")
        entry.pending.pop(descriptor.object_id)

    def request(self, lease_id: LeaseID) -> protocol.RequestWorkerLease | None:
        entry = self._entries.get(lease_id)
        return None if entry is None else protocol.revalidate_worker_lease_request(entry.request)

    def candidates(self, lease_id: LeaseID) -> tuple[protocol.ObjectStoreDescriptor, ...]:
        entry = self._entries.get(lease_id)
        if entry is None:
            return ()
        return protocol.LeaseDependencyInventory(
            entry.request, self.node_id, tuple(entry.pending[item.object_id]
                for item in entry.request.dependencies if item.object_id in entry.pending),
        ).descriptors

    def record(self, request: protocol.RequestWorkerLease, descriptor: protocol.ObjectStoreDescriptor) -> None:
        self.bind(request)
        entry = self._entries[request.lease_id]
        # Even a one-object addition is checked against the complete request.
        checked = protocol.LeaseDependencyInventory(entry.request, self.node_id, (descriptor,))
        descriptor = checked.descriptors[0]
        previous = entry.descriptors.get(descriptor.object_id)
        if previous is not None:
            if previous != descriptor:
                raise DependencyCustodyConflict("witnessed replica changed within one lease")
            entry.pending.pop(descriptor.object_id, None)
            return
        if entry.acknowledged is not None or entry.abandoned_complete:
            raise DependencyCustodyConflict("acknowledged inventory cannot acquire new replicas")
        entry.descriptors[descriptor.object_id] = descriptor
        entry.pending.pop(descriptor.object_id, None)

    def snapshot(self, lease_id: LeaseID) -> protocol.LeaseDependencyInventory | None:
        entry = self._entries.get(lease_id)
        if entry is None:
            return None
        if entry.pending:
            raise DependencyCustodyConflict("localization effects still need physical reconciliation")
        ordered = tuple(entry.descriptors[item.object_id] for item in entry.request.dependencies
                        if item.object_id in entry.descriptors)
        return protocol.LeaseDependencyInventory(entry.request, self.node_id, ordered)

    def acknowledge(self, inventory: protocol.LeaseDependencyInventory) -> bool:
        inventory = protocol.LeaseDependencyInventory(
            inventory.lease_request, inventory.node_id, inventory.descriptors,
        )
        actual = self.snapshot(inventory.lease_request.lease_id)
        if actual is None or actual != inventory:
            raise DependencyCustodyConflict("custody acknowledgement is not the exact Node inventory")
        entry = self._entries[inventory.lease_request.lease_id]
        if entry.acknowledged is not None:
            if entry.acknowledged != inventory:
                raise DependencyCustodyConflict("custody acknowledgement changed")
            return False
        entry.acknowledged = inventory
        return True

    def has_pending(self) -> bool:
        # Empty requests have no replica custody to lose. A recorded replica
        # stays pending across PENDING_CAPACITY, rejection and Cancel ACK loss.
        return any(entry.pending or entry.descriptors and entry.acknowledged is None and not entry.abandoned_complete
                   for entry in self._entries.values())

    def abandoned_candidates(self):
        return tuple(protocol.revalidate_worker_lease_request(entry.request)
            for entry in self._entries.values()
            if (entry.pending or entry.descriptors) and entry.acknowledged is None and not entry.abandoned_complete)

    def abandon(self, lease_id, death):
        death = protocol._revalidate_worker_death(death)
        entry = self._entries[lease_id]
        if entry.request.requester_worker_id != death.worker_id:
            raise DependencyCustodyConflict("death does not identify the dependency submitter")
        if death.reason not in (protocol.WorkerDeathReason.PROCESS_EXIT, protocol.WorkerDeathReason.NODE_EXIT):
            raise DependencyCustodyConflict("abandoned inventory requires unexpected submitter death")
        if entry.submitter_death is not None and entry.submitter_death != death:
            raise DependencyCustodyConflict("dependency submitter death changed")
        entry.submitter_death = death

    def receipt(self, lease_id, object_id):
        return self._entries[lease_id].owner_receipts.get(object_id)

    def next_abandoned_descriptor(self, inventory):
        entry = self._entries[inventory.lease_request.lease_id]
        if self.snapshot(entry.request.lease_id) != inventory:
            raise DependencyCustodyConflict("abandoned handoff changed its inventory")
        descriptors = inventory.descriptors
        for offset in range(len(descriptors)):
            index = (entry.handoff_cursor + offset) % len(descriptors)
            descriptor = descriptors[index]
            if descriptor.object_id not in entry.owner_receipts:
                entry.handoff_cursor = (index + 1) % len(descriptors)
                return descriptor
        return None

    def record_abandoned_receipt(self, inventory, descriptor, receipt):
        actual = self.snapshot(inventory.lease_request.lease_id)
        if actual != inventory or descriptor not in actual.descriptors:
            raise DependencyCustodyConflict("abandoned receipt changed the frozen inventory")
        entry = self._entries[inventory.lease_request.lease_id]
        if entry.submitter_death is None:
            raise DependencyCustodyConflict("abandoned receipt requires submitter death")
        if type(receipt) is protocol.ReportAbandonedDependencyReplicaReply:
            receipt = replace(receipt)
            if (receipt.request.inventory != inventory or receipt.request.descriptor != descriptor
                    or receipt.request.submitter_death != entry.submitter_death or not receipt.custody_transferred):
                raise DependencyCustodyConflict("owner did not accept abandoned replica custody")
        elif type(receipt) is protocol.InstallOwnerDeathFenceReply:
            receipt = replace(receipt)
            if (not receipt.complete or receipt.request.owner_worker_id != descriptor.owner_worker_id
                    or receipt.request.node_id != descriptor.node_id
                    or receipt.request.scope is not protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP):
                raise DependencyCustodyConflict("dead owner cleanup did not prove replica collection")
        else:
            raise DependencyCustodyConflict("unknown abandoned custody receipt")
        previous = entry.owner_receipts.setdefault(descriptor.object_id, receipt)
        if previous != receipt:
            # A lost first ACK may yield a later ALREADY reply for identical
            # work. Once custody is recorded its original receipt remains truth.
            return False
        return True

    def complete_abandoned(self, inventory):
        actual = self.snapshot(inventory.lease_request.lease_id)
        if actual != inventory:
            raise DependencyCustodyConflict("abandoned completion changed inventory")
        entry = self._entries[inventory.lease_request.lease_id]
        if entry.submitter_death is None or any(item.object_id not in entry.owner_receipts for item in inventory.descriptors):
            raise DependencyCustodyConflict("abandoned completion lacks owner custody")
        entry.abandoned_complete = True
