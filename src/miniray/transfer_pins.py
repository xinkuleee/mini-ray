"""Target-side lifetime of a remote source pin.

Replica custody and source-read pins are different obligations. This small
outbox retains the immutable source request before sending Pin, protects the
active reader from background release, and retries only its exact close after
the reader leaves. The Node composes these reducers under its state lock; all
I/O and physical pin operations stay outside this module.
"""

from dataclasses import dataclass, replace

from . import protocol
from .ids import NodeID


@dataclass
class TransferPinObligation:
    source_address: tuple[str, int]
    pin: protocol.PinObjectForTransfer
    active: bool = True
    in_flight: bool = False
    rounds: int = 0
    retry_after: float = 0.0
    source_death: protocol.NodeDeathRecord | None = None

    @property
    def release(self):
        return protocol.ReleaseObjectPin(self.pin.transfer_id, self.pin.descriptor.object_id, self.pin.requester_node_id)


class TransferPinOutbox:
    def __init__(self):
        self._pending: dict[str, TransferPinObligation] = {}

    def begin(self, source_address, request) -> TransferPinObligation:
        if type(request) is not protocol.PinObjectForTransfer:
            raise TypeError("source pin outbox requires a PinObjectForTransfer")
        request = replace(request)
        source_address = protocol._validate_bound_address(source_address, "source pin address")
        previous = self._pending.get(request.transfer_id)
        if previous is not None:
            if previous.pin != request or previous.source_address != source_address or not previous.active:
                raise ValueError("source transfer identity was rebound or already closing")
            return previous
        record = TransferPinObligation(source_address, request)
        self._pending[request.transfer_id] = record
        return record

    def close(self, transfer_id):
        record = self._pending[transfer_id]
        record.active = False
        record.retry_after = 0.0

    def close_and_acquire(self, transfer_id, now):
        self.close(transfer_id)
        return self.acquire(now, transfer_id=transfer_id, force=True)

    def acquire(self, now, *, transfer_id=None, force=False):
        records = (self._pending.get(transfer_id),) if transfer_id is not None else tuple(self._pending.values())
        for record in records:
            if record is not None and not record.active and not record.in_flight and (force or record.retry_after <= now):
                record.in_flight = True
                return record
        return None

    def settle(self, record, *, acknowledged, now):
        if type(acknowledged) is not bool:
            raise TypeError("source pin acknowledgement must be a bool")
        if self._pending.get(record.pin.transfer_id) is not record or not record.in_flight:
            raise ValueError("source pin settlement does not own its in-flight ticket")
        record.in_flight = False
        if acknowledged or record.source_death is not None:
            self._pending.pop(record.pin.transfer_id)
            return True
        record.rounds += 1
        record.retry_after = now + min(0.25, 0.01 * 2 ** min(record.rounds, 5))
        return False

    def mark_source_dead(self, death):
        if type(death) is not protocol.NodeDeathRecord or type(death.node_id) is not NodeID:
            raise TypeError("source pin retirement requires an exact Node death record")
        death = replace(death, node_id=NodeID(bytes(death.node_id)))
        for record in tuple(self._pending.values()):
            if record.pin.descriptor.node_id == death.node_id:
                if record.source_death is not None and record.source_death != death:
                    raise ValueError("source Node death proof changed")
                record.source_death = death
                if not record.active and not record.in_flight:
                    self._pending.pop(record.pin.transfer_id)

    def pending(self):
        return tuple(self._pending.values())

    def has_pending(self):
        return bool(self._pending)
