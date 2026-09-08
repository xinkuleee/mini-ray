"""Pure target source-pin lifetime reducer contracts.

Only typed identities, two small request DTOs at most, and explicit numeric
clock values are used. There are no Nodes, stores, RPCs, threads, processes,
GCS, waits, imports of runtime fixtures, or inferred physical deletion. Death
records are authoritative reducer inputs, not fabricated Release replies.
"""

from copy import deepcopy
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.ids import AttemptID, NodeID, ObjectID, TaskID, WorkerID
from miniray.transfer_pins import TransferPinObligation, TransferPinOutbox


pytestmark = pytest.mark.unit


def _pin(transfer_id="source-reader", *, source_byte=b"s"):
    task = TaskID(b"t" * 16)
    descriptor = protocol.ObjectStoreDescriptor(
        ObjectID.for_task(task), WorkerID(b"o" * 16), AttemptID(task, 2),
        NodeID(source_byte * 16), 7, "a" * 64,
    )
    return protocol.PinObjectForTransfer(transfer_id, descriptor, NodeID(b"r" * 16))


def _death(node_id):
    return protocol.NodeDeathRecord(
        "source-exit", node_id, 4101, 1, 3, -9,
        protocol.NodeDeathReason.PROCESS_EXIT, "authoritative reducer input",
    )


def test_begin_detaches_request_endpoint_and_generated_release_identity():
    box = TransferPinOutbox()
    pin = _pin()
    expected = deepcopy(pin)
    address = ("source.invalid", 1234)
    record = box.begin(address, pin)
    assert type(record) is TransferPinObligation
    assert record.active and not record.in_flight and record.source_death is None
    assert box.pending() == (record,) and box.has_pending()
    assert record.source_address == ("source.invalid", 1234) and record.pin == expected
    address = ("changed.invalid", 4321)
    object.__setattr__(pin.descriptor.node_id, "value", b"x" * 16)
    object.__setattr__(pin.descriptor.producer_attempt_id, "attempt_number", 9)
    object.__setattr__(pin.requester_node_id, "value", b"y" * 16)
    assert record.source_address == ("source.invalid", 1234) and record.pin == expected
    release = record.release
    assert release == protocol.ReleaseObjectPin(expected.transfer_id, expected.descriptor.object_id, expected.requester_node_id)
    object.__setattr__(release.object_id, "return_index", 4)
    object.__setattr__(release.requester_node_id, "value", b"z" * 16)
    assert record.pin == expected
    assert record.release == protocol.ReleaseObjectPin(expected.transfer_id, expected.descriptor.object_id, expected.requester_node_id)


def test_active_reader_blocks_even_forced_acquisition_until_close():
    box = TransferPinOutbox()
    pin = _pin()
    record = box.begin(("source.invalid", 1234), pin)
    assert box.acquire(100.0) is None
    assert box.acquire(100.0, transfer_id=pin.transfer_id, force=True) is None
    assert record.active and not record.in_flight and box.has_pending()
    box.close(pin.transfer_id)
    assert not record.active and not record.in_flight
    claim = box.acquire(100.0, transfer_id=pin.transfer_id)
    assert claim is record and record.in_flight
    assert box.acquire(100.0, transfer_id=pin.transfer_id, force=True) is None
    assert box.settle(claim, acknowledged=True, now=100.0)
    assert not box.has_pending() and box.pending() == ()


def test_exact_begin_replays_but_endpoint_or_request_rebinding_and_reopen_fail():
    box = TransferPinOutbox()
    pin = _pin()
    address = ("source.invalid", 1234)
    record = box.begin(address, pin)
    for invalid in (["source.invalid", 1234], ("source.invalid", True), ("source.invalid", 0)):
        with pytest.raises(protocol.ProtocolError):
            box.begin(invalid, replace(pin, transfer_id="invalid-endpoint"))
        assert box.pending() == (record,)
    with pytest.raises(TypeError, match="PinObjectForTransfer"):
        box.begin(address, object())
    assert box.begin(tuple(address), deepcopy(pin)) is record
    with pytest.raises(ValueError, match="rebound"):
        box.begin(("different.invalid", 1234), pin)
    changed = replace(pin, descriptor=replace(pin.descriptor, checksum="b" * 64))
    with pytest.raises(ValueError, match="rebound"):
        box.begin(address, changed)
    assert box.pending() == (record,) and record.pin == pin and record.source_address == address
    box.close(pin.transfer_id)
    with pytest.raises(ValueError, match="closing"):
        box.begin(address, pin)
    assert box.pending() == (record,) and not record.active


def test_only_current_claim_can_settle_and_settlement_is_not_reentrant():
    box = TransferPinOutbox()
    record = box.begin(("source.invalid", 1234), _pin())
    box.close(record.pin.transfer_id)
    with pytest.raises(ValueError, match="in-flight ticket"):
        box.settle(record, acknowledged=True, now=0.0)
    claim = box.acquire(0.0, force=True)
    assert claim is record
    copied = replace(record)
    assert copied == record and copied is not record
    with pytest.raises(ValueError, match="in-flight ticket"):
        box.settle(copied, acknowledged=True, now=0.0)
    for invalid_ack in (1, "ack", None):
        with pytest.raises(TypeError, match="bool"):
            box.settle(record, acknowledged=invalid_ack, now=0.0)
        assert box.pending() == (record,) and record.in_flight
    assert box.pending() == (record,) and record.in_flight
    assert box.settle(record, acknowledged=True, now=0.0)
    with pytest.raises(ValueError, match="in-flight ticket"):
        box.settle(record, acknowledged=True, now=0.0)
    assert box.pending() == ()


def test_unacknowledged_close_keeps_exact_request_and_respects_backoff_or_force():
    box = TransferPinOutbox()
    record = box.begin(("source.invalid", 1234), _pin())
    expected_release = record.release
    box.close(record.pin.transfer_id)
    assert box.acquire(10.0) is record
    assert not box.settle(record, acknowledged=False, now=10.0)
    assert box.has_pending() and not record.in_flight and not record.active
    assert record.rounds == 1 and 10.0 < record.retry_after <= 10.25
    assert record.release == expected_release and record.source_death is None
    assert box.acquire(10.0) is None
    assert box.acquire(10.0, force=True) is record
    assert not box.settle(record, acknowledged=False, now=10.0)
    assert record.rounds == 2 and record.release == expected_release
    assert box.acquire(record.retry_after) is record
    assert box.settle(record, acknowledged=True, now=record.retry_after)
    assert not box.has_pending()


def test_source_death_does_not_discharge_an_active_reader_before_close_and_settlement():
    box = TransferPinOutbox()
    pin = _pin()
    record = box.begin(("source.invalid", 1234), pin)
    proof = _death(pin.descriptor.node_id)
    expected = deepcopy(proof)
    with pytest.raises(TypeError, match="Node death"):
        box.mark_source_dead(object())
    assert record.source_death is None and box.pending() == (record,)
    box.mark_source_dead(proof)
    assert record.source_death == proof and record.active and not record.in_flight
    object.__setattr__(proof.node_id, "value", b"x" * 16)
    object.__setattr__(proof, "detail", "changed caller delivery")
    assert record.source_death == expected and record.source_death is not proof
    box.mark_source_dead(deepcopy(expected))
    with pytest.raises(ValueError, match="proof changed"):
        box.mark_source_dead(replace(expected, detection_id="different-source-exit"))
    assert record.source_death == expected and box.pending() == (record,)
    assert box.pending() == (record,) and box.acquire(0.0, force=True) is None
    box.close(pin.transfer_id)
    assert box.pending() == (record,) and not record.active
    assert box.acquire(0.0, force=True) is record
    # Node death, not a fabricated Release ACK, proves the source pin gone.
    assert box.settle(record, acknowledged=False, now=0.0)
    assert not box.has_pending() and record.source_death == expected


def test_source_death_preserves_inflight_ticket_and_leaves_other_source_unresolved():
    box = TransferPinOutbox()
    first, second = _pin("dead-source-reader"), _pin("live-source-reader", source_byte=b"u")
    dead_record = box.begin(("dead-source.invalid", 1234), first)
    live_record = box.begin(("live-source.invalid", 1235), second)
    box.close(first.transfer_id)
    box.close(second.transfer_id)
    assert box.acquire(0.0, transfer_id=first.transfer_id) is dead_record
    proof = _death(first.descriptor.node_id)
    box.mark_source_dead(proof)
    assert box.pending() == (dead_record, live_record)
    assert dead_record.in_flight and dead_record.source_death == proof
    assert live_record.source_death is None
    assert box.settle(dead_record, acknowledged=False, now=0.0)
    assert box.pending() == (live_record,)
    assert box.acquire(0.0, transfer_id=second.transfer_id) is live_record
    assert not box.settle(live_record, acknowledged=False, now=0.0)
    assert box.pending() == (live_record,) and live_record.source_death is None


def test_two_sessions_for_one_object_keep_independent_readers_and_close_progress():
    box = TransferPinOutbox()
    first = _pin("first-reader")
    second = replace(first, transfer_id="second-reader")
    first_record = box.begin(("source.invalid", 1234), first)
    second_record = box.begin(("source.invalid", 1234), second)
    assert first.descriptor == second.descriptor and first_record is not second_record
    box.close(first.transfer_id)
    assert box.acquire(0.0) is first_record
    assert box.settle(first_record, acknowledged=True, now=0.0)
    assert box.pending() == (second_record,) and second_record.active and not second_record.in_flight
    assert box.acquire(0.0, force=True) is None
    box.close(second.transfer_id)
    assert box.acquire(0.0) is second_record
    assert second_record.release != first_record.release
    assert box.settle(second_record, acknowledged=True, now=0.0)
    assert not box.has_pending()
