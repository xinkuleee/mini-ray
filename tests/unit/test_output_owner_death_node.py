"""Pure Node finalization: one tiny stored output, one fake Worker, no live I/O.

The real journal, store, lease ledger and Node handlers are composed in memory.
Each fault/replay sequence is explicit and bounded; the imported autouse
tripwire forbids threads, sockets and waiting.  No Node constructor is used.
"""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from miniray import output_protocol as wire, protocol
from miniray.errors import ProtocolError
from miniray.output_publication_journal import OutputPublicationJournalState
from miniray.resources import ResourceVector
from tests.unit.test_output_publication_node_server import _node, _no_runtime


pytestmark = pytest.mark.unit


def _fixture(monkeypatch, *, phase="running"):
    fixture, node, record, complete = _node(refs=False)
    prepare = wire.PrepareOutputPublication(fixture.manifest, (fixture.values.payload))
    if phase == "partial":
        write = fixture.store.write

        def lose_write(object_id, payload):
            write(object_id, payload[:2])
            raise RuntimeError("after partial write")

        monkeypatch.setattr(fixture.store, "write", lose_write)
        with pytest.raises(RuntimeError, match="partial write"):
            node._handle_prepare_output_publication(prepare)
        monkeypatch.setattr(fixture.store, "write", write)
    else:
        assert node._handle_prepare_output_publication(prepare).accepted
        if phase == "complete":
            assert node._handle_complete_worker_lease_inner(complete).accepted
    incarnation = fixture.manifest.header.node_incarnation
    death = protocol.WorkerDeathRecord(
        "output-owner-exit",
        protocol.WorkerIncarnation(incarnation.node_id, incarnation.node_pid,
                                   incarnation.registration_epoch, fixture.values.owner, 1901),
        1, 5, protocol.WorkerDeathReason.PROCESS_EXIT,
    )
    request = wire.FinalizeOutputOwnerDeath(fixture.manifest, death)
    # Use the real installed owner-wide fence.  GCS drives its observed sealed
    # replica cleanup before it is allowed to send Finalize to this Node.
    fence = node._handle_install_owner_death_fence(protocol.InstallOwnerDeathFence(
        "owner-sweep", death, node.node_id,
    ))
    assert fence.accepted
    slot = fixture.manifest.value
    if fixture.id.object_id in node._sealed_metadata:
        drop = node._handle_drop_object_replica(protocol.DropObjectReplica(fixture.id.object_id, fixture.id.attempt_id, fixture.values.owner, node.node_id, slot.checksum))
        assert drop.status is protocol.DropObjectReplicaStatus.DROPPED
    return fixture, node, record, complete, request


@pytest.mark.parametrize("phase", ("running", "partial", "complete"))
def test_owner_finalize_retires_exact_node_and_worker_custody_once(monkeypatch, phase):
    fixture, node, record, complete, request = _fixture(monkeypatch, phase=phase)
    calls = []

    def rpc(address, handler, message):
        assert not node._state_lock._is_owned()
        assert not fixture.journal._lock._is_owned()
        assert not fixture.adapter._lock._is_owned()
        assert fixture.id in fixture.adapter._tickets
        assert address == record.grant.worker_address
        assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER and message == request
        calls.append(message)
        return wire.FinalizeOutputOwnerDeathReply(message, True)

    node._background_rpc = rpc
    first = node._handle_finalize_output_owner_death(request)
    assert first.cleaned
    assert node._handle_finalize_output_owner_death(request) == first
    assert calls == [request]
    snapshot = fixture.journal.snapshot(fixture.id)
    assert snapshot.state is OutputPublicationJournalState.RETIRED
    assert snapshot.retained_result_slots == ()
    assert (snapshot.complete is not None) == (phase == "complete")
    assert snapshot.rollback_tombstone is None
    assert record.state is (protocol.LeaseExecutionState.COMPLETED if phase == "complete"
                            else protocol.LeaseExecutionState.ABANDONED)
    assert fixture.ledger.available == ResourceVector({"CPU": 1})
    assert fixture.store.used_bytes == 0 and not node._local_replica_write_claims
    assert fixture.adapter.owner_death_finished(fixture.id)
    assert fixture.adapter.pending_terminal_reports() == ()
    assert node._drive_output_publications()
    assert not node._handle_prepare_output_publication(wire.PrepareOutputPublication(
        fixture.manifest, (fixture.values.payload),
    )).accepted
    with pytest.raises(ValueError, match="death-fenced"):
        node._handle_complete_worker_lease_inner(complete)


def test_worker_cleanup_ack_loss_retains_node_payload_and_replays_exact_request(monkeypatch):
    fixture, node, record, _complete, request = _fixture(monkeypatch, phase="complete")
    calls = []

    def rpc(_address, _handler, message):
        calls.append(message)
        if len(calls) == 1:
            raise TimeoutError("Worker cleaned; ACK lost")
        return wire.FinalizeOutputOwnerDeathReply(message, True)

    node._background_rpc = rpc
    with pytest.raises(TimeoutError, match="ACK lost"):
        node._handle_finalize_output_owner_death(request)
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == (0,)
    assert not fixture.adapter.owner_death_finished(fixture.id)
    assert not fixture.adapter._tickets
    assert node._handle_finalize_output_owner_death(request).cleaned
    assert calls == [request, request]
    assert fixture.journal.snapshot(fixture.id).retained_result_slots == ()


def test_inflight_publication_makes_finalize_nonblocking_without_mutation(monkeypatch):
    fixture, node, record, _complete, request = _fixture(monkeypatch, phase="partial")
    node._background_rpc = lambda *_: pytest.fail("busy publication reached Worker")
    before = fixture.journal.snapshot(fixture.id)
    used = fixture.store.used_bytes
    with fixture.adapter._ticket(fixture.id):
        assert not node._handle_finalize_output_owner_death(request).cleaned
    assert record.state is protocol.LeaseExecutionState.RUNNING
    assert fixture.ledger.available == ResourceVector()
    assert fixture.store.used_bytes == used
    assert fixture.journal.snapshot(fixture.id) == before


@pytest.mark.parametrize("corruption", ("cleaned", "manifest"))
def test_worker_finalize_reply_is_revalidated_before_node_retirement(monkeypatch, corruption):
    fixture, node, _record, _complete, request = _fixture(monkeypatch)

    def rpc(_address, _handler, message):
        reply = wire.FinalizeOutputOwnerDeathReply(message, True)
        if corruption == "cleaned":
            object.__setattr__(reply, "cleaned", 1)
        else:
            object.__setattr__(reply, "request", replace(message, owner_death=replace(
                message.owner_death, detection_id="another-owner-exit",
            )))
        return reply

    node._background_rpc = rpc
    if corruption == "cleaned":
        with pytest.raises(ProtocolError):
            node._handle_finalize_output_owner_death(request)
    else:
        assert not node._handle_finalize_output_owner_death(request).cleaned
    assert fixture.journal.snapshot(fixture.id).retained_result_slots
    assert not fixture.adapter.owner_death_finished(fixture.id)
    node._background_rpc = lambda _a, _h, message: wire.FinalizeOutputOwnerDeathReply(message, True)
    assert node._handle_finalize_output_owner_death(request).cleaned


@pytest.mark.parametrize("observer", ("missing", "unavailable"))
def test_worker_unavailability_is_not_a_confirmed_custody_cleanup(monkeypatch, observer):
    fixture, node, _record, _complete, request = _fixture(monkeypatch)
    worker = node._workers[fixture.values.executor]
    if observer == "missing":
        worker.process = None
    else:
        def unavailable():
            raise ValueError("process observer unavailable")
        worker.process = SimpleNamespace(is_alive=unavailable)
    node._background_rpc = lambda *_: pytest.fail("unavailable Worker reached RPC")
    assert not node._handle_finalize_output_owner_death(request).cleaned
    assert not fixture.adapter.owner_death_finished(fixture.id)
    node._dead_worker_exitcodes = {fixture.values.executor: 5}
    worker.process = None
    assert node._handle_finalize_output_owner_death(request).cleaned


def test_wrong_partial_write_claim_cannot_authorize_deletion(monkeypatch):
    fixture, node, _record, _complete, request = _fixture(monkeypatch, phase="partial")
    object_id = (fixture.manifest.publication_id).object_id
    claim = node._local_replica_write_claims[object_id]
    wrong_effect = replace(claim.effect)
    object.__setattr__(wrong_effect, "slot_index", 1)
    node._local_replica_write_claims[object_id] = replace(claim, effect=wrong_effect)
    node._background_rpc = lambda *_: pytest.fail("wrong claim reached Worker")
    assert not node._handle_finalize_output_owner_death(request).cleaned
    assert fixture.store.contains(object_id, sealed_only=False)
    assert not fixture.adapter.owner_death_finished(fixture.id)
    node._local_replica_write_claims[object_id] = claim
    node._background_rpc = lambda _a, _h, message: wire.FinalizeOutputOwnerDeathReply(message, True)
    assert node._handle_finalize_output_owner_death(request).cleaned
