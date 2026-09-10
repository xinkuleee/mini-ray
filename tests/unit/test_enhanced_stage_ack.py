"""Finite StageAck boundary negatives on actual authority replies; no runtime I/O."""
from dataclasses import replace
import pickle

import pytest

from miniray import enhanced_publication as ep
from miniray.enhanced_publication_client import PublicationClient
from miniray.errors import SystemTaskError
from miniray.ids import WorkerID
from tests.unit.test_enhanced_publication import _task, _prepared, _begin, _commit, _id, OWNER
from tests.unit.test_output_publication_node_server import _no_runtime

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("change", ("request", "stage", "fact", "owner", "closing"))
def test_actual_stage_ack_is_revalidated_at_known_owner_consumer(change):
    authority, publication = ep.PublicationAuthority(), _task()
    client = PublicationClient(lambda handler, request: authority.apply(request))
    client.remember(publication)
    _begin(authority, publication)
    request = ep.ArmTask(_prepared(publication))
    ack = authority.apply(request)
    assert type(ack) is ep.PublicationStageAck and pickle.loads(pickle.dumps(ack)) == ack
    before = authority.query(ep.GetPublication(publication.reference)).snapshot
    bad = replace(ack)
    if change == "request":
        object.__setattr__(bad, "request", ep.PrepareGraph(publication.reference))
    elif change == "stage":
        object.__setattr__(bad, "receipt", before.receipt(ep.PublicationStage.PREPARED))
    elif change == "fact":
        object.__setattr__(bad.accepted_fact.materialization.node_incarnation, "registration_epoch", 0)
    elif change == "owner":
        object.__setattr__(bad, "owner_worker_id", _id(WorkerID, 99))
    else:
        object.__setattr__(bad, "retired_receipt", ep.PublicationReceipt(
            publication.reference, ep.PublicationStage.RETIRED, ack.receipt.sequence + 1))
    client._rpc = lambda handler, sent: bad
    with pytest.raises((TypeError, ValueError, SystemTaskError)):
        client.call(request, ep.PublicationStage.ARMED)
    assert authority.query(ep.GetPublication(publication.reference)).snapshot == before


def test_stage_ack_nested_reply_cannot_mutate_authority_history():
    authority, publication = ep.PublicationAuthority(), _task(children=())
    _begin(authority, publication)
    request = ep.ArmTask(_prepared(publication))
    ack = authority.apply(request)
    before = authority.query(ep.GetPublication(publication.reference)).snapshot
    assert ack.accepted_fact is not before.prepared
    object.__setattr__(ack.accepted_fact.materialization.node_incarnation, "registration_epoch", 0)
    assert authority.query(ep.GetPublication(publication.reference)).snapshot == before
    with pytest.raises((TypeError, ValueError)):
        replace(ack)


def test_stage_ack_construction_failure_precedes_authority_commit(monkeypatch):
    authority, publication = ep.PublicationAuthority(), _task()
    authority.begin(ep.BeginPublication(publication))
    before = authority.query(ep.GetPublication(publication.reference)).snapshot
    sequence = authority._sequence
    original = ep.stage_ack_from_snapshot

    def fail(request, snapshot, receipt, **kwargs):
        if type(request) is ep.PrepareGraph:
            raise ValueError("injected ACK construction failure")
        return original(request, snapshot, receipt, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(ep, "stage_ack_from_snapshot", fail)
        with pytest.raises(ValueError, match="ACK construction"):
            authority.prepare(ep.PrepareGraph(publication.reference))
    assert authority._sequence == sequence
    assert authority.query(ep.GetPublication(publication.reference)).snapshot == before
    ack = authority.prepare(ep.PrepareGraph(publication.reference))
    assert ack.receipt.sequence == sequence + 1


def test_reference_only_commit_ack_checks_known_owner_and_preserves_closed_history():
    authority, publication = ep.PublicationAuthority(), _task()
    committed = _commit(authority, publication)
    request = ep.CommitGraph(publication.reference)
    bad = replace(committed, owner_worker_id=_id(WorkerID, 99))
    client = PublicationClient(lambda handler, sent: bad)
    client.remember(publication)
    with pytest.raises(SystemTaskError, match="identity"):
        client.call(request, ep.PublicationStage.COMMITTED)
    fence = ep.OwnerRetirementReceipt(publication.reference, OWNER, "ack-gc", ep.RetirementReason.GC)
    authority.fence(ep.FencePublication(publication, fence))
    closed = authority.commit(request)
    assert closed.receipt == committed.receipt and not closed.forward_open
    assert closed.fence == fence and closed.fence_receipt is not None
    assert closed.accepted_fact == committed.accepted_fact
    assert pickle.loads(pickle.dumps(closed)) == closed
