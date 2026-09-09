"""Pure exact cleanup-vector boundaries on the current base journal.

The retired shared-container graph wire is not a base authority. Its common
invariant survives here: a terminal cleanup receipt must retain the complete
ordered identities of one real rollback plan. These are local ACK input facts,
not evidence of remote hold release. One output, two child identities, at most
five explicit effects; no runtime constructors, threads, sockets or waits.
"""

from dataclasses import replace
import pickle

import pytest

from miniray.output_publication import OutputPublicationConflictError
from miniray.output_publication_journal import (
    OutputPublicationAck, OutputPublicationRollbackTombstone,
)
from tests.unit.test_output_publication_journal import _Fixture


pytestmark = pytest.mark.unit


@pytest.mark.parametrize("bad", ("partial", "empty", "reversed", "duplicate", "foreign"))
def test_single_output_rollback_rejects_inexact_ack_vectors_and_tampered_roundtrip(bad):
    f = _Fixture()
    f.promote()
    plan = f.journal.begin_rollback(f.id, "exact-vector")
    assert f.rollback() == plan.effects
    terminal = f.journal.snapshot(f.id)
    receipt = terminal.rollback_tombstone
    assert len(receipt.acknowledgements) == len(plan.effects) == 5
    assert pickle.loads(pickle.dumps(receipt)) == receipt
    acks = receipt.acknowledgements
    foreign = OutputPublicationAck(replace(acks[0].effect, manifest_digest="f" * 64))
    invalid = {
        "partial": acks[:-1], "empty": (), "reversed": acks[::-1],
        "duplicate": acks + acks[:1], "foreign": (foreign,) + acks[1:],
    }[bad]
    with pytest.raises(OutputPublicationConflictError, match="every ordered effect ACK"):
        OutputPublicationRollbackTombstone(plan, invalid)
    forged = replace(receipt)
    object.__setattr__(forged, "acknowledgements", invalid)
    with pytest.raises(OutputPublicationConflictError, match="every ordered effect ACK"):
        pickle.loads(pickle.dumps(forged))
    assert f.journal.snapshot(f.id) == terminal
    assert f.journal.begin_rollback(f.id, "exact-vector") == plan
    assert f.journal.next_rollback_effect(f.id) is None
