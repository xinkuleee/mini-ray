"""Remaining owner-local single-output recovery rejection contracts.

One publication and one child owner table per case; Complete and death records
are explicit input facts. No Core/Node/Worker, Store, RPC, process, thread, user
function, or wait is started. This does not model a central publication registry.
The 27 legacy functions are mapped in the K3 output-recovery audit note.
"""

from dataclasses import replace

import pytest

from miniray.ids import WorkerID
from miniray.output_handoff import OutputHandoffConflictError
from miniray.ownership import OutputOwnerPublicationConflictError
from tests.unit.test_node_lost_output_resolution import _Fixture


pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "change", ("owner", "manifest-digest", "complete-witness", "publisher-pid"),
)
def test_resolution_rejects_changed_authority_before_owner_mutation(change):
    f = _Fixture()
    resolution = f.resolution(known=True, cleanup=f.cleanup())
    before = f.table.snapshot(f.output)
    child_before = f.child_table.snapshot(f.child)
    handoff_before = f.handoff.query(f.identity)
    with pytest.raises(OutputHandoffConflictError):
        if change == "owner":
            candidate = replace(resolution, owner_worker_id=WorkerID(b"x" * 16))
        elif change == "manifest-digest":
            candidate = replace(resolution, manifest_digest="f" * 64, complete=None)
        elif change == "complete-witness":
            candidate = replace(resolution, complete=replace(f.complete, manifest_digest="f" * 64))
        else:
            candidate = replace(resolution, node_death=replace(f.death, node_pid=f.death.node_pid + 1))
        f.table.resolve_output_node_loss(f.manifest, candidate)
    assert f.table.snapshot(f.output) == before
    assert f.child_table.snapshot(f.child) == child_before
    assert f.handoff.query(f.identity) == handoff_before
    assert f.table.output_owner_publication_receipt(f.plan) is None
    # Rejection must not occupy the receipt identity or prevent valid cleanup.
    assert f.table.resolve_output_node_loss(f.manifest, resolution)
    assert not f.table.resolve_output_node_loss(f.manifest, resolution)


@pytest.mark.parametrize(
    "known, message", ((False, "exact Complete"), (True, "locally retained output bytes")),
)
def test_keep_requires_exact_complete_and_independent_inline_bytes(known, message):
    f = _Fixture()
    if known:
        f.handoff.record_complete(f.complete)
    before = f.table.snapshot(f.output)
    child_before = f.child_table.snapshot(f.child)
    handoff_before = f.handoff.query(f.identity)
    with pytest.raises((OutputHandoffConflictError, OutputOwnerPublicationConflictError), match=message):
        resolution = f.resolution(keep=True)
        f.table.resolve_output_node_loss(f.manifest, resolution)
    assert f.table.snapshot(f.output) == before
    assert f.child_table.snapshot(f.child) == child_before
    assert f.handoff.query(f.identity) == handoff_before
    assert f.table.output_owner_publication_receipt(f.plan) is None


@pytest.mark.parametrize(
    "field, value", (("detection_id", "changed-death"), ("death_epoch", 4),
                      ("exit_code", 2), ("detail", "changed evidence")),
)
def test_resolved_receipt_rejects_changed_death_without_rewriting_history(field, value):
    f = _Fixture()
    resolution = f.resolution(known=True, cleanup=f.cleanup())
    assert f.table.resolve_output_node_loss(f.manifest, resolution)
    before = f.table.snapshot(f.output)
    child_before = f.child_table.snapshot(f.child)
    handoff_before = f.handoff.query(f.identity)
    receipt_before = f.table.output_owner_publication_receipt(f.plan)
    candidate = replace(resolution, node_death=replace(resolution.node_death, **{field: value}))
    with pytest.raises(OutputOwnerPublicationConflictError, match="resolution was rebound"):
        f.table.resolve_output_node_loss(f.manifest, candidate)
    assert f.table.snapshot(f.output) == before
    assert f.child_table.snapshot(f.child) == child_before
    assert f.handoff.query(f.identity) == handoff_before
    assert f.table.output_owner_publication_receipt(f.plan) == receipt_before
    assert not f.table.resolve_output_node_loss(f.manifest, resolution)
