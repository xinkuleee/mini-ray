"""Pure full-manifest boundaries retained from the old stored test family.

The identity is a real unified OutputPublicationID with two selected containers.
Only the in-memory graph authority runs: no GCS, Node, transport, thread or wait.
"""

from __future__ import annotations

from dataclasses import replace
import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray.contained_cycle import (
    ContainedContainerBusyError, ContainedGraphManifestDisposition,
    ContainedGraphManifestReceipt, ContainedGraphTransaction,
    ContainedGraphTransactionConflictError, ContainedGraphTransactionState,
    ContainedGraphTransactionStateError, ContainedReferenceGraphAuthority,
)
from miniray.output_publication import OutputPublicationID, OutputPublicationManifest
from tests.unit.test_output_publication import _Fixture


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure manifest boundary attempted runtime infrastructure")

    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait"),
                         (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


@pytest.mark.parametrize("target", (False, True), ids=("full", "targeted"))
def test_full_manifest_cannot_be_downgraded_or_released_through_edge_only_apis(target):
    values = _Fixture(target=target)
    manifest = values.manifest.to_graph_manifest()
    assert type(manifest.publication_id) is OutputPublicationID
    assert len(values.manifest.slots) == 2
    authority = ContainedReferenceGraphAuthority()
    prepared = authority.prepare_manifest(manifest)
    committed = authority.commit_manifest(manifest)
    assert prepared.disposition is ContainedGraphManifestDisposition.APPLIED
    assert committed.disposition is ContainedGraphManifestDisposition.APPLIED
    assert authority.get_manifest(manifest.transaction_id) == manifest
    before = authority.snapshot()
    edge_only = ContainedGraphTransaction(manifest.transaction_id, manifest.ordered_edges)
    # An exact edge list still lacks the frozen publication, owner and digest.
    # These mutations must raise a conflict, not return a misleading no-op ACK.
    for mutate in (authority.prepare, authority.commit, authority.abort, authority.publish):
        with pytest.raises(ContainedGraphTransactionConflictError, match="edge-only"):
            mutate(edge_only)
        assert authority.snapshot() == before
    for slot in values.manifest.slots:
        assert authority.release_container(slot.object_id) == ()
        assert authority.snapshot() == before

    first, second = values.manifest.slots
    released = authority.release_manifest_container(manifest, first.object_id)
    replay = authority.release_manifest_container(manifest, first.object_id)
    assert released.disposition is ContainedGraphManifestDisposition.RELEASED
    assert replay.disposition is ContainedGraphManifestDisposition.ALREADY_RELEASED
    assert released.released_edges == replay.released_edges == first.edges
    snapshot = authority.snapshot()
    assert set(snapshot.committed_edges) == set(second.edges)
    assert snapshot.manifests[0].state is ContainedGraphTransactionState.COMMITTED
    assert snapshot.manifests[0].released_container_ids == (first.object_id,)
    assert authority.has_active_obligations()
    assert authority.release_container(second.object_id) == ()
    assert authority.snapshot() == snapshot
    final = authority.release_manifest_container(manifest, second.object_id)
    assert final.released_edges == second.edges
    assert not authority.has_active_obligations()
    assert not authority.snapshot().committed_edges
    assert authority.snapshot().manifests[0].released_container_ids == tuple(sorted((first.object_id, second.object_id)))
    assert authority.get_manifest(manifest.transaction_id) == manifest
    assert authority.commit_manifest(manifest).disposition is ContainedGraphManifestDisposition.ALREADY_COMMITTED
    assert not authority.has_active_obligations()


@pytest.mark.parametrize("target", (False, True), ids=("full", "targeted"))
def test_unseen_full_manifest_abort_tombstones_exact_identity_and_fences_late_prepare(target):
    values = _Fixture(target=target)
    manifest = values.manifest.to_graph_manifest()
    authority = ContainedReferenceGraphAuthority()
    assert authority.get_manifest(manifest.transaction_id) is None
    aborted = authority.abort_manifest(manifest)
    replay = authority.abort_manifest(manifest)
    assert aborted.disposition is ContainedGraphManifestDisposition.APPLIED
    assert aborted.state is ContainedGraphTransactionState.ABORTED
    assert replay.disposition is ContainedGraphManifestDisposition.ALREADY_ABORTED
    assert authority.get_manifest(manifest.transaction_id) == manifest
    before = authority.snapshot()
    assert before.manifests[0].state is ContainedGraphTransactionState.ABORTED
    assert not before.prepared_edges and not before.committed_edges
    assert not authority.has_active_obligations()
    with pytest.raises(ContainedGraphTransactionStateError, match="aborted"):
        authority.prepare_manifest(manifest)
    assert authority.snapshot() == before
    changed = replace(manifest, manifest_digest="ef" * 32)
    assert changed.manifest_digest != manifest.manifest_digest
    with pytest.raises(ContainedGraphTransactionConflictError, match="identity"):
        authority.prepare_manifest(changed)
    with pytest.raises(ContainedGraphTransactionConflictError, match="identity"):
        authority.abort_manifest(changed)
    assert authority.snapshot() == before
    assert authority.get_manifest(manifest.transaction_id) == manifest
    assert authority.abort_manifest(manifest) == replay


def test_manifest_receipt_rejects_forged_release_state_and_foreign_edge():
    values = _Fixture()
    manifest = values.manifest.to_graph_manifest()
    with pytest.raises(ValueError, match="non-empty committed"):
        ContainedGraphManifestReceipt(
            manifest, ContainedGraphTransactionState.COMMITTED,
            ContainedGraphManifestDisposition.RELEASED,
        )
    with pytest.raises(ValueError, match="contradicts"):
        ContainedGraphManifestReceipt(
            manifest, ContainedGraphTransactionState.PREPARED,
            ContainedGraphManifestDisposition.ALREADY_COMMITTED,
        )
    foreign = replace(manifest.ordered_edges[0], transfer_token="not-the-original-hold")
    with pytest.raises(ValueError, match="ordered subset"):
        ContainedGraphManifestReceipt(
            manifest, ContainedGraphTransactionState.COMMITTED,
            ContainedGraphManifestDisposition.RELEASED, (foreign,),
        )
    # A legitimate release receipt remains exact and ordered, not merely a
    # truthy status object. Only one selected container belongs to this ACK.
    slot = values.manifest.slots[0]
    receipt = ContainedGraphManifestReceipt(
        manifest, ContainedGraphTransactionState.COMMITTED,
        ContainedGraphManifestDisposition.RELEASED, slot.edges,
    )
    assert receipt.manifest == manifest and receipt.released_edges == slot.edges


def test_graph_admission_close_preserves_prepared_and_committed_cleanup_replays():
    values = _Fixture()
    base = values.manifest.to_graph_manifest()
    # The shared value fixture is deterministic: distinct lease identities
    # create three genuine publication manifests instead of three aliases.
    prepared = base
    committed_id = replace(values.publication_id, lease_id=type(values.publication_id.lease_id)(b"C" * 16))
    unseen_id = replace(values.publication_id, lease_id=type(values.publication_id.lease_id)(b"U" * 16))
    committed = OutputPublicationManifest.create(
        replace(values.header, publication_id=committed_id), values.manifest.slots,
    ).to_graph_manifest()
    unseen = OutputPublicationManifest.create(
        replace(values.header, publication_id=unseen_id), values.manifest.slots,
    ).to_graph_manifest()
    authority = ContainedReferenceGraphAuthority()
    authority.prepare_manifest(prepared)
    authority.prepare_manifest(committed)
    authority.commit_manifest(committed)
    authority.close_admission()
    assert authority.snapshot().admission_closed and authority.has_active_obligations()
    assert authority.prepare_manifest(prepared).disposition is ContainedGraphManifestDisposition.ALREADY_PREPARED
    assert authority.commit_manifest(committed).disposition is ContainedGraphManifestDisposition.ALREADY_COMMITTED
    before = authority.snapshot()
    with pytest.raises(ContainedGraphTransactionStateError, match="admission is closed"):
        authority.prepare_manifest(unseen)
    assert authority.snapshot() == before and authority.get_manifest(unseen.transaction_id) is None
    with pytest.raises(ContainedContainerBusyError):
        authority.release_manifest_container(prepared, values.manifest.slots[0].object_id)
    assert authority.snapshot() == before
    assert authority.abort_manifest(prepared).disposition is ContainedGraphManifestDisposition.APPLIED
    assert authority.abort_manifest(prepared).disposition is ContainedGraphManifestDisposition.ALREADY_ABORTED
    for slot in values.manifest.slots:
        released = authority.release_manifest_container(committed, slot.object_id)
        replay = authority.release_manifest_container(committed, slot.object_id)
        assert released.disposition is ContainedGraphManifestDisposition.RELEASED
        assert replay.disposition is ContainedGraphManifestDisposition.ALREADY_RELEASED
        assert released.released_edges == replay.released_edges == slot.edges
    assert not authority.has_active_obligations()
    assert not authority.snapshot().prepared_edges and not authority.snapshot().committed_edges
    assert authority.snapshot().admission_closed
