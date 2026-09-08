"""Attach one tiny real publication path to an existing unstarted Node.

The caller owns the lease and root/bundle resource ledgers. This fixture adds
only an in-memory journal, metadata authority and optional 1 KiB object store;
it never starts transport, changes a lease state, or fabricates Complete.
"""

from types import SimpleNamespace

import pytest

from miniray import output_protocol as wire, protocol
from miniray.ids import JobID
from miniray.object_store import ObjectStore
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import (
    OutputPublicationHeader, OutputPublicationID, OutputPublicationNodeIncarnation,
)
from miniray.output_publication_journal import OutputPublicationJournal
from miniray.output_publication_node import OutputPublicationNodeAdapter
from miniray.output_recovery import OutputPublicationRecoveryAuthority
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest


def prepare_ref_free_output(node, request, grant, *, job_id=None, values=(7,)):
    """Prepare/ARM real INLINE slots without changing caller ledger truth.

    Return manifest/journal/recovery for assertions. Call the real Node
    Complete handler separately to test resource release and exact replay.
    Node process identity and return_ids must be declared by the fixture.
    """
    assert 1 <= len(values) <= 3 and len(request.return_ids) == len(values)
    assert request.target_execution is None
    identity = OutputPublicationID(request.lease_id, TaskExecutionKey(
        TaskOutputManifest(request.task_id, request.return_ids), request.attempt_id,
    ))
    session = OutputDiscoverySession(OutputPublicationHeader(
        identity, job_id or JobID.random(), grant.worker_id, request.requester_worker_id,
        OutputPublicationNodeIncarnation(node.node_id, node._node_pid, node._registration_epoch),
    ), inline_threshold=1024)
    outputs = session.discover(tuple(values))
    assert all(slot.tier is protocol.ResultStorage.INLINE and not slot.transfers
               and slot.size_bytes <= 1024 for slot in outputs.manifest.slots)
    journal = OutputPublicationJournal()
    recovery = OutputPublicationRecoveryAuthority()

    def forbidden(*_args, **_kwargs):
        pytest.fail("tiny INLINE lease fixture attempted child/graph/store/RPC work")

    adapter = OutputPublicationNodeAdapter(
        journal, report_intent=recovery.report_intent, arm_complete=recovery.arm_complete,
        report_terminal=recovery.report_terminal, report_rollback=recovery.report_rollback,
        prepare_child=forbidden, promote_child=forbidden, release_child=forbidden,
        prepare_graph=forbidden, abort_graph=forbidden, seal_replica=forbidden,
        drop_replica=forbidden,
    )
    assert getattr(node, "_output_publication_journal", None) is None
    node._output_publication_journal = journal
    node._output_publications = adapter
    if not hasattr(node, "_object_store"):
        node._object_store = ObjectStore(1024)
    node._sealed_metadata = getattr(node, "_sealed_metadata", {})
    node._dependency_pin_cleanups = getattr(node, "_dependency_pin_cleanups", {})
    before = node.resource_ledger.snapshot()
    prepared = node._handle_prepare_output_publication(
        wire.PrepareOutputPublication(outputs.manifest, outputs.slot_payloads),
    )
    assert prepared.accepted and journal.snapshot(identity).ready_to_complete
    assert recovery.snapshot(identity).armed and node.resource_ledger.snapshot() == before
    session.release_sources_after_promotions()
    return SimpleNamespace(manifest=outputs.manifest, journal=journal, recovery=recovery, adapter=adapter)
