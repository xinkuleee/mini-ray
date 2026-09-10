"""Pure single-output journal retirement, without retired per-slot APIs.

One actual OutputDiscoverySession and journal, one output <=64 bytes. Owner
registration/materialization ACKs and adoption proof are explicit pure inputs,
not evidence of a real lease, owner CAS, RPC or physical object GC. The current
real adoption ACK-loss smoke separately proves that wiring. No Core/Node/Worker
constructor, thread, socket, process, timer or wait is used.
"""

from dataclasses import replace

import pytest

from miniray import protocol
from miniray.ids import AttemptID, JobID, LeaseID, NodeID, TaskID, WorkerID
from miniray.output_discovery import OutputDiscoverySession
from miniray.output_publication import (OutputPublicationCompleteWitness, OutputPublicationHeader,
    OutputPublicationID, OutputPublicationNodeIncarnation)
from miniray.output_publication_journal import (OutputPublicationAck, OutputPublicationAdoptionProof,
    OutputPublicationConflictError, OutputPublicationJournal, OutputPublicationJournalState,
    OutputPublicationJournalStateError, OutputPublicationPayloadRetired)
from miniray.task_outputs import TaskExecution

pytestmark = pytest.mark.unit


def _prepared():
    job, task = JobID(b"j" * 16), TaskID(b"t" * 16)
    owner, executor = WorkerID(b"o" * 16), WorkerID(b"w" * 16)
    identity = OutputPublicationID(LeaseID(b"l" * 16),
        (TaskExecution(AttemptID(task, 0))))
    header = OutputPublicationHeader(identity, job, executor, owner,
        OutputPublicationNodeIncarnation(NodeID(b"n" * 16), 1001, 1))
    discovery = OutputDiscoverySession(header, inline_threshold=1024)
    outputs = discovery.discover((('one', 42)))
    manifest, = (outputs.manifest,)
    slot = (manifest.value)
    descriptor = protocol.ResultDescriptor(identity.object_id, slot.tier, slot.size_bytes, owner,
        header.node_incarnation.node_id, slot.checksum, (outputs.payload))
    journal = OutputPublicationJournal()
    journal.open(manifest)
    journal.ack_owner_registered(OutputPublicationAck(journal.begin_owner_register(identity)))
    journal.ack_materialized(OutputPublicationAck(journal.begin_materialize(identity, 0)), descriptor)
    witness = OutputPublicationCompleteWitness.for_manifest(manifest)
    proof = OutputPublicationAdoptionProof(witness, owner, "pure-owner-cas-input")
    discovery.release_sources_after_promotions()
    return journal, identity, descriptor, witness, proof


def test_retirement_requires_complete_and_keeps_precomplete_payload_unchanged():
    journal, identity, descriptor, witness, proof = _prepared()
    before = journal.snapshot(identity)
    with pytest.raises(OutputPublicationJournalStateError):
        journal.retire_completed(proof)
    assert journal.snapshot(identity) == before
    assert journal.materialized_result(identity, 0) == descriptor
    journal.complete(identity, witness)
    assert journal.retire_completed(proof)


def test_exact_retirement_replays_without_recreating_payload_or_erasing_complete():
    journal, identity, descriptor, witness, proof = _prepared()
    envelope = journal.complete(identity, witness)
    assert ((envelope.result,)) == (descriptor,)
    retired = journal.retire_completed(proof)
    assert len(retired) == 1 and journal.retire_completed(proof) == retired
    snapshot = journal.snapshot(identity)
    assert snapshot.state is OutputPublicationJournalState.RETIRED
    assert snapshot.complete == witness and snapshot.retained_result_slots == ()
    assert journal.materialized_result(identity, 0) is None
    with pytest.raises(OutputPublicationPayloadRetired) as caught:
        journal.complete(identity, witness)
    assert caught.value.tombstones == retired
    assert journal.snapshot(identity) == snapshot


def test_conflicting_retirement_proof_cannot_replace_exact_history():
    journal, identity, descriptor, witness, proof = _prepared()
    journal.complete(identity, witness)
    retired = journal.retire_completed(proof)
    before = journal.snapshot(identity)
    with pytest.raises(OutputPublicationConflictError):
        journal.retire_completed(replace(proof, owner_commit_id="different-cas"))
    assert journal.retire_completed(proof) == retired
    assert journal.snapshot(identity) == before
