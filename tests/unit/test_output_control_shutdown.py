"""Pure shutdown barriers for the base owner-led output path.

One accepted task, one inline output and zero or one real child owner.
The actual Core admission, handoff, loss cleanup and finish barriers run with
synchronous callbacks. No user execution, runtime constructors, thread,
socket, process, timer or wait is used. Complete and Node death are explicit
input facts; neither is evidence that a process ran or that bytes survived.
"""

from dataclasses import replace
import hashlib

import pytest

from miniray import output_protocol as wire, protocol
from miniray.contained_edges import ContainedReferenceHold
from miniray.core import _OutputNodeLossObligation
from miniray.ids import LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationHeader, OutputPublicationID,
    OutputPublicationManifest, OutputPublicationNodeIncarnation, OutputValue,
)
from miniray.ownership import ObjectOwnerTable, ObjectState
from miniray.publication_sources import OwnedContainedSource, PreparedContainedTransfer
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_common_cleanup_progress import _no_runtime


pytestmark = pytest.mark.unit


def _id(kind, value):
    return kind(bytes((value,)) * 16)


@pytest.mark.parametrize("refs", (False, True), ids=("plain", "child"))
def test_closed_admission_preserves_accepted_output_until_exact_cleanup_and_finish(refs, monkeypatch):
    core = make_pure_core()
    definition = core.define_remote_function(lambda: None)
    pending, ref = core._register_submission(
        definition, (), {}, ResourceVector(), num_returns=1, _enqueue=True,
    )
    assert core._submissions.get_nowait() == pending
    core._submissions.task_done()
    executor, publisher = _id(WorkerID, 91), _id(NodeID, 92)
    child_id = ObjectID.for_task(_id(TaskID, 93))
    transfer = PreparedContainedTransfer(
        child_id, executor, ("child.invalid", 1234), OwnedContainedSource(executor),
        ContainedReferenceHold(ref.object_id, executor, "shutdown-child"),
        ContainedReferenceHold(ref.object_id, core.worker_id, "shutdown-child"),
    )
    identity = OutputPublicationID(_id(LeaseID, 94), pending.execution)
    slot = (OutputValue(protocol.ResultStorage.INLINE, 5, hashlib.sha256(b'value').hexdigest(), (transfer,) if refs else ()))
    manifest = OutputPublicationManifest.create(OutputPublicationHeader(
        identity, core.job_id, executor, core.worker_id,
        OutputPublicationNodeIncarnation(publisher, 9201, 1),
    ), slot)
    complete = OutputPublicationCompleteWitness.for_manifest(manifest)
    request = wire.RegisterOutputHandoff(manifest)
    registered = core.register_output_handoff(request)
    assert registered.accepted
    child = ObjectOwnerTable()
    if refs:
        child.register(child_id, local_token="child-source")
        child.publish_inline(child_id, None, b"child")
        child.prepare_stored_contained_reference(transfer, authority_worker_id=executor)
        child.promote_stored_contained_reference(transfer, authority_worker_id=executor)
    calls = []

    def release(address, handler, message):
        assert refs and address == transfer.contained_owner_address
        assert handler == "release_contained_reference" and len(calls) < 3
        calls.append(message)
        released = child.release_contained_reference(message.object_id, message.hold)
        if len(calls) == 1:
            raise TimeoutError("exact child cleanup happened before its ACK was lost")
        return protocol.ReleaseContainedReferenceReply(
            message.object_id, message.owner_worker_id, message.hold, True, released,
        )

    core._borrow_rpc = release
    death = protocol.NodeDeathRecord(
        "shutdown-publisher-death", publisher, 9201, 1, 1, 7,
        protocol.NodeDeathReason.PROCESS_EXIT, "explicit member fact",
    )
    loss = _OutputNodeLossObligation(identity, death)
    try:
        # Admission closes through the real drain operation. Without a running
        # lane the accepted finish barrier is the finalization authority.
        assert core.shutdown(timeout=0.1, preserve_owner_protocol=True)
        assert not core._accepting and not core.owner_protocol_closed
        assert core._task_finish_barriers[ref.object_id] == pending
        assert core._accepted_task_count == 1
        assert not core.can_finalize_shutdown(require_distributed_clean=True)
        assert not core.finalize_shutdown(require_distributed_clean=True, timeout=0.1)
        before = core.owner_table.snapshot(ref.object_id)
        with pytest.raises(RuntimeError, match="shutting down"):
            core._register_submission(definition, (), {}, ResourceVector(), _enqueue=True)
        assert core.owner_table.snapshot(ref.object_id) == before
        assert core.register_output_handoff(request) == registered
        report = wire.ReportOutputHandoffComplete(complete)
        completed = core.report_output_handoff_complete(report)
        assert type(completed) is wire.OutputHandoffCompleteAck and completed.accepted and completed.witness == complete
        assert core.report_output_handoff_complete(report) == completed
        if refs:
            assert not core._drive_output_node_loss(pending, loss)
            assert identity in core._output_node_cleanup
            assert pending.task_key in core._protocol_unresolved
            assert not core.shutdown(timeout=0.1, preserve_owner_protocol=True)
            assert not core.owner_protocol_closed
            assert core.owner_table.snapshot(ref.object_id) == before
        assert core._drive_output_node_loss(pending, loss)
        assert core.owner_table.snapshot(ref.object_id).state is ObjectState.LOST
        assert identity not in core._output_node_cleanup
        assert pending.task_key not in core._protocol_unresolved
        assert not core.can_finalize_shutdown(require_distributed_clean=True)
        assert core._finish_pending_task(pending)
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
        assert core._finish_pending_task(pending)
        core._reference_mailbox.drain()
        assert core.can_finalize_shutdown(require_distributed_clean=True)
        assert core._drive_output_node_loss(pending, loss)
        assert len(calls) == (3 if refs else 0)
        if refs:
            assert calls[0] == calls[1]
            assert not child.snapshot(child_id).contained_holds
            assert child.snapshot(child_id).local_tokens == frozenset({"child-source"})
        # No reference event thread exists in this fixture. Finalization still
        # performs its real owner protocol fence before stopping that boundary.
        stopped = []
        def stop_reference_events(_deadline):
            assert core.owner_protocol_closed
            stopped.append(True)
            return True
        monkeypatch.setattr(core, "_stop_reference_events", stop_reference_events)
        assert core.finalize_shutdown(require_distributed_clean=True, timeout=0.1)
        assert core.owner_protocol_closed and stopped == [True]
        assert not core.report_output_handoff_complete(report).accepted
    finally:
        # Final owner closure leaves no consumer to service a handle close.
        # Release the fixture's local token explicitly, without claiming GC.
        for token in tuple(core.owner_table.snapshot(ref.object_id).local_tokens):
            core.owner_table.release_local_reference(ref.object_id, token)
        close_pure_core(core)
