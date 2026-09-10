"""Common output-loss invariants on the current base owner/Core authorities.

The old GCS freeze/graph/KEEP-DROP matrix remains in the fixed B/E archives;
its per-function disposition is recorded in the K3 output-control audit. Each
case uses one child table or one threadless Core submission, one publication,
and at most three synchronous child-release calls. No GCS, Node, Worker, Store,
transport, thread, process, timer, wait, or user function is started.
"""

from dataclasses import replace
import hashlib

import pytest

from miniray import output_protocol as wire, protocol
from miniray.ids import AttemptID, LeaseID, NodeID, ObjectID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationHeader, OutputPublicationID, OutputPublicationManifest,
    OutputPublicationNodeIncarnation, OutputValue,
)
from miniray.ownership import ObjectOwnerTable, ObjectState, OwnershipError, UnknownObjectError
from miniray.recovery import TaskState
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_common_cleanup_progress import _CorePublication, _no_runtime
from tests.unit.test_output_handoff import _Fixture as _HandoffValues


pytestmark = pytest.mark.unit


def test_collected_child_accepts_exact_compensation_without_reviving_its_identity():
    values = _HandoffValues()
    transfer = (values.manifest.value).transfers[0]
    child = transfer.contained_object_id
    owner = ObjectOwnerTable()
    attempt = AttemptID(child.task_id, 0)
    owner.register(child, current_attempt=attempt, local_token="ephemeral-source")
    owner.publish_inline(child, attempt, b"tiny")
    assert owner.release_local_reference(child, "ephemeral-source")
    plan = owner.begin_collection(child, collection_id="collected-ephemeral-child")
    assert plan is not None and owner.complete_collection(plan).collected

    for hold in (transfer.final_hold, transfer.provisional_hold):
        assert not owner.contained_release_was_seen(child, hold)
        assert not owner.release_contained_reference(child, hold)
        assert owner.contained_release_was_seen(child, hold)
        assert not owner.release_contained_reference(child, hold)
        assert not owner.contains(child)
    with pytest.raises(OwnershipError):
        owner.register(child)
    unrelated = ObjectID.for_task(TaskID(b"u" * 16))
    with pytest.raises(UnknownObjectError):
        owner.release_contained_reference(unrelated, transfer.final_hold)
    assert not owner.contained_release_was_seen(unrelated, transfer.final_hold)


@pytest.mark.parametrize("bad_reply", ("subclass", "accepted", "released", "hold", "rejected"))
def test_cleanup_revalidates_child_ack_before_retiring_an_obligation(bad_reply):
    publication = _CorePublication()
    try:
        publication.known_complete()
        core = publication.core
        before = core.owner_table.snapshot(publication.ref.object_id)
        corrupt = [True]

        class ReplySubclass(protocol.ReleaseContainedReferenceReply):
            pass

        def release(address, handler, request):
            assert not core._state_lock._is_owned()
            # The child really releases first. A malformed ACK cannot settle
            # Core's obligation, and its retry must use the real tombstone.
            reply = publication.release(address, handler, request)
            assert len(publication.releases) <= 3
            if not corrupt[0]:
                return reply
            if bad_reply == "subclass":
                return ReplySubclass(
                    reply.object_id, reply.owner_worker_id, reply.hold,
                    reply.accepted, reply.released,
                )
            if bad_reply in ("accepted", "released"):
                object.__setattr__(reply, bad_reply, 1)
            elif bad_reply == "hold":
                object.__setattr__(reply, "hold", replace(
                    request.hold, transfer_token="different-effect",
                ))
            else:
                return protocol.ReleaseContainedReferenceReply(
                    request.object_id, request.owner_worker_id, request.hold,
                    False, False, "release acknowledgement rejected",
                )
            return reply

        core._borrow_rpc = release
        assert not publication.loss()
        assert len(publication.releases) == 1
        first = publication.releases[0]
        assert first.hold == publication.transfer.final_hold
        assert publication.child_table.contained_release_was_seen(publication.child, first.hold)
        assert core.owner_table.snapshot(publication.ref.object_id) == before
        work = core._output_node_cleanup[publication.identity]
        assert work.acks == {} and work.complete == publication.complete
        assert publication.pending.task_key in core._protocol_unresolved
        assert core._output_loss_drivers == set()

        corrupt[0] = False
        assert publication.loss()
        assert publication.releases[:2] == [first, first]
        assert len(publication.releases) == 3
        assert publication.releases[2].hold == publication.transfer.provisional_hold
        assert not publication.child_table.snapshot(publication.child).contained_holds
        assert publication.identity not in core._output_node_cleanup
        assert publication.pending.task_key not in core._protocol_unresolved
        assert core._output_handoff_table().query(publication.identity).complete == publication.complete
        after = core.owner_table.snapshot(publication.ref.object_id)
        assert after.state is ObjectState.LOST and after.inline_data is None
        assert after.local_tokens == before.local_tokens and after.producer_task_spec == before.producer_task_spec
        assert core._recovery.task_record(publication.pending.task_id).state is TaskState.SUCCEEDED
        releases = tuple(publication.releases)
        assert publication.loss()
        assert tuple(publication.releases) == releases
        assert core.owner_table.snapshot(publication.ref.object_id) == after
    finally:
        publication.close()


def test_dead_publisher_first_registration_does_not_create_handoff_history():
    core = make_pure_core()
    reference = None
    try:
        definition = core.define_remote_function(lambda: None)
        pending, reference = core._register_submission(
            definition, (), {}, ResourceVector(), num_returns=1, _enqueue=True,
        )
        assert core._submissions.get_nowait() == pending
        core._submissions.task_done()
        publisher = NodeID(b"p" * 16)
        incarnation = OutputPublicationNodeIncarnation(publisher, 4201, 1)
        identity = OutputPublicationID(LeaseID(b"l" * 16), pending.execution)
        payload = b"one output"
        manifest = OutputPublicationManifest.create(
            OutputPublicationHeader(
                identity, core.job_id, WorkerID(b"e" * 16), core.worker_id, incarnation,
            ),
            (OutputValue(protocol.ResultStorage.INLINE, len(payload), hashlib.sha256(payload).hexdigest(), ())),
        )
        # An already-installed local membership fact is input to this handler
        # test; it is not a simulated detector or a new GCS admission rule.
        death = protocol.NodeDeathRecord(
            "publisher-exited-before-registration", publisher, 4201, 1, 1, 7,
            protocol.NodeDeathReason.PROCESS_EXIT, "confirmed membership input",
        )
        core._dead_nodes[publisher] = death
        table = core._output_handoff_table()
        assert table.snapshots() == ()
        before = core.owner_table.snapshot(reference.object_id)
        request = wire.RegisterOutputHandoff(manifest)
        for _ in range(2):
            rejected = core.register_output_handoff(request)
            assert not rejected.accepted and rejected.error == "publishing Node is dead"
            assert rejected.snapshot is None and table.snapshots() == ()
            assert core.owner_table.snapshot(reference.object_id) == before
            assert core._task_finish_barriers[reference.object_id] == pending
        assert core._dead_nodes[publisher] == death
        assert not core._protocol_unresolved
    finally:
        if reference is not None:
            reference.close()
        close_pure_core(core)
