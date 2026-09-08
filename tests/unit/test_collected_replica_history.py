"""Pure owner authority for late replicas of collected generic stored values.

One owner table and one tiny descriptor per case; no Core, Node, store, RPC,
thread, process, wait or payload execution. Collection completion is a supplied
reducer cut, not a claim of physical Drop. Tests check exact cleanup authority
and its compact history independently of the Node/GC composition tests.
"""

from dataclasses import fields, replace
import hashlib

import pytest

from miniray import protocol
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import (
    ObjectAlreadyRegisteredError, ObjectCollectionState, ObjectOwnerTable, ObjectState,
    OutputOwnerPublicationConflictError, UnknownObjectError,
)


pytestmark = pytest.mark.unit


def _fixture(*, canonical=True):
    owner, source, target = WorkerID(b"o" * 16), NodeID(b"s" * 16), NodeID(b"t" * 16)
    task = TaskID.for_put(JobID(b"j" * 16), owner, 0)
    output, attempt = ObjectID.for_task(task), AttemptID(task, 0)
    result = protocol.ResultDescriptor(output, protocol.ResultStorage.OBJECT_STORE, 3,
                                       owner, source, hashlib.sha256(b"put").hexdigest())
    table = ObjectOwnerTable()
    table.register(output, current_attempt=attempt)
    assert table.publish_stored(output, attempt, source, descriptor=result if canonical else None)
    late = protocol.ObjectStoreDescriptor(output, owner, attempt, target, result.size_bytes, result.checksum)
    return table, result, late


def _begin(table, result, late):
    return table.begin_collection(late.object_id, collection_id="generic-stored-collected",
                                  canonical_size_bytes=result.size_bytes, canonical_checksum=result.checksum)


def test_live_or_lost_current_replica_has_no_retired_generic_cleanup_authority():
    table, result, late = _fixture()
    assert table.retired_stored_replica(late) is None
    assert table.mark_lost(late.object_id, late.producer_attempt_id)
    before = table.snapshot(late.object_id)
    assert before.state is ObjectState.LOST and before.canonical_stored_result == result
    assert table.retired_stored_replica(late) is None
    assert table.snapshot(late.object_id) == before and not table._stored_collection_history


def test_collecting_put_accepts_late_cleanup_without_expanding_frozen_locations():
    table, result, late = _fixture()
    plan = _begin(table, result, late)
    assert plan is not None and plan.locations == (result.node_id,)
    before = table.snapshot(late.object_id)
    drop = table.retired_stored_replica(late)
    assert drop == protocol.DropObjectReplica(late.object_id, late.producer_attempt_id,
                                               late.owner_worker_id, late.node_id, late.checksum)
    assert table.snapshot(late.object_id) == before
    assert before.collection_plan == plan and before.locations == frozenset((result.node_id,))
    assert not table._stored_collection_history
    assert table.complete_collection(plan).collected
    assert table.retired_stored_replica(late) == drop


def test_collected_put_keeps_only_compact_identity_and_never_resurrects_metadata():
    table, result, late = _fixture()
    plan = _begin(table, result, late)
    assert table.complete_collection(plan).collected
    history = table._stored_collection_history[late.object_id]
    assert tuple(field.name for field in fields(history)) == (
        "object_id", "producer_attempt_id", "owner_worker_id", "size_bytes", "checksum", "collection_id",
    )
    assert (history.object_id, history.producer_attempt_id, history.owner_worker_id,
            history.size_bytes, history.checksum, history.collection_id) == (
        late.object_id, late.producer_attempt_id, late.owner_worker_id, late.size_bytes, late.checksum, plan.collection_id,
    )
    assert not table.contains(late.object_id) and not table._output_publication_receipts
    assert table.collection_state(late.object_id) is ObjectCollectionState.COLLECTED
    assert table.retired_output_replica(late) is None  # Generic history did not create an output publication.
    assert table.retired_stored_replica(late).node_id == late.node_id
    with pytest.raises(UnknownObjectError):
        table.snapshot(late.object_id)
    with pytest.raises(ObjectAlreadyRegisteredError):
        table.register(late.object_id, current_attempt=late.producer_attempt_id)


@pytest.mark.parametrize("field", ("owner", "attempt", "size", "checksum"))
def test_late_replica_conflict_never_authorizes_drop_before_or_after_collection(field):
    table, result, late = _fixture()
    if field == "owner":
        bad = replace(late, owner_worker_id=WorkerID(b"x" * 16))
    elif field == "attempt":
        bad = replace(late, producer_attempt_id=late.producer_attempt_id.next())
    elif field == "size":
        bad = replace(late, size_bytes=late.size_bytes + 1)
    else:
        bad = replace(late, checksum="d" * 64)
    plan = _begin(table, result, late)
    before = table.snapshot(late.object_id)
    with pytest.raises(OutputOwnerPublicationConflictError):
        table.retired_stored_replica(bad)
    assert table.snapshot(late.object_id) == before and not table._stored_collection_history
    assert table.complete_collection(plan).collected
    history = table._stored_collection_history[late.object_id]
    with pytest.raises(OutputOwnerPublicationConflictError):
        table.retired_stored_replica(bad)
    assert table._stored_collection_history[late.object_id] == history
    assert table.retired_stored_replica(late).checksum == late.checksum


def test_compact_history_does_not_alias_retired_input_or_cleanup_reply_ids():
    table, result, late = _fixture()
    plan = _begin(table, result, late)
    assert table.complete_collection(plan).collected
    original = protocol.ObjectStoreDescriptor(
        ObjectID(TaskID(bytes(late.object_id.task_id)), late.object_id.return_index),
        WorkerID(bytes(late.owner_worker_id)), AttemptID(TaskID(bytes(late.producer_attempt_id.task_id)), 0),
        NodeID(bytes(late.node_id)), late.size_bytes, late.checksum,
    )
    history = table._stored_collection_history[original.object_id]
    assert history.object_id is not late.object_id and history.producer_attempt_id is not late.producer_attempt_id
    assert history.owner_worker_id is not result.owner_worker_id
    object.__setattr__(result.owner_worker_id, "value", b"v" * 16)
    object.__setattr__(plan.producer_attempt_id, "attempt_number", 9)
    object.__setattr__(plan.object_id.task_id, "value", b"z" * 16)
    object.__setattr__(plan, "canonical_checksum", "e" * 64)
    drop = table.retired_stored_replica(original)
    assert drop.owner_worker_id == original.owner_worker_id and drop.producer_attempt_id == original.producer_attempt_id
    object.__setattr__(drop.owner_worker_id, "value", b"w" * 16)
    object.__setattr__(drop.producer_attempt_id, "attempt_number", 8)
    assert table.retired_stored_replica(original).checksum == original.checksum
    assert table._stored_collection_history[original.object_id] is history


def test_collection_plan_mismatch_cannot_mint_history_or_remove_owner_entry():
    table, result, late = _fixture()
    plan = table.begin_collection(late.object_id, collection_id="wrong-canonical",
                                  canonical_size_bytes=result.size_bytes, canonical_checksum="f" * 64)
    before = table.snapshot(late.object_id)
    with pytest.raises(OutputOwnerPublicationConflictError):
        table.retired_stored_replica(late)
    with pytest.raises(OutputOwnerPublicationConflictError):
        table.complete_collection(plan)
    assert table.snapshot(late.object_id) == before
    assert table.collection_state(late.object_id) is ObjectCollectionState.COLLECTING
    assert not table._stored_collection_history and late.object_id not in table._collected


def test_descriptorless_generic_collection_cannot_invent_owner_history():
    table, result, late = _fixture(canonical=False)
    plan = _begin(table, result, late)
    assert table.retired_stored_replica(late) is None
    assert table.complete_collection(plan).collected
    assert table.collection_state(late.object_id) is ObjectCollectionState.COLLECTED
    assert table.retired_stored_replica(late) is None
    assert not table._stored_collection_history


def test_unknown_or_collected_inline_object_does_not_gain_stored_retirement_authority():
    table, _result, late = _fixture()
    unknown = ObjectID.for_task(TaskID(b"u" * 16))
    descriptor = replace(late, object_id=unknown, producer_attempt_id=AttemptID(unknown.task_id, 0))
    assert table.retired_stored_replica(descriptor) is None
    table.register(unknown, current_attempt=descriptor.producer_attempt_id)
    assert table.publish_inline(unknown, descriptor.producer_attempt_id, b"inline")
    plan = table.begin_collection(unknown, collection_id="inline-collected")
    assert table.complete_collection(plan).collected
    assert table.retired_stored_replica(descriptor) is None
    assert unknown not in table._stored_collection_history
