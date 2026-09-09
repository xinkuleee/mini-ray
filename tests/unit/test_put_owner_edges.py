"""Pure owner CAS checks for put values containing references."""

from dataclasses import replace
import hashlib

import pytest

from miniray import protocol
from miniray.contained_edges import ContainedReferenceEdge
from miniray.errors import ProtocolError
from miniray.ids import AttemptID, JobID, NodeID, ObjectID, TaskID, WorkerID
from miniray.ownership import (
    ConflictingObjectResultError, InvalidObjectTransitionError,
    ObjectCollectionInProgressError, ObjectOwnerTable, ObjectState,
)
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit


def _id(kind, number):
    return kind(bytes((number,)) * 16)


class _Fixture:
    def __init__(self, *, stored=False, task_lineage=False):
        self.task = _id(TaskID, 1)
        self.output = ObjectID.for_task(self.task)
        self.attempt = AttemptID(self.task, 0)
        self.owner = _id(WorkerID, 2)
        self.node = _id(NodeID, 3)
        self.edge = ContainedReferenceEdge(
            self.output, ObjectID.for_task(_id(TaskID, 4)), _id(WorkerID, 5),
            ("child.invalid", 1234), "put-child",
        )
        payload = b"put value"
        self.descriptor = protocol.ResultDescriptor(
            self.output, protocol.ResultStorage.OBJECT_STORE if stored else protocol.ResultStorage.INLINE,
            len(payload), self.owner, self.node, hashlib.sha256(payload).hexdigest(),
            None if stored else payload,
        )
        self.table = ObjectOwnerTable()
        spec = None
        if task_lineage:
            job = _id(JobID, 6)
            spec = protocol.TaskSpec(
                job, self.task, self.attempt, protocol.FunctionKey(job, __name__, "fixture", "1"),
                (), 1, ResourceVector(), self.owner,
            )
        self.table.register(self.output, current_attempt=self.attempt, producer_task_spec=spec)

    def publish(self, *, descriptor=None, edges=None, attempt=None):
        return self.table.publish_put_value(
            self.output, self.attempt if attempt is None else attempt,
            self.descriptor if descriptor is None else descriptor,
            (self.edge,) if edges is None else edges,
        )


def test_put_publishes_inline_and_stored_value_with_edges_and_replays_exactly():
    for stored in (False, True):
        f = _Fixture(stored=stored)
        other_handle = replace(f.edge, transfer_token="other-handle-same-child")
        assert f.publish(edges=(f.edge, f.edge, other_handle))
        snapshot = f.table.snapshot(f.output)
        assert snapshot.state is (ObjectState.READY_STORED if stored else ObjectState.READY_INLINE)
        assert snapshot.outgoing_contained_edges == frozenset({f.edge, other_handle})
        assert snapshot.producer_task_spec is None and snapshot.output_publication is None
        assert snapshot.inline_data == (None if stored else f.descriptor.inline_data)
        assert snapshot.canonical_stored_result == (f.descriptor if stored else None)
        assert snapshot.locations == (frozenset({f.node}) if stored else frozenset())
        assert f.publish(edges=(other_handle, f.edge))
        assert f.table.snapshot(f.output) == snapshot


def test_bad_edge_or_descriptor_cannot_publish_value_or_partial_edges():
    f = _Fixture()
    before = f.table.snapshot(f.output)
    other_container = replace(f.edge, container_object_id=ObjectID.for_task(_id(TaskID, 7)))
    with pytest.raises(ValueError, match="published container"):
        f.publish(edges=(f.edge, other_container))
    assert f.table.snapshot(f.output) == before
    conflict = replace(f.edge, contained_owner_address=("another.invalid", 1234))
    with pytest.raises(ConflictingObjectResultError, match="conflicting edge"):
        f.publish(edges=(f.edge, conflict))
    assert f.table.snapshot(f.output) == before
    invalid = replace(f.descriptor)
    object.__setattr__(invalid, "checksum", "f" * 64)
    with pytest.raises(ProtocolError, match="checksum"):
        f.publish(descriptor=invalid)
    assert f.table.snapshot(f.output) == before


def test_put_replay_rejects_changed_owner_node_edges_and_never_revives_lost_bytes():
    for stored in (False, True):
        f = _Fixture(stored=stored)
        assert f.publish()
        before = f.table.snapshot(f.output)
        for descriptor in (replace(f.descriptor, owner_worker_id=_id(WorkerID, 8)),
                           replace(f.descriptor, node_id=_id(NodeID, 9))):
            with pytest.raises(ConflictingObjectResultError, match="put replay"):
                f.publish(descriptor=descriptor)
            assert f.table.snapshot(f.output) == before
        with pytest.raises(ConflictingObjectResultError, match="put replay"):
            f.publish(edges=())
        assert f.table.snapshot(f.output) == before
        if stored:
            assert f.table.remove_location(f.output, f.attempt, f.node)
            lost = f.table.snapshot(f.output)
            assert lost.state is ObjectState.LOST
            with pytest.raises(ConflictingObjectResultError, match="put replay"):
                f.publish()
            assert f.table.snapshot(f.output) == lost


def test_task_lineage_gc_claim_and_old_attempt_fence_put_publication():
    task = _Fixture(task_lineage=True)
    before = task.table.snapshot(task.output)
    with pytest.raises(InvalidObjectTransitionError, match="task lineage"):
        task.publish()
    assert task.table.snapshot(task.output) == before
    collected = _Fixture()
    assert collected.publish()
    assert collected.table.begin_collection(collected.output) is not None
    frozen = collected.table.snapshot(collected.output)
    with pytest.raises(ObjectCollectionInProgressError):
        collected.publish()
    assert collected.table.snapshot(collected.output) == frozen
    stale = _Fixture()
    assert stale.table.advance_attempt(
        stale.output, expected_attempt=stale.attempt, next_attempt=stale.attempt.next(),
    )
    current = stale.table.snapshot(stale.output)
    assert not stale.publish()
    assert stale.table.snapshot(stale.output) == current


def test_ready_assignment_observes_complete_put_edges_and_identity():
    f = _Fixture()
    observations = []
    # Observe the real entry assignment while its owner lock is held. This
    # checks that READY cannot precede the child obligations in the CAS.
    entry = f.table._entries[f.output]
    original = type(entry)

    class ObserveReady(original):
        def __setattr__(self, name, value):
            if name == "state" and value is ObjectState.READY_INLINE:
                observations.append((self.inline_data, frozenset(self.outgoing_contained_edges), self.put_identity))
            super().__setattr__(name, value)

    entry.__class__ = ObserveReady
    assert f.publish()
    assert len(observations) == 1
    payload, edges, identity = observations[0]
    assert payload == f.descriptor.inline_data and edges == frozenset({f.edge})
    assert identity is not None
