"""Pure wire contracts for per-container GC of a shared batch graph."""

from __future__ import annotations

import hashlib
import pickle
from dataclasses import replace

import pytest

from miniray import protocol
from miniray.contained_cycle import (
    ContainedGraphManifest, ContainedGraphManifestDisposition as Disposition,
    ContainedGraphManifestReceipt, ContainedGraphTransactionState as State,
    ContainedReferenceGraphAuthority,
)
from miniray.contained_edges import ContainedReferenceEdge
from miniray.errors import ProtocolError
from miniray.ids import JobID, ObjectID, TaskID, WorkerID


pytestmark = pytest.mark.unit


def _fixture():
    job = JobID(bytes.fromhex("11" * 16))
    task = TaskID.derive(job, TaskID.for_driver(job), 1)
    owner = WorkerID(bytes.fromhex("22" * 16))
    child_owner = WorkerID(bytes.fromhex("33" * 16))
    first, second, empty = tuple(ObjectID.for_task(task, index) for index in range(3))
    child = ObjectID.for_task(TaskID.derive(job, task, 2))
    other_child = ObjectID.for_task(TaskID.derive(job, task, 3))
    first_edges = (
        ContainedReferenceEdge(first, child, child_owner, ("127.0.0.1", 30301), "first-a"),
        ContainedReferenceEdge(first, other_child, child_owner, ("127.0.0.1", 30301), "first-b"),
    )
    second_edges = (
        ContainedReferenceEdge(second, child, child_owner, ("127.0.0.1", 30301), "second"),
    )
    # Generic graph manifests preserve declared order, even when container
    # edges are not adjacent.  Filtering must never sort or truncate it.
    graph = ContainedGraphManifest(
        "multi-container-graph", ("batch", task), owner,
        hashlib.sha256(b"multi-container-manifest").hexdigest(),
        (first_edges[0], second_edges[0], first_edges[1]),
    )
    return graph, first, second, empty, first_edges, second_edges


def _receipt(graph, edges, *, replay=False):
    return ContainedGraphManifestReceipt(
        graph, State.COMMITTED, Disposition.ALREADY_RELEASED if replay else Disposition.RELEASED,
        edges,
    )


def test_two_containers_release_independently_under_one_manifest_identity():
    graph, first, second, _empty, first_edges, second_edges = _fixture()
    authority = ContainedReferenceGraphAuthority()
    authority.prepare_manifest(graph)
    authority.commit_manifest(graph)
    first_request = protocol.ReleaseContainedGraphContainer(graph, first)
    assert first_request.manifest is graph
    released = protocol.ContainedGraphReply(
        first_request, authority.release_manifest_container(graph, first)
    )
    assert released.accepted
    assert released.receipt.manifest == graph
    assert released.receipt.released_edges == first_edges
    (remaining,) = authority.snapshot().manifests
    assert remaining.active_edges == second_edges
    replay = protocol.ContainedGraphReply(
        first_request, authority.release_manifest_container(graph, first)
    )
    assert replay.accepted
    assert replay.receipt.disposition is Disposition.ALREADY_RELEASED
    assert replay.receipt.released_edges == first_edges
    second_request = protocol.ReleaseContainedGraphContainer(graph, second)
    second_reply = protocol.ContainedGraphReply(
        second_request, authority.release_manifest_container(graph, second)
    )
    assert second_reply.receipt.released_edges == second_edges
    assert authority.snapshot().manifests[0].active_edges == ()


def test_request_rejects_absent_or_empty_container_without_projecting_manifest():
    graph, first, _second, empty, _first_edges, _second_edges = _fixture()
    for manifest, object_id in ((graph, empty), (replace(graph, ordered_edges=()), first)):
        with pytest.raises(ProtocolError, match="at least one"):
            protocol.ReleaseContainedGraphContainer(manifest, object_id)
    with pytest.raises(ProtocolError, match="ObjectID"):
        protocol.ReleaseContainedGraphContainer(graph, "first")


def test_reply_cannot_accept_empty_release_for_a_forged_absent_container():
    graph, first, _second, empty, first_edges, _second_edges = _fixture()
    request = protocol.ReleaseContainedGraphContainer(graph, first)
    receipt = _receipt(graph, first_edges)
    object.__setattr__(request, "container_object_id", empty)
    object.__setattr__(receipt, "released_edges", ())
    with pytest.raises(ProtocolError, match="exact mutation request"):
        protocol.ContainedGraphReply(request, receipt)


@pytest.mark.parametrize("replay", (False, True))
def test_release_reply_rejects_partial_subset_wrong_container_and_projected_manifest(replay):
    graph, first, _second, _empty, first_edges, second_edges = _fixture()
    request = protocol.ReleaseContainedGraphContainer(graph, first)
    for edges in (first_edges[:1], second_edges):
        with pytest.raises(ProtocolError, match="exact mutation request"):
            protocol.ContainedGraphReply(request, _receipt(graph, edges, replay=replay))
    projected = replace(graph, ordered_edges=first_edges)
    with pytest.raises(ProtocolError, match="exact manifest"):
        protocol.ContainedGraphReply(request, _receipt(projected, first_edges, replay=replay))


@pytest.mark.parametrize("replay", (False, True))
@pytest.mark.parametrize("bad_edges", ("all", "empty", "reversed", "duplicate"))
def test_reply_boundary_rejects_malformed_release_vectors_even_if_receipt_was_forged(replay, bad_edges):
    graph, first, _second, _empty, first_edges, _second_edges = _fixture()
    request = protocol.ReleaseContainedGraphContainer(graph, first)
    receipt = _receipt(graph, first_edges, replay=replay)
    # Simulate skipped nested dataclass validation without relaxing its model.
    value = {
        "all": graph.ordered_edges, "empty": (),
        "reversed": first_edges[::-1], "duplicate": first_edges + first_edges[:1],
    }[bad_edges]
    object.__setattr__(receipt, "released_edges", value)
    with pytest.raises(ProtocolError, match="exact mutation request"):
        protocol.ContainedGraphReply(request, receipt)


def test_release_request_keeps_existing_wire_aliases_and_pickle_validation():
    graph, first, _second, _empty, first_edges, _second_edges = _fixture()
    request = protocol.ReleaseContainedGraphContainer(graph, first)
    assert type(request) is protocol.ReleaseContainedGraphContainer
    assert pickle.loads(pickle.dumps(request)) == request
    reply = protocol.ContainedGraphReply(request, _receipt(graph, first_edges))
    assert type(reply) is protocol.ContainedGraphReply
    assert pickle.loads(pickle.dumps(reply)) == reply
    object.__setattr__(reply.receipt, "released_edges", first_edges[:1])
    with pytest.raises(ProtocolError, match="exact mutation request"):
        pickle.loads(pickle.dumps(reply))


def test_single_container_and_nonrelease_graph_protocol_semantics_are_unchanged():
    graph, first, _second, _empty, first_edges, _second_edges = _fixture()
    single = replace(graph, ordered_edges=first_edges)
    request = protocol.ReleaseContainedGraphContainer(single, first)
    for replay in (False, True):
        assert protocol.ContainedGraphReply(request, _receipt(single, first_edges, replay=replay)).accepted
    for request, state in (
        (protocol.PrepareContainedGraph(graph), State.PREPARED),
        (protocol.CommitContainedGraph(graph), State.COMMITTED),
        (protocol.AbortContainedGraph(graph), State.ABORTED),
    ):
        assert protocol.ContainedGraphReply(
            request, ContainedGraphManifestReceipt(graph, state, Disposition.APPLIED)
        ).accepted
    with pytest.raises(ProtocolError, match="exact mutation request"):
        protocol.ContainedGraphReply(
            protocol.ReleaseContainedGraphContainer(single, first),
            ContainedGraphManifestReceipt(single, State.COMMITTED, Disposition.APPLIED),
        )
