"""Pure identity contracts for the existing installed-survivor certificate.

At most three Node identities, two live NodeInfo values and two death records
per case. Constructors, equality, successor validation, replace and pickle
only: no Node/Core/Worker instance, ObjectStore, sockets, threads or waits.
The records model already-obtained authority; these tests do not claim that
constructing a DTO verifies an actual process exit or an actual remote ACK.
"""

from __future__ import annotations

from dataclasses import fields, replace
import multiprocessing.process
import pickle
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.ids import NodeID, WorkerID
from miniray.node_death_view import (
    GET_NODE_DEATH_VIEW, PUBLISH_NODE_DEATH_VIEW, GetInstalledNodeDeaths,
    GetInstalledNodeDeathsReply, InstalledNodeDeathView, PublishInstalledNodeDeaths,
    PublishInstalledNodeDeathsReply,
)
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit
_INVALID = (TypeError, ValueError, protocol.ProtocolError)


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("Node-death view protocol test attempted runtime infrastructure")

    for kind, method in (
        (threading.Thread, "start"), (threading.Thread, "join"),
        (threading.Timer, "start"), (threading.Event, "wait"),
        (threading.Condition, "wait"),
        (multiprocessing.process.BaseProcess, "start"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    for method in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, method, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _id(byte):
    return NodeID(bytes((byte,)) * 16)


def _live(byte):
    return protocol.NodeInfo(
        _id(byte), 1700 + byte, byte, ("127.0.0.1", 17000 + byte),
        ResourceVector({"CPU": 1}), ResourceVector({"CPU": 1}),
    )


def _death(byte=3, epoch=4):
    return protocol.NodeDeathRecord(
        "node-exit-{}".format(byte), _id(byte), 1700 + byte, byte, epoch,
        -9, protocol.NodeDeathReason.PROCESS_EXIT, "managed process exit",
    )


def _parts(*, epoch=5, survivors=(1, 2), deaths=None):
    nodes = tuple(_live(byte) for byte in survivors)
    snapshot = protocol.InstallClusterSnapshot(epoch, "snapshot-{}".format(epoch), nodes)
    acknowledgements = tuple(protocol.InstallClusterSnapshotReply(
        epoch, snapshot.snapshot_id, node.node_id, True,
    ) for node in nodes)
    return snapshot, acknowledgements, (_death(),) if deaths is None else tuple(deaths)


def _view(**kwargs):
    return InstalledNodeDeathView(*_parts(**kwargs))


def _message(kind):
    view = _view()
    request = PublishInstalledNodeDeaths(_id(1), view)
    query = GetInstalledNodeDeaths(_id(1))
    return {
        "view": view, "publish": request,
        "publish-ack": PublishInstalledNodeDeathsReply(request, True),
        "get": query, "get-reply": GetInstalledNodeDeathsReply(query, view),
        "get-empty": GetInstalledNodeDeathsReply(query),
    }[kind]


@pytest.mark.parametrize("kind", ("view", "publish", "publish-ack", "get", "get-reply", "get-empty"))
def test_wire_roundtrip_preserves_exact_schema_and_certificate(kind):
    message = _message(kind)
    expected = {
        "view": ("snapshot", "survivor_acks", "deaths"),
        "publish": ("node_id", "view"),
        "publish-ack": ("request", "accepted", "error"),
        "get": ("node_id",), "get-reply": ("request", "view"), "get-empty": ("request", "view"),
    }[kind]
    assert tuple(field.name for field in fields(message)) == expected
    assert replace(message) == pickle.loads(pickle.dumps(message)) == message
    assert GET_NODE_DEATH_VIEW != PUBLISH_NODE_DEATH_VIEW


def test_constructor_detaches_node_death_ack_and_resource_inputs():
    snapshot, acks, deaths = _parts()
    view = InstalledNodeDeathView(snapshot, list(acks), list(deaths))
    saved = pickle.loads(pickle.dumps(view))
    assert view.snapshot is not snapshot and view.snapshot.nodes[0] is not snapshot.nodes[0]
    assert view.survivor_acks[0] is not acks[0] and view.deaths[0] is not deaths[0]
    assert view.snapshot.nodes[0].total_resources is not snapshot.nodes[0].total_resources
    object.__setattr__(snapshot.nodes[0].node_id, "value", b"x" * 16)
    object.__setattr__(snapshot.nodes[0].total_resources, "_items", (("CPU", 9000),))
    object.__setattr__(acks[0], "snapshot_id", "mutated-after-send")
    object.__setattr__(deaths[0], "exit_code", 23)
    object.__setattr__(deaths[0].node_id, "value", b"y" * 16)
    assert view == saved


@pytest.mark.parametrize("kind", ("publish", "publish-ack", "get-reply"))
def test_outward_message_does_not_alias_canonical_certificate(kind):
    view = _view()
    saved = pickle.loads(pickle.dumps(view))
    publish = PublishInstalledNodeDeaths(_id(1), view)
    message = {
        "publish": publish,
        "publish-ack": PublishInstalledNodeDeathsReply(publish, True),
        "get-reply": GetInstalledNodeDeathsReply(GetInstalledNodeDeaths(_id(1)), view),
    }[kind]
    exposed = message.request.view if kind == "publish-ack" else message.view
    object.__setattr__(exposed.deaths[0], "exit_code", 31)
    object.__setattr__(exposed.snapshot.nodes[0].node_id, "value", b"z" * 16)
    object.__setattr__(exposed.survivor_acks[0].node_id, "value", b"q" * 16)
    assert view == saved
    if kind == "publish-ack":
        assert publish.view == saved


@pytest.mark.parametrize("case", ("missing", "duplicate", "reversed", "wrong-node", "epoch", "snapshot", "rejected", "truthy"))
def test_only_every_exact_survivor_ack_can_form_the_barrier(case):
    snapshot, acks, deaths = _parts()
    if case == "missing":
        acks = acks[:1]
    elif case == "duplicate":
        acks = (acks[0], acks[0])
    elif case == "reversed":
        acks = acks[::-1]
    elif case == "wrong-node":
        acks = (acks[0], replace(acks[1], node_id=_id(3)))
    elif case == "epoch":
        acks = (replace(acks[0], membership_epoch=6), acks[1])
    elif case == "snapshot":
        acks = (replace(acks[0], snapshot_id="another-snapshot"), acks[1])
    elif case == "rejected":
        acks = (replace(acks[0], installed=False, error="not installed"), acks[1])
    else:
        object.__setattr__(acks[0], "installed", 1)
    with pytest.raises(_INVALID):
        InstalledNodeDeathView(snapshot, acks, deaths)


@pytest.mark.parametrize("case", ("noncanonical", "duplicate", "dead-member", "over-capacity", "not-snapshot"))
def test_certified_snapshot_must_be_canonical_and_revalidate_every_live_node(case):
    snapshot, acks, deaths = _parts()
    if case == "noncanonical":
        snapshot = replace(snapshot, nodes=snapshot.nodes[::-1])
        acks = acks[::-1]  # Even a matching reversed ACK order is not canonical.
    elif case == "duplicate":
        object.__setattr__(snapshot, "nodes", (snapshot.nodes[0], snapshot.nodes[0]))
        acks = (acks[0], acks[0])
    elif case == "dead-member":
        object.__setattr__(snapshot.nodes[0], "state", protocol.NodeMembershipState.DEAD)
    elif case == "over-capacity":
        object.__setattr__(snapshot.nodes[0], "available_resources", ResourceVector({"CPU": 2}))
    else:
        snapshot = snapshot.nodes
    with pytest.raises(_INVALID):
        InstalledNodeDeathView(snapshot, acks, deaths)


@pytest.mark.parametrize("case", ("empty", "duplicate-node", "duplicate-epoch", "duplicate-detection", "unordered", "future", "live-node", "worker-death", "missing-proof"))
def test_death_vector_is_exact_unique_ordered_and_never_inferred(case):
    snapshot, acks, deaths = _parts(survivors=(1,), epoch=8)
    if case == "empty":
        deaths = ()
    elif case == "duplicate-node":
        deaths = (deaths[0], replace(deaths[0], detection_id="another-detection", death_epoch=6))
    elif case == "duplicate-epoch":
        deaths = (deaths[0], _death(2, 4))
    elif case == "duplicate-detection":
        deaths = (deaths[0], replace(_death(2, 6), detection_id=deaths[0].detection_id))
    elif case == "unordered":
        deaths = (_death(2, 6), deaths[0])
    elif case == "future":
        deaths = (_death(3, 9),)
    elif case == "live-node":
        deaths = (_death(1, 4),)
    elif case == "worker-death":
        deaths = (protocol.WorkerDeathRecord(
            "worker-node-exit", protocol.WorkerIncarnation(_id(3), 1703, 3, WorkerID(b"w" * 16), 1803),
            4, -9, protocol.WorkerDeathReason.NODE_EXIT,
        ),)
    else:
        deaths = (None,)
    with pytest.raises(_INVALID):
        InstalledNodeDeathView(snapshot, acks, deaths)


def test_same_epoch_can_add_a_late_death_fact_without_replacing_snapshot_or_old_fact():
    # B and C were absent already. The GCS response for B arrives later.
    earlier = _view(epoch=8, survivors=(1,), deaths=(_death(3, 6),))
    additional = replace(earlier, deaths=(_death(2, 4), earlier.deaths[0]))
    assert additional.validate_successor(earlier) is None
    assert additional.snapshot == earlier.snapshot and additional.survivor_acks == earlier.survivor_acks
    assert earlier.validate_successor(earlier) is None
    assert additional.validate_successor(None) is None
    assert tuple(item.death_epoch for item in additional.deaths) == (4, 6)  # Not a gap-free Worker cursor.
    with pytest.raises(ValueError, match="earlier death"):
        earlier.validate_successor(additional)


def test_successor_contracts_live_set_and_binds_removed_incarnation():
    previous = _view()
    survivor = replace(previous.snapshot.nodes[0], available_resources=ResourceVector())
    parts = _parts(epoch=7, survivors=(1,), deaths=(previous.deaths[0], _death(2, 6)))
    current = InstalledNodeDeathView(replace(parts[0], nodes=(survivor,)), parts[1], parts[2])
    saved = pickle.loads(pickle.dumps(previous))
    assert current.validate_successor(previous) is None
    assert previous == saved and current.deaths[0] == previous.deaths[0]
    # Availability changes across a new epoch, unlike Node capacity/identity.
    assert current.snapshot.nodes[0].available_resources == ResourceVector()


@pytest.mark.parametrize("case", ("regressed", "same-epoch-snapshot", "lost-death", "rebound-death",
                                  "added-live", "live-pid", "live-epoch", "live-route", "live-capacity",
                                  "missing-removed-proof", "wrong-removed-pid", "wrong-removed-epoch"))
def test_invalid_successor_does_not_mutate_either_certificate(case):
    previous = _view()
    if case == "regressed":
        current = _view(epoch=4)
    elif case == "same-epoch-snapshot":
        parts = _parts()
        current = InstalledNodeDeathView(
            replace(parts[0], snapshot_id="same-epoch-rebound"),
            tuple(replace(ack, snapshot_id="same-epoch-rebound") for ack in parts[1]), parts[2],
        )
    elif case == "lost-death":
        current = _view(epoch=7, survivors=(1,), deaths=(_death(2, 6),))
    elif case == "rebound-death":
        current = _view(epoch=7, deaths=(replace(previous.deaths[0], exit_code=31),))
    elif case == "added-live":
        previous = _view(survivors=(1,))
        current = _view(epoch=7)
    elif case.startswith("live-"):
        parts = _parts(epoch=7)
        changes = {
            "live-pid": {"node_pid": 1901}, "live-epoch": {"registration_epoch": 7},
            "live-route": {"address": ("127.0.0.1", 17101)},
            "live-capacity": {"total_resources": ResourceVector({"CPU": 2})},
        }[case]
        changed = replace(parts[0].nodes[0], **changes)
        current = InstalledNodeDeathView(replace(parts[0], nodes=(changed, parts[0].nodes[1])), parts[1], parts[2])
    else:
        death = _death(2, 6)
        if case == "wrong-removed-pid":
            death = replace(death, node_pid=1902)
        elif case == "wrong-removed-epoch":
            death = replace(death, registration_epoch=8)
        deaths = (previous.deaths[0],) if case == "missing-removed-proof" else (previous.deaths[0], death)
        current = _view(epoch=7, survivors=(1,), deaths=deaths)
    originals = pickle.loads(pickle.dumps((previous, current)))
    with pytest.raises(ValueError):
        current.validate_successor(previous)
    assert (previous, current) == originals


def test_removed_node_cannot_have_died_before_its_previous_certified_alive_view():
    previous = _view(epoch=5)
    # Same PID/incarnation is insufficient: B was certified alive at epoch 5.
    current = _view(epoch=7, survivors=(1,), deaths=(_death(2, 3), previous.deaths[0]))
    with pytest.raises(ValueError):
        current.validate_successor(previous)


@pytest.mark.parametrize("field", ("node-id", "resource-units", "live-pid", "live-address",
                                  "snapshot-epoch", "ack-flag", "death-pid", "death-epoch"))
def test_nested_wire_corruption_is_rejected_on_replace_and_unpickle(field):
    view = _view()
    if field == "node-id":
        object.__setattr__(view.snapshot.nodes[0].node_id, "value", b"short")
    elif field == "resource-units":
        object.__setattr__(view.snapshot.nodes[0].total_resources, "_items", (("CPU", -1000),))
    elif field == "live-pid":
        object.__setattr__(view.snapshot.nodes[0], "node_pid", True)
    elif field == "live-address":
        object.__setattr__(view.snapshot.nodes[0], "address", ("127.0.0.1", 0))
    elif field == "snapshot-epoch":
        object.__setattr__(view.snapshot, "membership_epoch", True)
    elif field == "ack-flag":
        object.__setattr__(view.survivor_acks[0], "installed", 1)
    elif field == "death-pid":
        object.__setattr__(view.deaths[0], "node_pid", 0)
    else:
        object.__setattr__(view.deaths[0], "death_epoch", 6)
    with pytest.raises(_INVALID):
        replace(view)
    with pytest.raises(_INVALID):
        pickle.loads(pickle.dumps(view))


def test_exact_wrapper_types_and_query_origin_are_part_of_authority():
    view = _view()
    request = PublishInstalledNodeDeaths(_id(1), view)
    query = GetInstalledNodeDeaths(_id(1))
    assert GetInstalledNodeDeathsReply(query).view is None
    for node_id in (_id(3), WorkerID(b"w" * 16)):
        with pytest.raises(_INVALID):
            PublishInstalledNodeDeaths(node_id, view)
    with pytest.raises(ValueError):
        GetInstalledNodeDeathsReply(GetInstalledNodeDeaths(_id(3)), view)
    for value in (view.snapshot, view.deaths, object()):
        with pytest.raises(TypeError):
            PublishInstalledNodeDeaths(_id(1), value)
        with pytest.raises(TypeError):
            GetInstalledNodeDeathsReply(query, value)
    for accepted, error in ((1, None), (False, None), (False, ""), (True, "contradiction")):
        with pytest.raises(_INVALID):
            PublishInstalledNodeDeathsReply(request, accepted, error)
    rejected = PublishInstalledNodeDeathsReply(request, False, "conflicting certified view")
    assert pickle.loads(pickle.dumps(rejected)) == rejected


def test_subclass_payloads_cannot_stand_in_for_certificate_members():
    class DisguisedNode(protocol.NodeInfo):
        pass

    class DisguisedAck(protocol.InstallClusterSnapshotReply):
        pass

    class DisguisedDeath(protocol.NodeDeathRecord):
        pass

    snapshot, acks, deaths = _parts()
    node = DisguisedNode(*(getattr(snapshot.nodes[0], field.name) for field in fields(snapshot.nodes[0])))
    ack = DisguisedAck(*(getattr(acks[0], field.name) for field in fields(acks[0])))
    death = DisguisedDeath(*(getattr(deaths[0], field.name) for field in fields(deaths[0])))
    for parts in ((replace(snapshot, nodes=(node, snapshot.nodes[1])), acks, deaths),
                  (snapshot, (ack, acks[1]), deaths), (snapshot, acks, (death,))):
        with pytest.raises(TypeError):
            InstalledNodeDeathView(*parts)


def test_terminal_empty_live_set_is_valid_but_has_no_publish_or_query_target():
    view = _view(epoch=7, survivors=(), deaths=(_death(3, 4), _death(2, 6)))
    assert view.snapshot.nodes == view.survivor_acks == ()
    assert pickle.loads(pickle.dumps(view)) == view
    with pytest.raises(ValueError):
        PublishInstalledNodeDeaths(_id(1), view)
    with pytest.raises(ValueError):
        GetInstalledNodeDeathsReply(GetInstalledNodeDeaths(_id(1)), view)
