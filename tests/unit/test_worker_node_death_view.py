"""Threadless Node -> embedded-Core death-view and lazy Worker contracts.

One bare Node, at most two threadless owner Cores, two one-byte stored-result
metadata records, and three Node identities per case. Node publish/get are
the actual handlers; certificates explicitly model an already-obtained
Driver/GCS/survivor barrier, not an actual process exit or network ACK.

No Core/Node/Worker constructor, ObjectStore, socket, process, thread, timer
or wait runs. FakeCore is used only to audit lazy construction/cache ordering.
All progress/replays are fixed calls; the existing coordinator is never run.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import hashlib
import queue
import threading
from types import SimpleNamespace

import pytest

from miniray import core as core_module, protocol, worker as worker_module
from miniray.core import CoreWorker, _HomeRoute, _NodeDeathObserved, _ObjectWaiter
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.node import NodeServer
from miniray.node_death_view import (
    GET_NODE_DEATH_VIEW, GetInstalledNodeDeaths, GetInstalledNodeDeathsReply,
    PublishInstalledNodeDeaths, PublishInstalledNodeDeathsReply,
)
from miniray.ownership import ObjectState
from miniray.resources import ResourceLedger
from miniray.trace import MemoryEventSink, NonOwningEventSink
from miniray.worker import WorkerServer
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit.test_node_death_view import _death, _id, _live, _no_runtime as _no_runtime, _view


pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _no_constructors(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("death-view composition used a real runtime constructor")

    for kind in (CoreWorker, NodeServer, WorkerServer):
        monkeypatch.setattr(kind, "__init__", forbidden)


def _node(view):
    node = object.__new__(NodeServer)
    info = view.snapshot.nodes[0]
    node.node_id = info.node_id
    node._node_pid = info.node_pid
    node._registration_epoch = info.registration_epoch
    node._server = SimpleNamespace(address=info.address)
    node._state_lock = threading.RLock()
    node._stop_event = threading.Event()
    node._shutdown_request_id = None
    node._ledger = ResourceLedger(info.total_resources)
    node._installed_membership_epoch = node._membership_epoch = 0
    node._cluster_snapshot_id = None
    node._installed_snapshot_nodes = None
    node._certified_node_deaths = None
    _install_snapshot(node, view.snapshot)
    return node


def _install_snapshot(node, snapshot):
    reply = node._handle_install_cluster_snapshot(snapshot)
    assert type(reply) is protocol.InstallClusterSnapshotReply and reply.installed
    assert (reply.node_id, reply.membership_epoch, reply.snapshot_id) == (node.node_id, snapshot.membership_epoch, snapshot.snapshot_id)


def _publish(node, view):
    request = PublishInstalledNodeDeaths(node.node_id, view)
    reply = node._handle_publish_installed_node_deaths(request)
    assert type(reply) is PublishInstalledNodeDeathsReply and reply.request == request and reply.accepted
    return reply


def _get(node):
    query = GetInstalledNodeDeaths(node.node_id)
    reply = node._handle_get_installed_node_deaths(query)
    assert type(reply) is GetInstalledNodeDeathsReply and reply.request == query
    return reply


def _core(node):
    core = make_pure_core()
    core.node_id, core.node_address = node.node_id, node.address
    # Exactly the state initialized for poll_node_deaths=True, without threads.
    core._poll_node_deaths = True
    core._node_death_view_sync_lock = threading.Lock()
    core._node_death_next_poll_at = 0.0
    core._applied_node_death_view = None
    core._membership_epoch = 0
    core._installed_cluster_snapshot = None
    core._home_route = _HomeRoute(node.node_id, node.address, 0)
    core._node_death_removals = {}
    core._delayed_ready = queue.PriorityQueue()
    calls = []

    def read(request):
        assert not core._state_lock._is_owned() and not node._state_lock._is_owned()
        assert type(request) is GetInstalledNodeDeaths and request.node_id == node.node_id
        calls.append(request)
        assert len(calls) <= 8
        return node._handle_get_installed_node_deaths(request)

    core._node_death_view_rpc = read
    return core, calls


def _stored(core, node_id, byte=9):
    task_id = TaskID(bytes((byte,)) * 16)
    object_id, attempt = ObjectID.for_task(task_id), AttemptID(task_id, 0)
    descriptor = protocol.ResultDescriptor(
        object_id, protocol.ResultStorage.OBJECT_STORE, 1, core.worker_id, node_id, hashlib.sha256(b"x").hexdigest(),
    )
    core.owner_table.register(object_id, current_attempt=attempt, local_token="test-live-source")
    assert core.owner_table.publish_stored(object_id, attempt, node_id, descriptor=descriptor)
    core._recovery.register_put(object_id)
    core._stored_descriptors[object_id] = descriptor
    # A notification-only waiter lets the effect-then-error test observe wake.
    core._objects[object_id] = _ObjectWaiter(threading.Event())
    return object_id, descriptor


def _state(core):
    return deepcopy((
        tuple(core.owner_table.snapshot(object_id) for object_id in core._objects),
        core._stored_descriptors, core._dead_nodes, core._membership_epoch, core._installed_cluster_snapshot,
        core._home_route, core._applied_node_death_view, core._node_death_removals,
        tuple(core._submissions.queue), tuple(waiter.event.is_set() for waiter in core._objects.values()),
    ))


def _close(core):
    for object_id in core._objects:
        assert core.owner_table.release_local_reference(object_id, "test-live-source")
    close_pure_core(core)


def _events(core):
    events = tuple(core._submissions.queue)
    assert all(type(event) is _NodeDeathObserved for event in events)
    assert len(events) <= 3
    return events


def test_driver_default_and_explicit_disabled_sync_never_query_local_node():
    core = make_pure_core()

    def forbidden(*_args):
        pytest.fail("Driver/default Core unexpectedly queried embedded death view")

    core._node_death_view_rpc = forbidden
    try:
        assert not hasattr(core, "_poll_node_deaths") and core._sync_node_deaths()
        core._poll_node_deaths_best_effort()
        core._poll_node_deaths = False
        assert core._sync_node_deaths()
        core._poll_node_deaths_best_effort()
        assert not core._dead_nodes and core._submissions.empty()
    finally:
        close_pure_core(core)


def test_live_embedded_owner_consumes_actual_node_view_and_replay_is_idempotent():
    view = _view()
    node = _node(view)
    core, calls = _core(node)
    object_id, descriptor = _stored(core, view.deaths[0].node_id)
    _publish(node, view)
    try:
        assert core._sync_node_deaths()
        snapshot = core.owner_table.snapshot(object_id)
        assert snapshot.state is ObjectState.LOST and not snapshot.locations
        assert snapshot.canonical_stored_result == descriptor and snapshot.local_tokens == frozenset(("test-live-source",))
        assert object_id not in core._stored_descriptors and core._objects[object_id].event.is_set()
        assert core._dead_nodes == {view.deaths[0].node_id: view.deaths[0]}
        assert _events(core) == (_NodeDeathObserved(view.deaths[0], view.snapshot.membership_epoch),)
        assert core._installed_cluster_snapshot == view.snapshot and core._applied_node_death_view == view
        before = _state(core)
        assert core._sync_node_deaths() and _state(core) == before and len(calls) == 2
        # Returning a query DTO cannot expose mutable aliases into Node/Core.
        reply = _get(node)
        object.__setattr__(reply.view.deaths[0], "exit_code", 17)
        assert _get(node).view == view and core._applied_node_death_view == view
    finally:
        _close(core)


def test_surviving_copy_replaces_fetch_route_without_rewriting_canonical_descriptor():
    view = _view()
    node = _node(view)
    core, _calls = _core(node)
    object_id, descriptor = _stored(core, view.deaths[0].node_id)
    assert core.owner_table.add_location(object_id, AttemptID(object_id.task_id, 0), node.node_id,
                                         descriptor=replace(descriptor, node_id=node.node_id))
    _publish(node, view)
    try:
        assert core._sync_node_deaths()
        snapshot = core.owner_table.snapshot(object_id)
        assert snapshot.state is ObjectState.READY_STORED and snapshot.locations == frozenset((node.node_id,))
        assert snapshot.canonical_stored_result == descriptor
        assert core._stored_descriptors[object_id] == replace(descriptor, node_id=node.node_id)
        assert len(_events(core)) == 1
    finally:
        _close(core)


@pytest.mark.parametrize("case", ("wrong-type", "wrong-request", "partial-acks", "malformed-death"))
def test_invalid_local_reply_cannot_mutate_owner_or_install_partial_death(case):
    view = _view()
    node = _node(view)
    core, _calls = _core(node)
    _stored(core, view.deaths[0].node_id)
    _publish(node, view)
    reply = _get(node)
    if case == "wrong-type":
        reply = SimpleNamespace(request=reply.request, view=reply.view)
    elif case == "wrong-request":
        object.__setattr__(reply, "request", GetInstalledNodeDeaths(_id(2)))
    elif case == "partial-acks":
        object.__setattr__(reply.view, "survivor_acks", reply.view.survivor_acks[:1])
    else:
        object.__setattr__(reply.view.deaths[0], "node_pid", 0)
    core._node_death_view_rpc = lambda _request: reply
    try:
        before = _state(core)
        assert not core._sync_node_deaths() and _state(core) == before
        assert _get(node).view == view
    finally:
        _close(core)


def test_lost_get_reply_keeps_node_custody_and_next_poll_applies_same_view():
    view = _view()
    node = _node(view)
    core, calls = _core(node)
    object_id, _descriptor = _stored(core, view.deaths[0].node_id)
    _publish(node, view)
    original, lost = core._node_death_view_rpc, []

    def read_then_lose(request):
        reply = original(request)
        if not lost:
            lost.append(reply)
            raise TimeoutError("local query response lost after Node read")
        return reply

    core._node_death_view_rpc = read_then_lose
    try:
        before = _state(core)
        assert not core._sync_node_deaths() and _state(core) == before
        assert lost[0].view == _get(node).view == view
        assert core._sync_node_deaths() and len(calls) == 2
        assert core.owner_table.snapshot(object_id).state is ObjectState.LOST
        assert len(_events(core)) == 1
    finally:
        _close(core)


def test_empty_local_history_is_not_permission_to_forget_a_previously_applied_view():
    view = _view()
    node = _node(view)
    core, calls = _core(node)
    _stored(core, view.deaths[0].node_id)
    try:
        before = _state(core)
        assert core._sync_node_deaths() and _state(core) == before and _get(node).view is None
        _publish(node, view)
        assert core._sync_node_deaths() and len(calls) == 2
        committed = _state(core)
        core._node_death_view_rpc = lambda request: GetInstalledNodeDeathsReply(request)
        assert not core._sync_node_deaths() and _state(core) == committed
    finally:
        _close(core)


def test_same_epoch_late_fact_is_consumed_and_lazy_new_owner_receives_both():
    earlier = _view(epoch=8, survivors=(1,), deaths=(_death(3, 6),))
    additional = replace(earlier, deaths=(_death(2, 4), earlier.deaths[0]))
    node = _node(earlier)
    core, calls = _core(node)
    first, _ = _stored(core, _id(3), 9)
    second, _ = _stored(core, _id(2), 10)
    lazy, lazy_calls = _core(node)
    try:
        _publish(node, earlier)
        assert core._sync_node_deaths()
        assert core.owner_table.snapshot(first).state is ObjectState.LOST
        assert core.owner_table.snapshot(second).state is ObjectState.READY_STORED
        _publish(node, additional)
        assert core._sync_node_deaths() and len(calls) == 2
        assert core.owner_table.snapshot(second).state is ObjectState.LOST
        assert tuple(event.death for event in _events(core)) == (earlier.deaths[0], additional.deaths[0])
        assert core._applied_node_death_view == additional
        assert lazy._sync_node_deaths() and len(lazy_calls) == 1
        assert tuple(event.death for event in _events(lazy)) == additional.deaths
        assert lazy._dead_nodes == core._dead_nodes
    finally:
        _close(core)
        close_pure_core(lazy)


def test_node_does_not_certify_a_new_snapshot_until_driver_publishes_its_full_barrier():
    view = _view()
    node = _node(view)
    _publish(node, view)
    future = _view(epoch=7, survivors=(1,), deaths=(view.deaths[0], _death(2, 6)))
    request = PublishInstalledNodeDeaths(node.node_id, future)
    refused = node._handle_publish_installed_node_deaths(request)
    assert not refused.accepted and _get(node).view == view
    _install_snapshot(node, future.snapshot)
    assert _get(node).view == view  # Its new scheduling view is not a certificate.
    _publish(node, future)
    assert _get(node).view == future
    replay = node._handle_publish_installed_node_deaths(PublishInstalledNodeDeaths(node.node_id, view))
    assert not replay.accepted and _get(node).view == future
    node._shutdown_request_id = "real-drain-state"
    assert _publish(node, future).accepted and _get(node).view == future


def test_node_rejects_wrong_target_and_tampered_ack_before_changing_retained_history():
    view = _view()
    node = _node(view)
    original = _publish(node, view)
    wrong = PublishInstalledNodeDeaths(_id(2), view)
    refused = node._handle_publish_installed_node_deaths(wrong)
    assert type(refused) is PublishInstalledNodeDeathsReply and refused.request == wrong and not refused.accepted
    invalid = PublishInstalledNodeDeaths(node.node_id, view)
    object.__setattr__(invalid.view, "survivor_acks", invalid.view.survivor_acks[:1])
    with pytest.raises((TypeError, ValueError, protocol.ProtocolError)):
        node._handle_publish_installed_node_deaths(invalid)
    assert _get(node).view == view and _publish(node, view) == original


def test_local_remove_effect_then_exception_replays_routes_wake_and_classification(monkeypatch):
    view = _view()
    node = _node(view)
    core, calls = _core(node)
    object_id, descriptor = _stored(core, view.deaths[0].node_id)
    _publish(node, view)
    original, removals = core.owner_table.remove_node_locations, []

    def remove_then_fail(node_id):
        delta = original(node_id)
        removals.append(delta)
        assert len(removals) <= 2
        if len(removals) == 1:
            raise RuntimeError("owner removed location before local callback failed")
        return delta

    monkeypatch.setattr(core.owner_table, "remove_node_locations", remove_then_fail)
    try:
        assert not core._sync_node_deaths()
        assert core.owner_table.snapshot(object_id).state is ObjectState.LOST
        assert core._stored_descriptors[object_id] == descriptor and not core._objects[object_id].event.is_set()
        assert core._applied_node_death_view is None and core._node_death_removals
        assert not _events(core)
        assert core._sync_node_deaths() and len(calls) == len(removals) == 2
        assert removals[0].lost == (object_id,) and removals[1].lost == ()
        assert object_id not in core._stored_descriptors and core._objects[object_id].event.is_set()
        assert not core._node_death_removals and core._applied_node_death_view == view
        assert _events(core) == (_NodeDeathObserved(view.deaths[0], view.snapshot.membership_epoch),)
        assert core._sync_node_deaths() and len(removals) == 2
    finally:
        _close(core)


def test_best_effort_poll_and_coordinator_timeout_share_bounded_existing_wakeup(monkeypatch):
    view = _view()
    node = _node(view)
    core, calls = _core(node)
    _publish(node, view)
    monkeypatch.setattr(core_module.time, "monotonic", lambda: 10.0)
    try:
        core._node_death_next_poll_at = 11.0
        core._poll_node_deaths_best_effort()
        assert not calls and core._next_coordinator_timeout() == 1.0
        core._node_death_next_poll_at = 10.0
        core._poll_node_deaths_best_effort()
        assert len(calls) == 1 and core._node_death_next_poll_at > 10.0
        assert 0 < core._next_coordinator_timeout() <= 1.0
        core._poll_node_deaths_best_effort()
        assert len(calls) == 1
    finally:
        close_pure_core(core)


def test_node_view_rpc_is_local_and_has_one_short_absolute_deadline(monkeypatch):
    view = _view()
    node = _node(view)
    core, _calls = _core(node)
    request = GetInstalledNodeDeaths(node.node_id)
    calls = []
    monkeypatch.setattr(core_module.time, "monotonic", lambda: 10.0)

    def rpc(address, handler, message, **options):
        calls.append((address, handler, message, options))
        assert address == core.node_address and handler == GET_NODE_DEATH_VIEW and message == request
        assert options == {"connect_timeout": 0.25, "request_timeout": 0.5, "deadline": 10.75}
        return node._handle_get_installed_node_deaths(message)

    monkeypatch.setattr(core_module, "rpc_request", rpc)
    try:
        assert CoreWorker._node_death_view_rpc(core, request) == GetInstalledNodeDeathsReply(request)
        assert len(calls) == 1
    finally:
        close_pure_core(core)


@pytest.mark.parametrize("bootstrap", (False, True))
def test_lazy_worker_core_syncs_before_exposure_or_aborts_without_caching(monkeypatch, bootstrap):
    worker = object.__new__(WorkerServer)
    worker.node_id, worker.node_address = _id(1), _live(1).address
    worker.worker_id = WorkerID(b"w" * 16)
    worker.gcs_address, worker.inline_threshold = ("gcs.invalid", 1), 1024
    worker._embedded_core_lock = threading.Lock()
    worker._embedded_core_stopped = False
    worker._embedded_core = worker._embedded_core_job_id = None
    worker._owner_retain_admission_open = False
    worker._server = SimpleNamespace(address=("owner.invalid", 1))
    worker.event_sink = MemoryEventSink()
    job = JobID(b"j" * 16)
    created, events = [], []

    class FakeCore:
        def __init__(self, address, node_id, **options):
            assert address == worker.node_address and node_id == worker.node_id
            assert options["poll_node_deaths"] is True and options["dispatch_lanes"] == 1
            assert options["job_id"] == job and options["worker_id"] == worker.worker_id
            assert options["owner_address"] == worker._server.address and options["gcs_address"] == worker.gcs_address
            assert options["inline_threshold"] == 1024
            assert isinstance(options["event_sink"], NonOwningEventSink)
            assert options["event_sink"].sink is worker.event_sink
            created.append(self)
            events.append("construct")

        def _sync_node_deaths(self):
            assert worker._embedded_core is None and worker._embedded_core_job_id is None
            events.append("sync")
            return bootstrap

        def close_owner_retain_admission(self):
            assert worker._embedded_core is None
            events.append("close-retain")

        def _abort_unpublished_startup(self):
            assert worker._embedded_core is None and worker._embedded_core_job_id is None
            events.append("abort")

    monkeypatch.setattr(worker_module, "CoreWorker", FakeCore)
    if bootstrap:
        core = worker._embedded_core_for(job)
        assert worker._embedded_core is core is created[0] and worker._embedded_core_job_id == job
        assert worker._embedded_core_for(job) is core
        assert events == ["construct", "sync", "close-retain"] and len(created) == 1
    else:
        with pytest.raises(RuntimeError, match="certified local Node deaths"):
            worker._embedded_core_for(job)
        assert events == ["construct", "sync", "abort"] and len(created) == 1
        assert worker._embedded_core is worker._embedded_core_job_id is None
