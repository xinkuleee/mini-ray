"""Unified GCS owner-death control, plus two explicitly bounded L1 checks.

Pure cases use one fake TCPServer, one Node, at most two owner publications,
two slots per publication and two small child owner tables. Public Worker-death
reports, owner-wide fence outboxes, child releases, graph cleanup and terminal
registry transitions are real reducers; transport replies are synchronous.

Only the two exact loopback_smoke parameters start a thread: the GCS's single
owner-death driver. They have one injected progress failure, bounded Event gates,
a three-second convergence bound and finally-based bounded stop/join. No
socket, subprocess, Actor, user task, payload transfer or test timer runs.
Legacy source and unported lifecycle cases are retained under
docs/history/retired-gcs-publication/test_publication_owner_death_control.py.txt.
"""

from __future__ import annotations

from dataclasses import replace
import multiprocessing as mp
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

from miniray import control, output_protocol as wire, protocol
from miniray.ids import AttemptID, LeaseID, TaskID, WorkerID
from miniray.output_publication import (
    OutputPublicationCompleteWitness, OutputPublicationID, OutputPublicationManifest,
    OutputSlotManifest,
)
from miniray.output_publication_journal import OutputPublicationAdoptionProof
from miniray.output_recovery import OutputRecoveryDisposition
from miniray.ownership import ObjectOwnerTable
from miniray.task_outputs import TaskExecutionKey, TaskOutputManifest
from tests.unit.test_output_publication import _assert_metadata
from tests.unit.test_output_publication_control import _service


# No module-level unit marker: the two live cases are L1 only.


@pytest.fixture(autouse=True)
def _bounded_infrastructure(monkeypatch, request):
    def forbidden(*_args, **_kwargs):
        pytest.fail("owner-death control test attempted unreviewed infrastructure")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(mp.Process, "start", forbidden)
    monkeypatch.setattr(threading.Timer, "start", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    monkeypatch.setattr(control, "rpc_request", forbidden)
    if request.node.get_closest_marker("unit") is not None:
        monkeypatch.setattr(threading.Thread, "start", forbidden)
        monkeypatch.setattr(threading.Thread, "join", forbidden)
        monkeypatch.setattr(threading.Event, "wait", forbidden)
        monkeypatch.setattr(threading.Condition, "wait", forbidden)
        yield
        return
    assert request.node.get_closest_marker("loopback_smoke") is not None
    real_start = threading.Thread.start
    started = []

    def start_only_gcs_driver(thread):
        assert thread.name == "miniray-owner-death-progress"
        assert not started
        started.append(thread)
        return real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start_only_gcs_driver)
    yield
    assert len(started) <= 1
    assert all(not thread.is_alive() for thread in started)


class _LifecycleServer:
    """TCPServer lifecycle only; no network operation is implemented."""

    def __init__(self, handlers):
        self.handlers = handlers
        self.address = ("127.0.0.1", 32999)
        self.is_running = False

    def start(self):
        self.is_running = True
        return self.address

    def stop(self):
        self.is_running = False


def _id(kind, byte):
    return kind(bytes((byte,)) * 16)


def _owner_case(monkeypatch, *, owners=1, refs=True):
    assert owners in (1, 2) and (owners == 1 or not refs)
    service, values = _service(monkeypatch, refs=refs)
    registry = service.publications.output_recovery
    node = values.header.node_incarnation
    domains, children, release_owners, calls, finalized = [], {}, {}, [], {}
    for index in range(owners):
        owner = values.owner if index == 0 else _id(WorkerID, 40)
        manifest = values.manifest
        if index:
            task = _id(TaskID, 41)
            identity = OutputPublicationID(
                _id(LeaseID, 42), TaskExecutionKey(TaskOutputManifest.for_task(task, 2), AttemptID(task, 0)),
            )
            header = replace(values.header, publication_id=identity, owner_worker_id=owner)
            manifest = OutputPublicationManifest.create(header, tuple(
                OutputSlotManifest(output, slot.tier, slot.size_bytes, slot.checksum)
                for output, slot in zip(identity.output_ids, values.slots)
            ))
        incarnation = protocol.WorkerIncarnation(
            node.node_id, node.node_pid, node.registration_epoch, owner, 1901 + index,
        )
        assert service.register_worker_incarnation(protocol.RegisterWorkerIncarnation(incarnation)).accepted
        witness = OutputPublicationCompleteWitness.for_manifest(manifest)
        graph = manifest.to_graph_manifest()
        assert service.report_output_publication(wire.ReportOutputPublicationIntent(manifest)).accepted
        if graph is not None:
            assert service.prepare_contained_graph(protocol.PrepareContainedGraph(graph)).accepted
        assert service.report_output_publication(wire.ArmOutputPublication(manifest.publication_id, manifest.manifest_digest)).accepted
        assert service.report_output_publication(wire.ReportOutputPublicationTerminal(witness)).accepted
        if graph is not None:
            assert service.commit_contained_graph(protocol.CommitContainedGraph(graph)).accepted
        proof = OutputPublicationAdoptionProof(witness, owner, "owner-control-adopted-{}".format(index))
        assert service.report_output_publication(wire.ReportOutputPublicationAdopted(proof)).accepted
        releases = []
        for slot in manifest.slots:
            for transfer in slot.transfers:
                table = children.setdefault(transfer.contained_owner_worker_id, ObjectOwnerTable())
                table.register(transfer.contained_object_id, local_token="source-live")
                # Seed the real child owner's already-promoted state. The
                # provisional hold is tombstoned; only final custody remains.
                assert table.add_contained_reference(transfer.contained_object_id, transfer.provisional_hold)
                assert table.release_contained_reference(transfer.contained_object_id, transfer.provisional_hold)
                assert table.add_contained_reference(transfer.contained_object_id, transfer.final_hold)
                for hold in (transfer.final_hold, transfer.provisional_hold):
                    release = protocol.ReleaseContainedReference(transfer.contained_object_id, transfer.contained_owner_worker_id, hold)
                    releases.append(release)
                    release_owners[release] = owner
        death_request = protocol.ReportWorkerDeath(
            "unified-owner-exit-{}".format(index), incarnation, 1,
            protocol.WorkerDeathReason.PROCESS_EXIT,
        )
        domains.append(SimpleNamespace(
            owner=owner, incarnation=incarnation, manifest=manifest, witness=witness,
            graph=graph, releases=tuple(releases), death_request=death_request,
        ))

    def rpc(address, handler, request):
        calls.append((address, handler, request))
        assert len(calls) <= 32
        if handler == control.INSTALL_OWNER_DEATH_FENCE_HANDLER:
            assert address == service.nodes.get(request.node_id).address
            assert request.scope is protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP
            assert service.workers.get_state_reply(protocol.GetWorkerState(request.owner_worker_id)).death == request.owner_death
            return protocol.InstallOwnerDeathFenceReply(request, protocol.OwnerDeathFenceDisposition.FENCED, ())
        if handler == "release_contained_reference":
            owner = release_owners[request]
            assert not service._owner_fence_registry().pending_for_owner(owner)
            transfer = next(transfer for domain in domains for slot in domain.manifest.slots
                            for transfer in slot.transfers if transfer.contained_object_id == request.object_id)
            assert address == transfer.contained_owner_address
            released = children[request.owner_worker_id].release_contained_reference(request.object_id, request.hold)
            return protocol.ReleaseContainedReferenceReply(request.object_id, request.owner_worker_id, request.hold, True, released)
        assert handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER
        domain = next(domain for domain in domains if domain.manifest == request.manifest)
        assert address == service.nodes.get(node.node_id).address
        assert not service._owner_fence_registry().pending_for_owner(domain.owner)
        assert registry.snapshot(domain.manifest.publication_id).owner_death == request.owner_death
        cleanup = service.publications._output_owner_cleanup[domain.manifest.publication_id]
        assert cleanup["released"] == set(domain.releases)
        for release in domain.releases:
            table = children[release.owner_worker_id]
            assert release.hold not in table.snapshot(release.object_id).contained_holds
            assert table.contained_release_was_seen(release.object_id, release.hold)
        if domain.graph is not None:
            saved = next(item for item in service.publications.graph.snapshot().manifests if item.manifest == domain.graph)
            assert not saved.active_edges
            assert cleanup["graph"]
        previous = finalized.setdefault(domain.manifest.publication_id, request)
        assert previous == request
        return wire.FinalizeOutputOwnerDeathReply(request, True)

    service._stored_hold_rpc = rpc
    return SimpleNamespace(service=service, registry=registry, domains=tuple(domains), children=children,
                           calls=calls, finalized=finalized, rpc=rpc)


@pytest.mark.unit
def test_direct_worker_death_freezes_without_remote_effect_and_replays(monkeypatch):
    case = _owner_case(monkeypatch)
    service, domain = case.service, case.domains[0]
    before = case.registry.snapshot(domain.manifest.publication_id)
    graph_before = service.publications.graph.snapshot()

    first = service.report_worker_death(domain.death_request)
    replay = service.report_worker_death(domain.death_request)

    assert first.disposition is protocol.WorkerDeathDisposition.APPLIED
    assert replay.disposition is protocol.WorkerDeathDisposition.ALREADY_DEAD
    assert replay.death == first.death and case.calls == []
    (work,) = case.registry.frozen_owner_workset(first.death)
    assert work.snapshot == replace(before, owner_death=first.death)
    assert work.snapshot.owner_cleaned is None
    _assert_metadata(work)
    assert case.registry.frozen_owner_workset(first.death) == (work,)
    assert len(service._owner_fence_registry().pending_for_owner(domain.owner)) == 1
    assert service.publications.graph.snapshot() == graph_before
    assert service._owner_death_progress_thread is None
    assert not service._drive_output_owner_deaths() and case.calls == []
    assert not service.commit_contained_graph(protocol.CommitContainedGraph(domain.graph)).accepted
    late = service.report_output_publication(wire.ReportOutputPublicationTerminal(domain.witness))
    assert not late.accepted and late.ack.disposition is OutputRecoveryDisposition.FENCED
    assert case.registry.snapshot(domain.manifest.publication_id) == work.snapshot
    assert service.publications.has_active_operations()


@pytest.mark.unit
def test_exact_child_ack_loss_and_invalid_finalize_ack_keep_cleanup_replayable(monkeypatch):
    case = _owner_case(monkeypatch)
    service, domain = case.service, case.domains[0]
    death = service.report_worker_death(domain.death_request).death
    lost, invalid_final = [], []

    def faulty_rpc(address, handler, request):
        reply = case.rpc(address, handler, request)
        if handler == "release_contained_reference" and not lost:
            assert reply.released
            lost.append(reply)
            raise TimeoutError("child released before exact ACK was lost")
        if handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER and not invalid_final:
            invalid_final.append(reply)
            object.__setattr__(reply, "cleaned", 1)
        return reply

    service._stored_hold_rpc = faulty_rpc
    progress = protocol.ProgressPublicationOwnerDeath(domain.owner)
    first = service.progress_publication_owner_death(progress)
    assert first.progressed and not first.clean and len(lost) == 1
    cleanup = service.publications._output_owner_cleanup[domain.manifest.publication_id]
    assert cleanup["released"] == set() and not cleanup["graph"]
    assert case.registry.snapshot(domain.manifest.publication_id).owner_cleaned is None
    assert service.publications.graph.snapshot().committed_edges
    for _ in range(8):
        service.progress_publication_owner_death(progress)
    assert len(invalid_final) == 1
    releases = [request for _, handler, request in case.calls if handler == "release_contained_reference"]
    assert len(releases) == 9 and releases[0] == releases[1]
    assert tuple(releases[1:]) == domain.releases
    assert cleanup["released"] == set(domain.releases) and cleanup["graph"]
    assert case.registry.snapshot(domain.manifest.publication_id).owner_cleaned is None
    assert not service.publications.graph.snapshot().committed_edges
    assert service.publications.has_active_operations()

    terminal = service.progress_publication_owner_death(progress)

    assert terminal.clean and terminal.progressed and terminal.active_publications == 0
    finalizations = [request for _, handler, request in case.calls if handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER]
    assert len(finalizations) == 2 and finalizations[0] == finalizations[1]
    assert len([1 for _, handler, _ in case.calls if handler == "release_contained_reference"]) == 9
    resolved = case.registry.snapshot(domain.manifest.publication_id)
    assert resolved.owner_cleaned == death and resolved.complete == domain.witness
    _assert_metadata(resolved)
    before = tuple(case.calls)
    assert service.progress_publication_owner_death(progress).clean
    assert tuple(case.calls) == before
    assert not service.publications.has_active_operations()


@pytest.mark.unit
def test_explicit_progress_filters_owner_and_global_drain_converges(monkeypatch):
    case = _owner_case(monkeypatch, owners=2, refs=False)
    service = case.service
    first, second = case.domains
    first_death = service.report_worker_death(first.death_request).death
    second_death = service.report_worker_death(second.death_request).death
    second_before = case.registry.snapshot(second.manifest.publication_id)
    second_fences = service._owner_fence_registry().pending_for_owner(second.owner)

    progress = service.progress_publication_owner_death(protocol.ProgressPublicationOwnerDeath(first.owner))

    assert progress.owner_worker_id == first.owner and progress.clean and progress.active_publications == 0
    assert case.registry.snapshot(first.manifest.publication_id).owner_cleaned == first_death
    assert case.registry.snapshot(second.manifest.publication_id) == second_before
    assert service._owner_fence_registry().pending_for_owner(second.owner) == second_fences
    assert [handler for _, handler, _ in case.calls] == [control.INSTALL_OWNER_DEATH_FENCE_HANDLER, wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER]
    assert all(request.owner_death.worker_id == first.owner for _, _, request in case.calls)
    assert service.publications.has_active_operations()
    before = tuple(case.calls)
    again = service.progress_publication_owner_death(protocol.ProgressPublicationOwnerDeath(first.owner))
    assert again.clean and not again.progressed and tuple(case.calls) == before

    drain = protocol.DrainPublicationOwnerDeaths("unified-owner-drain")
    terminal = service.drain_publication_owner_deaths(drain)
    assert terminal.clean and terminal.active_publications == 0 and terminal.request_id == drain.request_id
    assert case.registry.snapshot(second.manifest.publication_id).owner_cleaned == second_death
    assert not service._owner_fence_registry().has_active_operations()
    assert not service.publications.has_active_operations()
    before = tuple(case.calls)
    assert service.drain_publication_owner_deaths(drain) == terminal
    assert tuple(case.calls) == before


@pytest.mark.loopback_smoke
@pytest.mark.parametrize("failed_domain", ("fence", "publication"))
def test_live_background_converges_owner_wide_and_publication_sagas(monkeypatch, failed_domain):
    """One GCS thread, one progress fault, finite Event gate and full teardown."""
    case = _owner_case(monkeypatch)
    service, domain = case.service, case.domains[0]
    service._server = _LifecycleServer(service.handlers)
    entered, permit, converged = threading.Event(), threading.Event(), threading.Event()
    inspected = threading.Event()
    progress_events, failures = [], []
    rounds = {"fence": 0, "publication": 0}
    original_fence = service._drive_owner_death_fence_once
    original_publication = service._drive_output_owner_deaths
    death = service.report_worker_death(domain.death_request).death
    assert death is not None and case.calls == []

    def fence_progress():
        rounds["fence"] += 1
        if rounds["fence"] <= 12:
            progress_events.append(("fence", rounds["fence"]))
        if rounds["fence"] == 1:
            entered.set()
            if not permit.wait(1.0):
                raise RuntimeError("bounded initial progress gate was not released")
            if failed_domain == "fence":
                failures.append("fence")
                raise RuntimeError("injected owner-wide progress failure")
        return original_fence()

    def publication_progress():
        rounds["publication"] += 1
        if rounds["publication"] <= 12:
            progress_events.append(("publication", rounds["publication"]))
        if failed_domain == "publication" and rounds["publication"] == 1:
            failures.append("publication")
            raise RuntimeError("injected unified-publication progress failure")
        progressed = original_publication()
        snapshot = case.registry.snapshot(domain.manifest.publication_id)
        if snapshot.owner_cleaned is not None and not service._owner_fence_registry().has_active_operations():
            converged.set()
            # Park the driver on an ACK-controlled gate while the main thread
            # checks the exact terminal state; finally always releases it.
            if not inspected.wait(1.0):
                raise RuntimeError("terminal inspection gate exceeded its bound")
        return progressed

    monkeypatch.setattr(service, "_drive_owner_death_fence_once", fence_progress)
    monkeypatch.setattr(service, "_drive_output_owner_deaths", publication_progress)
    thread = None
    try:
        service.start()
        thread = service._owner_death_progress_thread
        assert thread is not None and thread.is_alive()
        assert entered.wait(1.0)
        assert case.calls == [] and not converged.is_set()
        permit.set()
        assert converged.wait(3.0)
        assert failures == [failed_domain]
        assert progress_events[:4] == [("fence", 1), ("publication", 1), ("fence", 2), ("publication", 2)]
        assert rounds["fence"] <= 12 and rounds["publication"] <= 12
        snapshot = case.registry.snapshot(domain.manifest.publication_id)
        assert snapshot.owner_cleaned == death and snapshot.complete == domain.witness
        assert not service._owner_fence_registry().has_active_operations()
        assert service._active_output_owner_deaths() == 0
        assert not service.publications.graph.snapshot().committed_edges
        scopes = [request.scope for _, handler, request in case.calls if handler == control.INSTALL_OWNER_DEATH_FENCE_HANDLER]
        assert scopes == [protocol.OwnerDeathFenceScope.OWNER_WIDE_SWEEP]
        assert len([1 for _, handler, _ in case.calls if handler == "release_contained_reference"]) == 8
        assert len([1 for _, handler, _ in case.calls if handler == wire.FINALIZE_OUTPUT_OWNER_DEATH_HANDLER]) == 1
        assert thread.is_alive() and service.is_running
    finally:
        permit.set()
        inspected.set()
        thread = thread or service._owner_death_progress_thread
        try:
            service.stop()
        finally:
            if thread is not None and thread.ident is not None:
                thread.join(1.0)
            service._server.stop()
            assert thread is None or not thread.is_alive()
            assert not service.is_running
