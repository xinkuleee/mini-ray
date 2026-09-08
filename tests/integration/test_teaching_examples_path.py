"""Run the original bounded teaching mains, one exact case per process tree.

Each case starts one GCS, one or two Nodes, and at most two ordinary Workers
(three to five startup children), with a 1 MiB store per Node. Example04 adds
one dedicated Actor Worker (six children) and three tiny method calls; example07
reserves two bundles and runs two ordinary Tasks. The remaining examples
submit at most two logical Tasks, use at most 64 KiB application payload, and
only example06 performs one physical drop and one reconstruction. Their get
calls share a ten-second post-init deadline; reference cleanup uses three
seconds. Examples01/06 each have at most two seconds of observational trace
wait within that same work budget; example06 observes only two Driver facts,
not a full distributed trace. Its debug drop retains its own finite RPC timeout. The exact test must
run alone through the existing 30-second process-tree runner.
Actor creation and PG create/remove have no per-call cancellation deadline:
only that outer experiment bound can abort unresolved control operations. A
runner timeout is failure, never a clean-shutdown or cancelled-reservation ACK.

runpy uses a non-__main__ name, so loading never starts the example. Calling
the returned main exercises its actual public API, assertions and output. The
temporary module is gone before function serialization, so cloudpickle sends
its functions by value; spawn does not import or rerun the example main.
Observers below forward real init/shutdown/close calls without replacing their
effects. The close observer is Driver-local: example05's Worker-side child
close is part of its real parent code, not claimed as a captured Driver call.
No extra test thread, gate, socket server or nested subprocess is created.
"""

from __future__ import annotations

import math
import multiprocessing as mp
import os
from pathlib import Path
import runpy
import socket

import pytest

import miniray as ray
from miniray.api import _get_runtime
from miniray.core import ActorEndpoint


pytestmark = pytest.mark.multiprocess_smoke
_EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
_STORE_BYTES = 1024 * 1024


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _assert_runtime_exited(
    managed_pids: set[int], managed_addresses: set[tuple[str, int]]
) -> None:
    """Probe only captured PIDs/endpoints, including an unsuccessful main."""

    live_pids = tuple(sorted(pid for pid in managed_pids if _pid_exists(pid)))
    active_pids = tuple(
        sorted(child.pid for child in mp.active_children() if child.pid in managed_pids)
    )
    open_addresses = []
    for address in sorted(managed_addresses):
        try:
            with socket.create_connection(address, timeout=0.1):
                open_addresses.append(address)
        except OSError:
            pass
    initialized = ray.is_initialized()
    assert not live_pids, live_pids
    assert not active_pids, active_pids
    assert not open_addresses, open_addresses
    assert not initialized


@pytest.mark.parametrize(
    "filename,num_nodes,workers_per_node,actor_count,pg_count,tracing,driver_closes,output_fragments",
    (
        pytest.param(
            "01_task_path.py", 1, 1, 0, 0, True, 1,
            ("TaskID:", "ObjectID:", "owner WorkerID:", "result: 49", "Canonical trace",
             "stage=INTENT", "stage=ARM_COMPLETE", "stage=TERMINAL", "stage=ADOPTED",
             "output_owner_ready", "output_payload_retired", "complete_worker_lease"),
            id="example01",
        ),
        pytest.param(
            "02_spillback_direct_submission.py", 2, 1, 0, 0, True, 1,
            ("ran on node 2", "worker PID"), id="example02",
        ),
        pytest.param(
            "03_cross_node_object_pull.py", 2, 1, 0, 0, True, 2,
            ("source PID:", "target PID:", "pulled bytes: 65536", "sha256:"),
            id="example03",
        ),
        pytest.param(
            "04_actor_control_direct.py", 2, 1, 1, 0, True, 3,
            ("counter values: [1, 2, 3]", "dedicated Actor PID:"),
            id="example04",
        ),
        pytest.param(
            "05_nested_get_cpu_yield.py", 1, 2, 0, 0, True, 1,
            ("parent PID:", "child PID:", "one CPU was yielded and then reacquired"),
            id="example05",
        ),
        pytest.param(
            "06_lineage_reconstruction.py", 1, 1, 0, 0, True, 1,
            ("stable TaskID:", "stable ObjectID:", "observed AttemptID:",
             "attempt numbers: 0 -> 1", "value after reconstruction:",
             "large-enough-for-the-object-store"),
            id="example06",
        ),
        pytest.param(
            "07_placement_group.py", 2, 1, 0, 1, False, 2,
            ("committed PGID:", "attempt: 0", "bundle placement: 0 -> NodeID:",
             "bundle placement: 1 -> NodeID:", "distinct Nodes are a hard constraint",
             "PACK would also need two Nodes", "bundle executors:"), id="example07",
        ),
    ),
)
def test_original_teaching_example_main_is_bounded_and_cleans_cluster(
    filename: str, num_nodes: int, workers_per_node: int, actor_count: int,
    pg_count: int, tracing: bool, driver_closes: int,
    output_fragments: tuple[str, ...], capsys: pytest.CaptureFixture[str],
) -> None:
    assert not ray.is_initialized()
    original_init, original_shutdown = ray.init, ray.shutdown
    original_close = ray.ObjectRef.close
    original_pg_create, original_pg_remove = ray.placement_group, ray.remove_placement_group
    init_calls, contexts, shutdown_reports, fallback_reports = [], [], [], []
    close_calls, close_receipts = [], []
    actor_calls, actor_endpoints, groups, group_removals = [], [], [], []
    pg_create_calls, pg_remove_calls = [], []
    actor_core = original_actor_create = None
    actor_create_missing = object()
    original_actor_override = actor_create_missing
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    owner_addresses, owner_ids = [], []

    def observe_init(*args, **kwargs):
        nonlocal actor_core, original_actor_create, original_actor_override
        assert not init_calls, "example exceeded its single-init bound"
        init_calls.append((args, dict(kwargs)))
        context = original_init(*args, **kwargs)
        contexts.append(context)
        runtime = _get_runtime()
        managed_pids.update((context.gcs_pid, *context.node_pids, *context.worker_pids))
        managed_addresses.update((context.gcs_address, *context.node_addresses, *context.worker_addresses))
        if context.trace_address is not None:
            managed_addresses.add(context.trace_address)
        if runtime.owner_service is not None:
            owner_addresses.append(runtime.owner_service.address)
            managed_addresses.add(runtime.owner_service.address)
        owner_ids.append(runtime.core_worker.worker_id)
        if actor_count:
            actor_core = runtime.core_worker
            original_actor_override = vars(actor_core).get("create_actor", actor_create_missing)
            original_actor_create = actor_core.create_actor
            actor_core.create_actor = observe_actor_create
        return context

    def observe_actor_create(*args, **kwargs):
        assert len(actor_calls) < actor_count, "example exceeded its Actor-creation bound"
        actor_calls.append(True)
        endpoint = original_actor_create(*args, **kwargs)
        # Capture the actual new process immediately, before the public handle
        # or any method result is exposed. No extra GCS query drives the route.
        assert isinstance(endpoint, ActorEndpoint) and endpoint.worker_pid is not None
        actor_endpoints.append(endpoint)
        managed_pids.add(endpoint.worker_pid)
        managed_addresses.add(endpoint.worker_address)
        return endpoint

    def observe_pg_create(*args, **kwargs):
        assert len(pg_create_calls) < pg_count, "example exceeded its placement-group bound"
        pg_create_calls.append(True)
        group = original_pg_create(*args, **kwargs)
        groups.append(group)
        return group

    def observe_pg_remove(group):
        assert len(pg_remove_calls) < pg_count, "example exceeded its public removal bound"
        pg_remove_calls.append(True)
        result = original_pg_remove(group)
        group_removals.append((group, result))
        return result

    def observe_shutdown():
        report = original_shutdown()
        # Preserve the first real result. A later cleanup must never turn an
        # unclean example shutdown into an apparently successful acceptance.
        assert not shutdown_reports, "example exceeded its single-shutdown bound"
        shutdown_reports.append(report)
        return report

    def observe_close(reference, *, timeout=None):
        assert len(close_calls) < driver_closes, "example exceeded its Driver-close bound"
        close_calls.append((reference.object_id, reference.owner_worker_id, timeout))
        done = reference._release_done
        result = original_close(reference, timeout=timeout)
        close_receipts.append((reference.closed, done is not None and done.is_set()))
        return result

    try:
        ray.init, ray.shutdown = observe_init, observe_shutdown
        ray.ObjectRef.close = observe_close
        if pg_count:
            ray.placement_group, ray.remove_placement_group = observe_pg_create, observe_pg_remove
        namespace = runpy.run_path(
            str(_EXAMPLES / filename), run_name="miniray_teaching_example",
        )
        assert namespace["__name__"] == "miniray_teaching_example"
        namespace["main"]()
        output = capsys.readouterr().out
    finally:
        try:
            if ray.is_initialized():
                # This is a separate fallback observation, not a replacement
                # for the main's own shutdown report or release receipts.
                fallback_reports.append(original_shutdown())
        finally:
            try:
                ray.init, ray.shutdown = original_init, original_shutdown
                ray.ObjectRef.close = original_close
                ray.placement_group, ray.remove_placement_group = original_pg_create, original_pg_remove
                if actor_core is not None:
                    if original_actor_override is actor_create_missing:
                        del actor_core.create_actor
                    else:
                        actor_core.create_actor = original_actor_override
            finally:
                _assert_runtime_exited(managed_pids, managed_addresses)

    assert len(init_calls) == len(contexts) == len(shutdown_reports) == 1
    assert not fallback_reports
    context, = contexts
    report, = shutdown_reports
    positional, options = init_calls[0]
    assert positional == ()
    assert options["num_nodes"] == num_nodes
    assert options.get("num_workers_per_node", 1) == workers_per_node
    assert options["object_store_bytes"] == _STORE_BYTES
    assert options.get("enable_tracing", True) is tracing
    assert len(context.nodes) == num_nodes
    assert (context.trace_address is not None) is tracing
    assert all(len(node.worker_pids) == workers_per_node for node in context.nodes)
    assert len(owner_addresses) == len(owner_ids) == 1
    assert len(actor_calls) == len(actor_endpoints) == actor_count
    for endpoint in actor_endpoints:
        assert endpoint.node_id == context.nodes[1].node_id
        assert endpoint.worker_pid not in context.worker_pids
        assert endpoint.worker_id not in context.worker_ids
        assert "dedicated Actor PID: {}".format(endpoint.worker_pid) in output
    assert len(pg_create_calls) == len(pg_remove_calls) == len(groups) == len(group_removals) == pg_count
    for group, (removed_group, removed) in zip(groups, group_removals):
        assert removed_group is group and removed is True
        assert group.bundle_count == 2
        assert tuple(key.bundle_index for key in group.placements) == (0, 1)
        assert {key.node_id for key in group.placements} == set(context.node_ids)
    assert len(managed_pids) == 1 + num_nodes * (1 + workers_per_node) + actor_count
    assert 3 <= len(managed_pids) <= 6 and os.getpid() not in managed_pids
    # GCS, all Node/Worker endpoints, Driver OwnerService, Driver trace collector.
    assert len(managed_addresses) == len(managed_pids) + 1 + int(tracing)
    assert len(close_calls) == len(close_receipts) == driver_closes
    assert len({object_id for object_id, _owner, _timeout in close_calls}) == driver_closes
    for _object_id, owner, timeout in close_calls:
        assert owner == owner_ids[0]
        assert not isinstance(timeout, bool) and isinstance(timeout, (int, float))
        assert math.isfinite(timeout) and 0 <= timeout <= 3.0
    assert all(closed and acknowledged for closed, acknowledged in close_receipts)
    assert all(fragment in output for fragment in output_fragments)

    assert report is not None and report.core_stopped
    assert report.gcs_pid == context.gcs_pid and report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_pids == context.node_pids and report.worker_pids == context.worker_pids
    assert all(exitcode == 0 for exitcode in report.node_exitcodes + report.worker_exitcodes)
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized and report.shutdown_ack_clean
    assert not report.forced
