"""Bounded partial-PREPARE rollback acceptance path.

One two-bundle STRICT_SPREAD placement group uses the explicit test-only GCS
checkpoint to reject the second immutable participant only after the first
real Node has acknowledged PREPARED.  GCS must then send real ABORT operations
to the complete participant set, wait for both ACKs, publish no placement, and
leave both Node root ledgers at their initial one-CPU baselines.

Run only this exact node ID through ``scripts/run_baseline.py --smoke EXACT``. Bounds are
five children (one GCS, two one-Worker Nodes), 1 MiB per Node store, one failed
PG attempt with the original single PREPARE rejection, and no application
Tasks, object data, test-owned thread/listener or sleeps. PG control, semantic
trace observations and typed queries share ten seconds after init. Trace
polling is finite and passive; it never repairs protocol state. Synchronous
PG creation, startup and shutdown still need the outer 30-second process-tree
bound: the work deadline is not transaction cancellation. Finally checks all
five PIDs and seven endpoints, including Driver owner/trace, even on failure.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import socket
import threading
import time

import pytest

import miniray as ray
from miniray import protocol
from miniray.api import _get_runtime
from miniray.control import (
    GET_NODES_HANDLER,
    GET_PLACEMENT_GROUP_HANDLER,
    PlacementGroupPrepareFailureConfig,
)
from miniray.ids import PlacementGroupID
from miniray.transport import request as rpc_request


pytestmark = pytest.mark.multiprocess_smoke

_WORK_SECONDS = 10.0
_POLL_SECONDS = 0.01
_MAX_TRACE_POLLS = 1024
_MAX_TRACE_RECORDS = 4096
_EXPECTED_ROOT = {"CPU": "1"}


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("placement-group PREPARE work exceeded its deadline")
    return remaining


def _query(address, handler, request, deadline):
    remaining = _remaining(deadline)
    return rpc_request(
        address, handler, request, connect_timeout=min(0.5, remaining / 2),
        request_timeout=remaining / 2, deadline=deadline,
    )


def _assert_managed_cleanup(pids, addresses) -> None:
    surviving_pids = tuple(pid for pid in sorted(pids) if _pid_exists(pid))
    surviving_children = tuple(
        child.pid for child in mp.active_children() if child.pid in pids
    )
    open_addresses = []
    for address in sorted(addresses):
        try:
            with socket.create_connection(address, timeout=0.1):
                open_addresses.append(address)
        except OSError:
            pass
    assert not surviving_pids, surviving_pids
    assert not surviving_children, surviving_children
    assert not open_addresses, open_addresses


def _fields(record: object) -> dict[str, str]:
    return dict(record.fields)


def _available(record: object) -> dict[str, str]:
    raw = json.loads(_fields(record)["root_available"])
    return dict(raw)


def _descends_from(
    records: tuple[object, ...], descendant: object, ancestor: object
) -> bool:
    by_id = {record.event_id: record for record in records}
    current = descendant
    visited: set[str] = set()
    while current.cause_event_id is not None:
        cause_id = current.cause_event_id
        if cause_id == ancestor.event_id:
            return True
        if cause_id in visited or cause_id not in by_id:
            return False
        visited.add(cause_id)
        current = by_id[cause_id]
    return False


def _semantic_records(deadline: float) -> tuple[object, ...]:
    wake = threading.Event()
    for _ in range(_MAX_TRACE_POLLS):
        _remaining(deadline)
        records = tuple(ray.trace())
        assert len(records) <= _MAX_TRACE_RECORDS, "trace exceeded reviewed observation bound"
        names = {(record.component, record.event) for record in records}
        node_aborts = tuple(
            record for record in records
            if record.component == "node"
            and record.event == "placement_group_abort_applied"
            and _fields(record).get("status") == "ABORTED"
        )
        rejected = tuple(
            record for record in records
            if record.component == "gcs"
            and record.event == "placement_group_prepare_rejected_by_failpoint"
        )
        prepares = tuple(
            record for record in records
            if record.component == "node"
            and record.event == "placement_group_prepare_applied"
            and _fields(record).get("status") == "PREPARED"
        )
        if (
            ("gcs", "placement_group_prepare_rejected_by_failpoint")
            in names
            and len(node_aborts) == 2
            and len(rejected) == 1
            and len(prepares) == 1
            and _descends_from(records, rejected[0], prepares[0])
            and all(
                _descends_from(records, abort, rejected[0])
                for abort in node_aborts
            )
        ):
            return records
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return records
        wake.wait(min(_POLL_SECONDS, remaining))
    raise TimeoutError("PREPARE trace exceeded reviewed polling bound")


def test_second_participant_prepare_rejection_aborts_first_and_restores_roots(
) -> None:
    context = None
    report = None
    managed_pids: set[int] = set()
    managed_addresses: set[tuple[str, int]] = set()
    try:
        context = ray.init(
            num_nodes=2,
            num_cpus=1,
            num_workers_per_node=1,
            object_store_bytes=1024 * 1024,
            _test_placement_group_prepare_failure=(
                PlacementGroupPrepareFailureConfig(participant_ordinal=2)
            ),
            enable_tracing=True,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        managed_pids.update(
            {context.gcs_pid, *context.node_pids, *context.worker_pids}
        )
        managed_addresses.update(
            {
                context.gcs_address,
                *context.node_addresses,
                *context.worker_addresses,
            }
        )
        assert context.trace_address is not None
        managed_addresses.add(context.trace_address)
        runtime = _get_runtime()
        assert runtime.owner_service is not None
        managed_addresses.add(runtime.owner_service.address)
        assert len(managed_pids) == 5
        assert len(managed_addresses) == 7
        assert os.getpid() not in managed_pids

        _remaining(deadline)
        with pytest.raises(
            ray.SystemTaskError, match="injected placement-group prepare rejection"
        ):
            ray.placement_group(
                [{"CPU": 1}, {"CPU": 1}],
                strategy="STRICT_SPREAD",
            )
        _remaining(deadline)

        records = _semantic_records(deadline)
        rejected = tuple(
            record for record in records
            if record.component == "gcs"
            and record.event == "placement_group_prepare_rejected_by_failpoint"
        )
        assert len(rejected) == 1
        rejected_fields = _fields(rejected[0])
        placement_group_id = rejected_fields["placement_group_id"]
        assert rejected_fields["attempt"] == "0"
        assert rejected_fields["participant_ordinal"] == "2"
        assert rejected_fields["status"] == "REJECTED"
        assert rejected_fields["applied"] == "false"
        prepared_prefix = tuple(json.loads(rejected_fields["prepared_prefix"]))
        assert len(prepared_prefix) == 1

        prepares = tuple(
            record for record in records
            if record.component == "node"
            and record.event == "placement_group_prepare_applied"
            and _fields(record).get("placement_group_id") == placement_group_id
        )
        aborts = tuple(
            record for record in records
            if record.component == "node"
            and record.event == "placement_group_abort_applied"
            and _fields(record).get("placement_group_id") == placement_group_id
        )
        # Only participant one receives a real PREPARE.  Participant two is the
        # typed semantic checkpoint, but both receive and ACK real ABORTs.
        assert len(prepares) == 1
        first_prepare = prepares[0]
        first_fields = _fields(first_prepare)
        assert first_fields["node_id"] == prepared_prefix[0]
        assert first_fields["status"] == "PREPARED"
        assert first_fields["applied"] == "true"
        assert _available(first_prepare) == {}
        assert first_fields["plan_digest"]
        assert rejected_fields["node_id"] != first_fields["node_id"]
        assert {
            rejected_fields["node_id"], first_fields["node_id"]
        } == {str(node_id) for node_id in context.node_ids}
        assert len(aborts) == 2
        assert {
            _fields(record)["node_id"] for record in aborts
        } == {str(node_id) for node_id in context.node_ids}
        assert all(_fields(record)["attempt"] == "0" for record in aborts)
        assert all(_fields(record)["status"] == "ABORTED" for record in aborts)
        assert all(_fields(record)["applied"] == "true" for record in aborts)
        assert all(_available(record) == _EXPECTED_ROOT for record in aborts)
        assert _descends_from(records, rejected[0], first_prepare)
        assert all(
            _descends_from(records, record, rejected[0]) for record in aborts
        )
        first_abort = next(
            record for record in aborts
            if _fields(record)["node_id"] == first_fields["node_id"]
        )
        assert _fields(first_abort)["plan_digest"] == first_fields["plan_digest"]
        rejected_abort = next(
            record for record in aborts
            if _fields(record)["node_id"] == rejected_fields["node_id"]
        )
        assert (
            _fields(rejected_abort)["plan_digest"]
            == rejected_fields["plan_digest"]
        )

        pg = _query(
            context.gcs_address,
            GET_PLACEMENT_GROUP_HANDLER,
            protocol.GetPlacementGroupRequest(
                PlacementGroupID.from_hex(placement_group_id)
            ),
            deadline,
        )
        assert isinstance(pg, protocol.GetPlacementGroupReply)
        assert pg.found
        assert pg.phase is protocol.PlacementGroupPhaseStatus.REMOVED
        assert pg.placements == ()

        nodes = _query(
            context.gcs_address, GET_NODES_HANDLER, protocol.GetNodes(),
            deadline,
        )
        assert isinstance(nodes, protocol.GetNodesReply)
        assert {node.node_id for node in nodes.nodes} == set(context.node_ids)
        assert all(
            node.available_resources == node.total_resources
            for node in nodes.nodes
        )
        _remaining(deadline)
    finally:
        try:
            report = ray.shutdown()
        finally:
            _assert_managed_cleanup(managed_pids, managed_addresses)

    assert context is not None and report is not None
    assert not ray.is_initialized()
    assert report.core_stopped
    assert report.gcs_pid == context.gcs_pid
    assert report.gcs_clean and report.gcs_exitcode == 0
    assert report.node_pids == context.node_pids
    assert report.worker_pids == context.worker_pids
    assert report.node_clean and report.worker_clean
    assert report.resources_clean and report.finalized
    assert report.shutdown_ack_clean and not report.forced
    assert all(exitcode == 0 for exitcode in report.node_exitcodes)
    assert all(exitcode == 0 for exitcode in report.worker_exitcodes)
