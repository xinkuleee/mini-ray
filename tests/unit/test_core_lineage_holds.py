"""Pure Core composition for dependency lifetime owned by lineage.

Two Tasks use actual discovery/journal/adoption and explicit local-token/GC
progress. No runtime constructor, thread, socket, timer or real wait runs.
The same original lifetime assertions now expose the finish/collection cuts
directly instead of depending on an unbounded background mailbox join.
"""

from __future__ import annotations

import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.core import CoreWorker, _lineage_hold_token
from miniray.ids import LeaseID, WorkerID
from miniray.ownership import ObjectCollectionState
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core
from tests.unit._pure_output_runtime import PureOutputRuntime


@pytest.fixture(autouse=True)
def _no_runtime(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("pure lineage hold test attempted runtime infrastructure")

    def already_set(event, timeout=None):
        assert event.is_set(), "pure close attempted a blocking receipt wait"
        return True

    for kind, name in ((CoreWorker, "__init__"), (threading.Thread, "start"),
                       (threading.Thread, "join"), (threading.Condition, "wait"),
                       (multiprocessing.process.BaseProcess, "start"),
                       (multiprocessing.process.BaseProcess, "join")):
        monkeypatch.setattr(kind, name, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


def _fixture():
    core = make_pure_core()
    outputs = PureOutputRuntime(core)
    core.gcs_address, core._rpc = outputs.gcs_address, outputs.rpc
    return core, outputs


def _complete_inline(core: CoreWorker, outputs: PureOutputRuntime, pending, value: object) -> None:
    assert core._dependencies_ready(pending)
    prepared, dependencies, _ = core._prepare_task_dependencies(pending.spec)
    push = protocol.PushTask(LeaseID.random(), WorkerID.random(), prepared, dependencies)
    reply = outputs.complete(push, (value,))
    assert core._publish_reply(pending, reply, expected_lease_id=push.lease_id,
                               expected_node_id=core.node_id)
    assert core.owner_table.snapshot(pending.object_id).output_publication is not None
    assert not outputs.journal.snapshot(reply.output_publication.publication_id).retained_result_slots


@pytest.mark.unit
def test_lineage_hold_outlives_execution_and_releases_with_producer() -> None:
    core, outputs = _fixture()
    leaf, leaf_ref = core._register_submission(
        core.define_remote_function(lambda: 1), (), {}, ResourceVector()
    )
    _complete_inline(core, outputs, leaf, 1)
    assert core._finish_pending_task(leaf)
    producer, producer_ref = core._register_submission(
        core.define_remote_function(lambda value: value + 1),
        (leaf_ref,), {}, ResourceVector(), max_retries=1,
    )
    lineage_token = _lineage_hold_token(
        producer.spec.task_id, leaf.object_id
    )
    try:
        assert lineage_token in core.owner_table.snapshot(
            leaf.object_id
        ).lineage_tokens
        _complete_inline(core, outputs, producer, 2)
        assert core._finish_pending_task(producer)
        # Physical execution is terminal, but canonical producer lineage still
        # needs the dependency if its output is lost later.
        assert lineage_token in core.owner_table.snapshot(
            leaf.object_id
        ).lineage_tokens
        leaf_ref.close()
        core._reference_mailbox.drain()
        assert core.owner_table.contains(leaf.object_id)

        producer_ref.close()
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(
            producer.object_id
        ) is ObjectCollectionState.COLLECTED
        assert core.owner_table.collection_state(
            leaf.object_id
        ) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(producer.object_id) is None
        assert core._recovery.lineage_for_object(leaf.object_id) is None
        outputs.assert_collected()
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
    finally:
        leaf_ref.close()
        producer_ref.close()
        close_pure_core(core)


@pytest.mark.unit
def test_nested_lineage_hold_outlives_execution_and_releases_with_producer() -> None:
    core, outputs = _fixture()
    nested, nested_ref = core._register_submission(
        core.define_remote_function(lambda: 1), (), {}, ResourceVector()
    )
    _complete_inline(core, outputs, nested, 1)
    assert core._finish_pending_task(nested)
    producer, producer_ref = core._register_submission(
        core.define_remote_function(lambda value: value),
        ({"ref": nested_ref},), {}, ResourceVector(), max_retries=1,
    )
    lineage_token = _lineage_hold_token(
        producer.spec.task_id, nested.object_id
    )
    try:
        assert producer.protected_dependencies == ()
        assert producer.nested_local_holds == (nested.object_id,)
        assert lineage_token in core.owner_table.snapshot(
            nested.object_id
        ).lineage_tokens
        _complete_inline(core, outputs, producer, "done")
        assert core._finish_pending_task(producer)
        assert lineage_token in core.owner_table.snapshot(
            nested.object_id
        ).lineage_tokens

        nested_ref.close()
        core._reference_mailbox.drain()
        assert core.owner_table.contains(nested.object_id)

        producer_ref.close()
        core._reference_mailbox.drain()
        assert core.owner_table.collection_state(
            producer.object_id
        ) is ObjectCollectionState.COLLECTED
        assert core.owner_table.collection_state(
            nested.object_id
        ) is ObjectCollectionState.COLLECTED
        assert core._recovery.lineage_for_object(producer.object_id) is None
        assert core._recovery.lineage_for_object(nested.object_id) is None
        outputs.assert_collected()
        assert core._accepted_task_count == 0 and not core._task_finish_barriers
    finally:
        nested_ref.close()
        producer_ref.close()
        close_pure_core(core)
