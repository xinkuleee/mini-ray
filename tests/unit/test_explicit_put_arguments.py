"""Small explicit-put argument admission and rollback contracts.

Each case owns one threadless Core, at most one Task and one inline put.
Values are <=256 bytes; no Node, Worker, socket, timer or task function runs.
The positive case checks actual queue admission, then uses an explicit local
error for terminal cleanup. Rejections must never create an implicit put.
"""

from __future__ import annotations

import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time
from types import SimpleNamespace

import cloudpickle
import pytest

from miniray import control, core as core_module, node, protocol, transport, worker
from miniray.core import CoreWorker, _WAKE_COORDINATOR
from miniray.ids import AttemptID, ObjectID, TaskID
from miniray.ownership import (
    ObjectCollectionState, ObjectState, ReleasedTaskReferenceHoldError,
)
from miniray.resources import ResourceVector
from tests.unit._pure_core import close_pure_core, make_pure_core


@pytest.fixture
def arguments(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("explicit-put argument contract attempted runtime or implicit put")

    def already_set(event, timeout=None):
        assert event.is_set(), "pure reference close attempted a blocking wait"
        return True

    for kind, method in (
        (CoreWorker, "__init__"), (CoreWorker, "shutdown"),
        (node.NodeServer, "__init__"), (worker.WorkerServer, "__init__"),
        (control.GCSLite, "__init__"), (transport.TCPServer, "__init__"),
        (threading.Thread, "__init__"), (threading.Thread, "start"),
        (threading.Thread, "join"), (threading.Timer, "__init__"),
        (threading.Condition, "wait"), (threading.Condition, "wait_for"),
        (threading.Barrier, "wait"), (queue.Queue, "join"),
        (multiprocessing.process.BaseProcess, "start"),
        (multiprocessing.process.BaseProcess, "join"),
    ):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(threading.Event, "wait", already_set)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)
    for module in (control, core_module, node, worker):
        monkeypatch.setattr(module, "rpc_request", forbidden)
    monkeypatch.setattr(transport, "request", forbidden)
    core = make_pure_core()
    core.inline_threshold = 128
    state = SimpleNamespace(core=core, refs=[], admitted=[], forbidden=forbidden)
    try:
        yield state
    finally:
        for pending in state.admitted:
            assert core.owner_table.snapshot(pending.object_id).state is ObjectState.PENDING
            assert core._publish_error(
                pending.object_id, pending.spec.attempt_id,
                RuntimeError("explicit test cleanup of admitted Task"),
            )
            assert core._finish_pending_task(pending)
        for ref in state.refs:
            ref.close(timeout=0)
        assert core._reference_mailbox.pending.qsize() <= 8
        core._reference_mailbox.drain()
        wake_count = core._submissions.qsize()
        assert wake_count <= 8
        for _ in range(wake_count):
            assert core._submissions.get_nowait() is _WAKE_COORDINATOR
            core._submissions.task_done()
        assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
        assert core._reference_mailbox.pending.empty()
        assert core._reference_mailbox.pending.unfinished_tasks == 0
        assert not core._objects and not core._stored_descriptors
        assert not core._object_gc_obligations and not core._task_finish_barriers
        assert core._accepted_task_count == core._inflight_submissions == 0
        for ref in state.refs:
            assert core.owner_table.collection_state(ref.object_id) is ObjectCollectionState.COLLECTED
            assert core._recovery.lineage_for_object(ref.object_id) is None
        close_pure_core(core)


def _rejects_without_admission(state, args, kwargs):
    core = state.core
    before_objects = set(core._objects)
    before_puts = core._put_index
    task_id = TaskID.derive(core.job_id, core.driver_task_id, core._submission_index)
    with pytest.raises(ValueError, match=r"inline budget; use ray\.put\(value\)"):
        core._register_submission(
            core.define_remote_function(state.forbidden), args, kwargs,
            ResourceVector(), _enqueue=True,
        )
    assert core._accepted_task_count == core._inflight_submissions == 0
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    assert set(core._objects) == before_objects
    assert core._put_index == before_puts and core._inflight_puts == 0
    assert not core._stored_descriptors and not core._task_finish_barriers
    assert not core.owner_table.contains(ObjectID.for_task(task_id))
    assert core._recovery.lineage_for_object(ObjectID.for_task(task_id)) is None
    return task_id


@pytest.mark.unit
def test_over_budget_value_requires_explicit_put_without_node_work(arguments, monkeypatch):
    monkeypatch.setattr(arguments.core, "_put_value", arguments.forbidden)
    value = b"large" * 50
    assert 128 < len(cloudpickle.dumps(value)) <= 300
    _rejects_without_admission(arguments, (value,), {})


@pytest.mark.unit
def test_positional_and_keyword_values_share_one_inline_budget(arguments, monkeypatch):
    monkeypatch.setattr(arguments.core, "_put_value", arguments.forbidden)
    value = b"x" * 70
    one_size = len(cloudpickle.dumps(value))
    assert one_size <= 128 < one_size * 2
    _rejects_without_admission(arguments, (value,), {"other": value})


@pytest.mark.unit
def test_small_values_keep_normal_single_output_queue_admission(arguments, monkeypatch):
    core = arguments.core
    monkeypatch.setattr(core, "_put_value", arguments.forbidden)
    pending, result = core._register_submission(
        core.define_remote_function(arguments.forbidden), (7,), {"word": "small"},
        ResourceVector(), _enqueue=True,
    )
    arguments.refs.append(result)
    arguments.admitted.append(pending)
    assert core._accepted_task_count == 1 and core._inflight_submissions == 0
    assert core._submissions.get_nowait() is pending
    core._submissions.task_done()
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    assert pending.output_ids == (result.object_id,)
    assert core._task_finish_barriers[result.object_id] is pending
    encoded = pending.spec.args + tuple(value for _, value in pending.spec.kwargs)
    assert all(type(value) is protocol.InlineArg for value in encoded)
    assert tuple(cloudpickle.loads(value.data) for value in encoded) == (7, "small")
    assert sum(len(value.data) for value in encoded) <= core.inline_threshold
    assert not any(value.nested_refs for value in encoded)
    assert core._put_index == 0 and not core._stored_descriptors
    assert core._recovery.lineage_for_object(result.object_id).task_spec == pending.spec


@pytest.mark.unit
def test_later_large_argument_releases_already_installed_nested_hold(arguments, monkeypatch):
    core = arguments.core
    source = core.put(7)
    arguments.refs.append(source)
    # Consume the completed put's existing wake before testing Task admission.
    assert core._submissions.get_nowait() is _WAKE_COORDINATOR
    core._submissions.task_done()
    assert core._submissions.empty() and core._submissions.unfinished_tasks == 0
    before = core.owner_table.snapshot(source.object_id)
    assert before.state is ObjectState.READY_INLINE and before.local_tokens
    assert not before.submitted_tokens and not before.lineage_tokens
    monkeypatch.setattr(core, "_put_value", arguments.forbidden)
    original_encoder = core_module.encode_task_argument
    encoded_values, observed_holds = [], []
    large = b"x" * 256

    def encode(value, **kwargs):
        assert len(encoded_values) < 2
        if value is large:
            held = core.owner_table.snapshot(source.object_id)
            assert len(held.submitted_tokens) == 1
            observed_holds.extend(held.submitted_tokens)
            assert len(encoded_values) == 1
            assert len(encoded_values[0].data) <= core.inline_threshold
            assert len(encoded_values[0].nested_refs) == 1
        result = original_encoder(value, **kwargs)
        encoded_values.append(result)
        return result

    monkeypatch.setattr(core_module, "encode_task_argument", encode)
    rejected_task = _rejects_without_admission(arguments, ({"child": source}, large), {})
    expected_hold = protocol.TaskReferenceHold(
        protocol.TaskReferenceHoldKind.SUBMITTED, core.worker_id,
        rejected_task, AttemptID(rejected_task, 0),
    )
    assert observed_holds == [expected_hold] and len(encoded_values) == 2
    after = core.owner_table.snapshot(source.object_id)
    assert after.state is before.state and after.inline_data == before.inline_data
    assert after.local_tokens == before.local_tokens
    assert not after.submitted_tokens and not after.lineage_tokens
    assert not after.contained_holds and not after.borrowed_tokens
    # The same submitted-hold identity was really retired, not merely hidden.
    with pytest.raises(ReleasedTaskReferenceHoldError):
        core.owner_table.add_submitted_reference(source.object_id, expected_hold)
    assert core._reference_mailbox.pending.qsize() == 1
