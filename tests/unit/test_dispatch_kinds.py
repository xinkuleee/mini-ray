"""Pure queue-envelope classification, not a substitute protocol runtime.

Opaque payloads deliberately prove only top-level continuation selection and
identity-preserving forwarding. The dispatcher uses a preloaded two-item FIFO
and spies, with no Core constructor, user execution, owner or remote mutation.
Existing publication/PG/custody tests exercise the actual retained protocols.
"""

from dataclasses import FrozenInstanceError, replace
from itertools import combinations
import queue

import pytest

from miniray import protocol
from miniray.core import CoreWorker, _DispatchKind, _PendingTask, _ReadyTask, _STOP
from miniray.ids import AttemptID, JobID, ObjectID, TaskID, WorkerID
from miniray.resources import ResourceVector


pytestmark = pytest.mark.unit

_FIELDS = (
    ("lease_state", _DispatchKind.LEASE),
    ("cancellation", _DispatchKind.CANCEL),
    ("push_state", _DispatchKind.PUSH),
    ("location_state", _DispatchKind.CUSTODY),
    ("output_adoption", _DispatchKind.OUTPUT_ADOPTION),
    ("output_node_loss", _DispatchKind.OUTPUT_NODE_LOSS),
    ("system_failure", _DispatchKind.SYSTEM_FAILURE),
)


def _pending():
    job = JobID.random()
    task = TaskID.derive(job, TaskID.for_driver(job), 0)
    spec = protocol.TaskSpec(
        job, task, AttemptID(task, 2),
        protocol.FunctionKey(job, __name__, "never_executed", "v1"),
        (), 1, ResourceVector({"CPU": 1}), WorkerID.random(),
    )
    return _PendingTask(ObjectID.for_task(task), spec, capacity_round=3)


def test_fresh_and_each_continuation_have_one_derived_kind():
    pending = _pending()
    # An ordinary new physical attempt is fresh even after prior retry/capacity
    # rounds. The kind is not an execution/owner state or an attempt number.
    fresh = _ReadyTask(pending, pending.spec, system_failure=None)
    assert fresh.kind is _DispatchKind.FRESH and fresh.pending is pending
    for name, kind in _FIELDS:
        payload = object()
        item = _ReadyTask(pending, pending.spec, **{name: payload})
        assert item.kind is kind and getattr(item, name) is payload
        assert item.pending is pending and item.spec is pending.spec
        assert all(getattr(item, other) is None for other, _ in _FIELDS if other != name)


def test_two_top_level_continuations_are_rejected_without_truthiness_or_mutation():
    pending = _pending()
    payload = object()
    for (first, _), (second, _) in combinations(_FIELDS, 2):
        with pytest.raises(ValueError, match="only one continuation"):
            _ReadyTask(pending, pending.spec, **{first: payload, second: payload})
    # Payload validity belongs to its authority, but even false-y present
    # values must not be mistaken for absence by this envelope validator.
    with pytest.raises(ValueError, match="only one continuation"):
        _ReadyTask(pending, pending.spec, lease_state=False, cancellation=False)


def test_ambiguity_belongs_only_to_a_frozen_lease_and_keeps_zero_round_legal():
    pending = _pending()
    lease = object()
    for round_number in (0, 1, 3):
        item = _ReadyTask(pending, pending.spec, lease_state=lease, ambiguity_round=round_number)
        assert item.kind is _DispatchKind.LEASE and item.ambiguity_round == round_number
    for invalid in (False, True, -1, 0.5, "1", None):
        with pytest.raises(ValueError, match="non-negative integer"):
            _ReadyTask(pending, pending.spec, lease_state=lease, ambiguity_round=invalid)
    with pytest.raises(ValueError, match="requires the original lease"):
        _ReadyTask(pending, pending.spec, ambiguity_round=1)
    for name, _ in _FIELDS[1:]:
        with pytest.raises(ValueError, match="requires the original lease"):
            _ReadyTask(pending, pending.spec, **{name: object()}, ambiguity_round=1)


def test_replace_rederives_kind_without_rebinding_original_payload():
    pending = _pending()
    lease, cancellation = object(), object()
    leased = _ReadyTask(pending, pending.spec, lease_state=lease, ambiguity_round=2)
    with pytest.raises(ValueError, match="only one continuation"):
        replace(leased, cancellation=cancellation)
    cancelled = replace(leased, lease_state=None, ambiguity_round=0, cancellation=cancellation)
    assert cancelled.kind is _DispatchKind.CANCEL and cancelled.cancellation is cancellation
    assert leased.kind is _DispatchKind.LEASE and leased.lease_state is lease
    assert leased.ambiguity_round == 2
    with pytest.raises(FrozenInstanceError):
        cancelled.kind = _DispatchKind.FRESH
    with pytest.raises(TypeError):
        _ReadyTask(pending, pending.spec, kind=_DispatchKind.CANCEL)


@pytest.mark.parametrize("name,kind", ((None, _DispatchKind.FRESH), *_FIELDS))
def test_dispatch_preserves_original_authority_and_exact_execute_arguments(name, kind):
    pending = _pending()
    payload = object()
    selected = {} if name is None else {name: payload}
    if name == "lease_state":
        selected["ambiguity_round"] = 2
    item = _ReadyTask(pending, pending.spec, **selected)
    core = object.__new__(CoreWorker)
    core._ready_tasks = queue.Queue(maxsize=2)
    core._ready_tasks.put_nowait(item)
    core._ready_tasks.put_nowait(_STOP)
    calls = []
    core._is_current_task_pending = lambda value: value is pending
    core._raise_if_placement_group_lost = lambda value: calls.append(("admit", value))

    def execute(*args, **kwargs):
        calls.append(("execute", args, kwargs))
        return False

    def cancel(*args):
        calls.append(("cancel", args))
        return False

    core._execute, core._resolve_lease_cancellation = execute, cancel
    core._finish_pending_task = lambda *_: pytest.fail("nonterminal turn was finished")
    core._dispatch_loop()
    assert core._ready_tasks.empty() and core._ready_tasks.unfinished_tasks == 0
    assert item.kind is kind
    if kind is _DispatchKind.FRESH:
        assert calls.pop(0) == ("admit", pending)
    if kind is _DispatchKind.CANCEL:
        assert calls == [("cancel", (pending, pending.spec, (), payload))]
    else:
        expected = {field: getattr(item, field) for field, _ in _FIELDS if field != "cancellation"}
        expected["ambiguity_round"] = item.ambiguity_round
        assert calls == [("execute", (pending, pending.spec, ()), expected)]


def test_nonpending_filter_defers_only_output_tails_to_their_original_authority():
    pending = _pending()
    for name, kind in _FIELDS:
        item = _ReadyTask(pending, pending.spec, **{name: object()})
        core = object.__new__(CoreWorker)
        core._ready_tasks = queue.Queue(maxsize=2)
        core._ready_tasks.put_nowait(item)
        core._ready_tasks.put_nowait(_STOP)
        calls = []
        core._is_current_task_pending = lambda _: False
        core._raise_if_placement_group_lost = lambda _: pytest.fail("retained work became fresh admission")
        core._resolve_lease_cancellation = lambda *_: pytest.fail("nonpending cancellation bypassed original filter")
        core._execute = lambda *_args, **_kwargs: calls.append("execute") or False
        core._finish_pending_task = lambda _: calls.append("finish")
        core._dispatch_loop()
        expected = "execute" if kind in (_DispatchKind.OUTPUT_ADOPTION, _DispatchKind.OUTPUT_NODE_LOSS) else "finish"
        assert calls == [expected]
        assert core._ready_tasks.empty() and core._ready_tasks.unfinished_tasks == 0
