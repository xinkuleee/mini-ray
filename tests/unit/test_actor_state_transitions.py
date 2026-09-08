"""Focused transition-graph tests for the serial Actor mailbox."""

from __future__ import annotations

import pytest

from miniray.actor_state import (
    ActorCall, ActorCallState, ActorMailboxStateError, _CallRecord,
    _transition_call,
)
from miniray.ids import ActorGeneration, ActorID, WorkerID


pytestmark = pytest.mark.unit


def _record(state: ActorCallState) -> _CallRecord:
    actor_id = ActorID.random()
    return _CallRecord(
        ActorCall(
            actor_id, ActorGeneration(actor_id, 0), WorkerID.random(), 0,
            "payload",
        ),
        state,
    )


def test_actor_call_transition_graph_accepts_only_adjacent_forward_edges() -> None:
    record = _record(ActorCallState.BUFFERED)

    for expected in (
        ActorCallState.QUEUED,
        ActorCallState.RUNNING,
        ActorCallState.COMPLETED,
    ):
        _transition_call(record, expected)
        assert record.state is expected


@pytest.mark.parametrize(
    "source,target",
    (
        (ActorCallState.BUFFERED, ActorCallState.RUNNING),
        (ActorCallState.QUEUED, ActorCallState.COMPLETED),
        (ActorCallState.RUNNING, ActorCallState.QUEUED),
        (ActorCallState.COMPLETED, ActorCallState.COMPLETED),
    ),
)
def test_actor_call_transition_graph_rejects_skips_and_regressions(
    source: ActorCallState, target: ActorCallState,
) -> None:
    record = _record(source)

    with pytest.raises(ActorMailboxStateError, match="illegal actor call"):
        _transition_call(record, target)

    assert record.state is source
