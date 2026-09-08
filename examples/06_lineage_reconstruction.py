"""Reconstruct a stable TaskID/ObjectID with a new physical AttemptID."""

from __future__ import annotations

import os
import threading
import time

import miniray as ray
from miniray.ids import AttemptID, ObjectID

_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0
_TRACE_SECONDS = 2.0
_TRACE_POLLS = 201


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("lineage example exceeded its work deadline")
    return remaining


def _observed_attempts(logical_id: ObjectID, work_deadline: float) -> tuple[str, str]:
    """Observe two committed Driver facts after get() has proved recovery."""

    task_id = logical_id.task_id
    expected = {
        "task_submitted": str(AttemptID(task_id, 0)),
        "object_reconstruction_started": str(AttemptID(task_id, 1)),
    }
    driver_pid = str(os.getpid())
    deadline = min(work_deadline, time.monotonic() + _TRACE_SECONDS)
    wake = threading.Event()
    for poll in range(_TRACE_POLLS):
        if time.monotonic() >= deadline:
            break
        found = {}
        for record in ray.trace():
            if (record.process_id != driver_pid
                    or record.component != "core_worker"
                    or record.event not in expected):
                continue
            fields = dict(record.fields)
            if fields.get("task_id") != str(task_id):
                continue
            assert fields.get("object_id") == str(logical_id)
            assert fields.get("attempt_id") == expected[record.event]
            assert record.event not in found, "expected one observation of each transition"
            found[record.event] = (record, fields["attempt_id"])
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        if len(found) == len(expected):
            submitted, first_attempt = found["task_submitted"]
            reconstructed, second_attempt = found["object_reconstruction_started"]
            assert submitted.process_sequence < reconstructed.process_sequence
            return first_attempt, second_attempt
        if poll + 1 < _TRACE_POLLS:
            wake.wait(min(0.01, remaining))
    raise TimeoutError(
        "lineage reconstruction succeeded, but its two Driver trace events "
        "were not delivered within the observation budget"
    )


@ray.remote(max_retries=1)
def produce() -> dict[str, str]:
    return {"payload": "large-enough-for-the-object-store"}


def main() -> None:
    result_ref = None
    try:
        ray.init(num_nodes=1, num_cpus=1, inline_threshold=1, object_store_bytes=1024 * 1024)
        deadline = time.monotonic() + _WORK_SECONDS
        result_ref = produce.remote()
        logical_id = result_ref.object_id
        first = ray.get(result_ref, timeout=_remaining(deadline))
        # Public teaching failpoint, not normal lifetime API: the next get()
        # sees LOST and replays lineage with a new AttemptID.
        _remaining(deadline)
        # drop_object uses its own finite RPC timeout; the next get shares the
        # original work deadline rather than granting reconstruction ten more seconds.
        assert ray.drop_object(result_ref)
        second = ray.get(result_ref, timeout=_remaining(deadline))
        assert first == second and result_ref.object_id == logical_id
        # Trace delivery is observational, never reconstruction authority.
        # Wait only for these two Driver events, within the same work deadline.
        attempts = _observed_attempts(logical_id, deadline)
        attempt_numbers = tuple(int(value.rsplit(":", 1)[1]) for value in attempts)
        print("stable TaskID:", logical_id.task_id)
        print("stable ObjectID:", logical_id)
        print("observed AttemptID:", attempts[0], "->", attempts[1])
        print("attempt numbers:", attempt_numbers[0], "->", attempt_numbers[1])
        print("value after reconstruction:", second)
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if result_ref is not None:
                result_ref.close(timeout=max(0.0, cleanup_deadline - time.monotonic()))
        finally:
            ray.shutdown()


if __name__ == "__main__":
    main()
