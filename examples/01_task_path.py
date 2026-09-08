"""The smallest complete Task path and its stable logical identities."""

from __future__ import annotations

import os
import threading
import time

import miniray as ray
from miniray.trace_contract import (
    SUCCESS_TRACE_CONTRACT,
    TraceContract,
    load_trace_contract,
)


_TRACE_DELIVERY_TIMEOUT_SECONDS = 2.0
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("task-path example exceeded its work deadline")
    return remaining


def _canonical_trace(contract: TraceContract, task_id: str, work_deadline: float) -> str:
    """Wait for observational delivery, never for task correctness."""

    _remaining(work_deadline)
    deadline = min(work_deadline, time.monotonic() + _TRACE_DELIVERY_TIMEOUT_SECONDS)
    wake = threading.Event()
    while True:
        match = contract.match(ray.trace(), task_id=task_id)
        if match.ok:
            return match.render_sequence()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            match.require()
        wake.wait(min(0.01, remaining))


@ray.remote
def square(value: int) -> tuple[int, int]:
    return os.getpid(), value * value


def main() -> None:
    result_ref = None
    try:
        context = ray.init(num_nodes=1, num_cpus=1, object_store_bytes=1024 * 1024)
        deadline = time.monotonic() + _WORK_SECONDS
        # remote() finishes synchronous argument/reference preparation and
        # registers TaskID/ObjectID, without waiting for user execution.
        result_ref = square.remote(7)
        print("TaskID:        ", result_ref.object_id.task_id)
        print("ObjectID:      ", result_ref.object_id)
        print("owner WorkerID:", result_ref.owner_worker_id)
        worker_pid, value = ray.get(result_ref, timeout=_remaining(deadline))
        assert worker_pid == context.worker_pid and value == 49
        print("executor PID:  ", worker_pid, "result:", value)
        # Core submit -> Node lease -> direct Worker push -> owner publication.
        # GCS is not a Task queue, but mini's ordinary success still needs its
        # synchronous publication ACKs. The canonical path shows those calls.
        contract = load_trace_contract(SUCCESS_TRACE_CONTRACT)
        print("\nCanonical trace (volatile IDs and timestamps omitted):")
        print(_canonical_trace(contract, str(result_ref.object_id.task_id), deadline))
        print("owner_ready: owner CAS/wake; payload_retired: reply custody, not object GC.")
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if result_ref is not None:
                # This waits for reference release, not physical object GC.
                result_ref.close(timeout=max(0.0, cleanup_deadline - time.monotonic()))
        finally:
            ray.shutdown()


if __name__ == "__main__":
    main()
