"""Force deterministic spillback with a node-local custom resource."""

from __future__ import annotations

import os
import time

import miniray as ray

REMOTE_ONLY = "remote_only"
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("spillback example exceeded its work deadline")
    return remaining


@ray.remote(num_cpus=1, resources={REMOTE_ONLY: 1})
def identify_executor(value: str) -> tuple[int, str]:
    return os.getpid(), value


def main() -> None:
    result_ref = None
    try:
        context = ray.init(
            num_nodes=2,
            node_resources=({"CPU": 1}, {"CPU": 1, REMOTE_ONLY: 1}),
            object_store_bytes=1024 * 1024,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        local_node, target_node = context.nodes
        result_ref = identify_executor.remote("ran on node 2")
        worker_pid, value = ray.get(result_ref, timeout=_remaining(deadline))
        assert local_node.node_id == context.node_id
        assert worker_pid == target_node.worker_pid
        print(value, "(worker PID", worker_pid, ")")
        # Home Node returns SpillbackWorkerLease; the Core keeps the same
        # LeaseID/TaskID/AttemptID on hop two, then pushes directly to Worker 2.
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        try:
            if result_ref is not None:
                result_ref.close(timeout=max(0.0, cleanup_deadline - time.monotonic()))
        finally:
            ray.shutdown()


if __name__ == "__main__":
    main()
