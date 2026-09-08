"""A parent Task yields the only CPU while waiting for a child Task."""

from __future__ import annotations

import os
import time

import miniray as ray


_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("CPU-yield example exceeded its work deadline")
    return remaining


@ray.remote(num_cpus=1, max_retries=0)
def child() -> int:
    return os.getpid()


@ray.remote(num_cpus=1, max_retries=0)
def parent(deadline: float) -> tuple[int, int]:
    parent_pid = os.getpid()
    child_ref = None
    try:
        _remaining(deadline)
        child_ref = child.remote()
        # Worker-side get() reports Blocked before waiting. The Node yields only
        # CPU capacity and requires it to be reacquired before get() returns.
        return parent_pid, ray.get(child_ref, timeout=_remaining(deadline))
    finally:
        if child_ref is not None:
            child_ref.close(timeout=_CLEANUP_SECONDS)


def main() -> None:
    parent_ref = None
    try:
        context = ray.init(num_nodes=1, num_cpus=1, num_workers_per_node=2,
                           object_store_bytes=1024 * 1024)
        deadline = time.monotonic() + _WORK_SECONDS
        parent_ref = parent.remote(deadline)
        parent_pid, child_pid = ray.get(parent_ref, timeout=_remaining(deadline))
        assert parent_pid != child_pid
        assert {parent_pid, child_pid} == set(context.worker_pids)
        print("parent PID:", parent_pid, "child PID:", child_pid)
        print("one CPU was yielded and then reacquired")
    finally:
        try:
            if parent_ref is not None:
                parent_ref.close(timeout=_CLEANUP_SECONDS)
        finally:
            ray.shutdown()


if __name__ == "__main__":
    main()
