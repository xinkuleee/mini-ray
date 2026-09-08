"""Create through GCS, then call a dedicated Actor Worker directly.

Run under the exact-tree 30-second example runner. Its budget bounds the whole
experiment, not Actor creation: timeout is a failed/aborted experiment, never
proof that creation was cancelled or cluster cleanup completed cleanly.
"""

from __future__ import annotations

import os
import time

import miniray as ray

ACTOR_ONLY = "actor_only"


@ray.remote(num_cpus=1, resources={ACTOR_ONLY: 1})
class Counter:
    def __init__(self) -> None:
        self.value = 0

    def increment(self) -> tuple[int, int]:
        self.value += 1
        return self.value, os.getpid()


def main() -> None:
    context = None
    call_refs: list[ray.ObjectRef] = []
    try:
        context = ray.init(
            num_nodes=2,
            node_resources=({"CPU": 1}, {"CPU": 1, ACTOR_ONLY: 1}),
            object_store_bytes=1024 * 1024,
        )
        work_deadline = time.monotonic() + 10.0
        actor_node = context.nodes[1]
        # Creation is control-plane work: GCS chooses a Node, which reserves
        # lifetime resources and starts a dedicated Actor Worker.
        # This synchronous exact-replay operation has no user cancellation
        # deadline. The outer runner, not get's budget, bounds the experiment.
        counter = Counter.remote()
        # Calls use the returned endpoint directly; they do not revisit GCS.
        for _ in range(3):
            call_refs.append(counter.increment.remote())
        results = ray.get(
            call_refs, timeout=max(0.0, work_deadline - time.monotonic())
        )
        assert [value for value, _pid in results] == [1, 2, 3]
        actor_pids = {pid for _value, pid in results}
        assert len(actor_pids) == 1 and actor_pids.isdisjoint(actor_node.worker_pids)
        print("counter values:", [value for value, _pid in results])
        print("dedicated Actor PID:", next(iter(actor_pids)))
    finally:
        close_deadline = time.monotonic() + 3.0
        close_error: BaseException | None = None
        try:
            for ref in call_refs:
                try:
                    ref.close(
                        timeout=max(0.0, close_deadline - time.monotonic())
                    )
                except BaseException as exc:
                    # Every original handle still gets a release attempt. A
                    # wait timeout does not cancel its pending owner cleanup.
                    if close_error is None:
                        close_error = exc
        finally:
            report = ray.shutdown()
        if close_error is not None:
            raise close_error
        if context is not None:
            assert report is not None and report.resources_clean and report.finalized


if __name__ == "__main__":
    main()
