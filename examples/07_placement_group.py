"""Reserve two bundles atomically, then bind one task to each bundle.

Run under the exact-tree 30-second example runner. Synchronous PG create/remove
retain their exact-replay semantics: an expired experiment budget means failure,
not cancellation of a reservation or proof of clean cluster shutdown.
"""

from __future__ import annotations

import os
import time

import miniray as ray


@ray.remote(num_cpus=1)
def identify(bundle_index: int) -> tuple[int, int]:
    return bundle_index, os.getpid()


def main() -> None:
    context = None
    group = None
    refs: list[ray.ObjectRef] = []
    try:
        context = ray.init(
            num_nodes=2, num_cpus=2, num_workers_per_node=1, enable_tracing=False,
            object_store_bytes=1024 * 1024,
        )
        work_deadline = time.monotonic() + 10.0
        # This call is synchronous: the handle is returned only after both Nodes
        # acknowledge commit.  There is no synthetic ``group.ready()`` object.
        # get's budget does not bound or cancel this control-plane transaction.
        group = ray.placement_group(
            [{"CPU": 1}, {"CPU": 1}], strategy="STRICT_SPREAD"
        )
        assert group.bundle_count == 2
        assert tuple(key.bundle_index for key in group.placements) == (0, 1)
        placed_nodes = {key.node_id for key in group.placements}
        assert len(placed_nodes) == 2 and placed_nodes == set(context.node_ids)
        # These are the committed public handle's values, not a synthetic trace
        # of intermediate PREPARE/COMMIT states. No Task has been submitted yet.
        print("committed PGID:", group.placement_group_id, "attempt:", group.attempt)
        for key in group.placements:
            print("bundle placement:", key.bundle_index, "-> NodeID:", key.node_id)
        print("STRICT_SPREAD: distinct Nodes are a hard constraint, not a preference.")
        print(
            "Each Node has two CPUs: STRICT_PACK could place both one-CPU bundles "
            "on one Node; STRICT_SPREAD requires the two distinct Nodes used here."
        )
        for index in range(group.bundle_count):
            refs.append(
                identify.options(placement_group=group, bundle_index=index).remote(index)
            )
        results = ray.get(
            refs, timeout=max(0.0, work_deadline - time.monotonic())
        )
        expected_pid_by_node = {
            node.node_id: node.worker_pid for node in context.nodes
        }
        assert results == [
            (key.bundle_index, expected_pid_by_node[key.node_id])
            for key in group.placements
        ]
        print("bundle executors:", results)
        # Normal execution demonstrates explicit public removal. It converges
        # the same PG attempt; a timeout must not guess that removal completed.
        removed = ray.remove_placement_group(group)
        assert removed
    finally:
        close_deadline = time.monotonic() + 3.0
        close_error: BaseException | None = None
        try:
            for ref in refs:
                try:
                    ref.close(
                        timeout=max(0.0, close_deadline - time.monotonic())
                    )
                except BaseException as exc:
                    if close_error is None:
                        close_error = exc
        finally:
            # Do not start another unbounded per-group remove in cleanup. The
            # existing cluster drain owns every known PG, including an initial
            # create whose reply never supplied this example with a handle.
            report = ray.shutdown()
        if close_error is not None:
            raise close_error
        if context is not None:
            assert report is not None and report.resources_clean and report.finalized


if __name__ == "__main__":
    main()
