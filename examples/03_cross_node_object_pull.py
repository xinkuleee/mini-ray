"""Move a store-backed Task dependency directly between two Nodes."""

from __future__ import annotations

import hashlib
import os
import time

import miniray as ray

SOURCE_ONLY = "source_only"
TARGET_ONLY = "target_only"
PAYLOAD = b"D" * (64 * 1024)
_WORK_SECONDS = 10.0
_CLEANUP_SECONDS = 3.0


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("cross-node pull example exceeded its work deadline")
    return remaining


@ray.remote(num_cpus=1, resources={SOURCE_ONLY: 1})
def produce() -> tuple[int, bytes]:
    return os.getpid(), PAYLOAD


@ray.remote(num_cpus=1, resources={TARGET_ONLY: 1})
def consume(value: tuple[int, bytes]) -> tuple[int, int, int, str]:
    producer_pid, payload = value
    return os.getpid(), producer_pid, len(payload), hashlib.sha256(payload).hexdigest()


def main() -> None:
    producer_ref = consumer_ref = None
    try:
        context = ray.init(
            num_nodes=2,
            node_resources=(
                {"CPU": 1, SOURCE_ONLY: 1},
                {"CPU": 1, TARGET_ONLY: 1},
            ),
            inline_threshold=1024,
            object_store_bytes=1024 * 1024,
        )
        deadline = time.monotonic() + _WORK_SECONDS
        source_node, target_node = context.nodes
        producer_ref = produce.remote()
        # This stays a logical RefArg; the Driver neither get()s nor forwards bytes.
        consumer_ref = consume.remote(producer_ref)
        consumer_pid, producer_pid, size, digest = ray.get(consumer_ref, timeout=_remaining(deadline))
        assert producer_pid == source_node.worker_pid
        assert consumer_pid == target_node.worker_pid
        assert (size, digest) == (len(PAYLOAD), hashlib.sha256(PAYLOAD).hexdigest())
        print("source PID:", producer_pid, "target PID:", consumer_pid)
        print("pulled bytes:", size, "sha256:", digest)
        # Before target grant: source pin -> chunk pull -> checksum -> target
        # seal -> source unpin. PushTask carries a descriptor, never these bytes.
    finally:
        cleanup_deadline = time.monotonic() + _CLEANUP_SECONDS
        # Attempt both releases even if the first receipt times out. Neither
        # timeout cancels GC, and shutdown must still own cluster teardown.
        try:
            if consumer_ref is not None:
                consumer_ref.close(timeout=max(0.0, cleanup_deadline - time.monotonic()))
        finally:
            try:
                if producer_ref is not None:
                    producer_ref.close(timeout=max(0.0, cleanup_deadline - time.monotonic()))
            finally:
                ray.shutdown()


if __name__ == "__main__":
    main()
