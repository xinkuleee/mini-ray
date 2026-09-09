"""Two passive managed sentinels through the real monitor reducer; no I/O."""
import threading
import pytest
from miniray.node_monitor import ManagedNodeMonitor

pytestmark = pytest.mark.unit


def test_sentinel_dispatch_reports_only_each_exact_managed_process_once():
    class Process:
        def __init__(self, pid):
            self.pid, self.sentinel, self.joins = pid, object(), []

        def join(self, timeout):
            self.joins.append(timeout)

    first, second = Process(201), Process(202)
    unknown = object()
    batches = [(unknown, first.sentinel), (first.sentinel, second.sentinel)]
    waits, reported = [], []
    monitor = object.__new__(ManagedNodeMonitor)
    monitor._sentinels = {first.sentinel: first, second.sentinel: second}
    monitor._wake_reader = object()
    monitor._lock, monitor._stopping = threading.Lock(), False
    monitor._reported = set()
    monitor._on_death = reported.append

    def ready(readers):
        waits.append(tuple(readers))
        assert len(waits) <= 2
        return batches[len(waits) - 1]

    monitor._wait = ready
    monitor._run()
    assert reported == [first, second]
    assert first.joins == second.joins == [0]
    assert monitor._reported == {first, second}
    assert first.sentinel not in waits[1] and second.sentinel in waits[1]
