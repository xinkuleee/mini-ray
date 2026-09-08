"""Pure safety contracts for the bounded multiprocess-test runner."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Dict, Iterable, Optional

import pytest

from scripts import run_bounded_test as runner


pytestmark = pytest.mark.unit


def _snapshot(
    rows: Iterable[tuple[int, int, int]],
) -> Dict[int, runner._ProcessRecord]:
    return {
        pid: runner._ProcessRecord(pid, parent_pid, process_group_id)
        for pid, parent_pid, process_group_id in rows
    }


def test_process_snapshot_parser_keeps_only_unambiguous_positive_targets() -> None:
    records = runner._parse_process_snapshot(
        """
          100    42   100
          200   100   200
          300     0   300
            0   100   100
          400   100     0
          500    -1   500
          bad   row  here
          600   100   600 extra
        """
    )

    assert records == _snapshot(
        (
            (100, 42, 100),
            (200, 100, 200),
            (300, 0, 300),
        )
    )


def test_read_process_snapshot_uses_portable_numeric_ps_columns(monkeypatch) -> None:
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(stdout=" 100 42 100\n 200 100 200\n")

    monkeypatch.setattr(runner.subprocess, "run", fake_run)

    assert runner._read_process_snapshot() == _snapshot(
        ((100, 42, 100), (200, 100, 200))
    )
    assert calls == [
        (
            ("ps", "-axo", "pid=,ppid=,pgid="),
            {
                "check": True,
                "stdout": runner.subprocess.PIPE,
                "stderr": runner.subprocess.DEVNULL,
                "text": True,
                "timeout": runner.PROCESS_SNAPSHOT_TIMEOUT_SECONDS,
            },
        )
    ]


def test_process_snapshot_timeout_does_not_make_cleanup_wait_unbounded(monkeypatch) -> None:
    def timed_out(command, **kwargs):
        assert command == ("ps", "-axo", "pid=,ppid=,pgid=")
        assert kwargs["timeout"] == runner.PROCESS_SNAPSHOT_TIMEOUT_SECONDS == 0.25
        raise runner.subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(runner.subprocess, "run", timed_out)
    with pytest.raises(runner.subprocess.TimeoutExpired):
        runner._read_process_snapshot()


def test_descendant_closure_never_adopts_unrelated_processes() -> None:
    records = _snapshot(
        (
            (100, 42, 100),
            (200, 100, 200),
            (201, 200, 200),
            (202, 201, 202),
            (900, 1, 900),
            (901, 900, 900),
        )
    )

    assert set(runner._descendant_closure(records, (100,))) == {100, 200, 201, 202}


def test_tracker_retains_known_reparented_rows_and_expands_only_live_known_tree() -> None:
    tracker = runner._TrackedProcessTree(100)
    first = _snapshot(
        (
            (100, 42, 100),
            (200, 100, 200),
            (201, 200, 200),
            (900, 1, 900),
        )
    )
    assert set(tracker.refresh(first, leader_alive=True)) == {100, 200, 201}

    # Pytest has exited and both known Node/Worker rows have been reparented.
    # They remain exact tracked identities.  A newly observed child of the
    # still-live known Worker is admitted, while the unrelated tree is not.
    second = _snapshot(
        (
            (200, 1, 200),
            (201, 1, 200),
            (202, 201, 202),
            (900, 1, 900),
            (901, 900, 900),
        )
    )
    assert set(tracker.refresh(second, leader_alive=False)) == {200, 201, 202}

    # A PID whose PGID changed while disconnected from every trusted parent is
    # ambiguous (exit/PID-reuse versus setsid), so cleanup must not adopt it.
    reused = _snapshot(((200, 1, 777), (203, 200, 777), (900, 1, 900)))
    assert tracker.refresh(reused, leader_alive=False) == {}


def test_tracker_never_reacquires_a_pid_after_one_complete_snapshot_lost_it() -> None:
    tracker = runner._TrackedProcessTree(100)
    assert set(
        tracker.refresh(
            _snapshot(((100, 42, 100), (200, 100, 200))),
            leader_alive=True,
        )
    ) == {100, 200}
    assert tracker.refresh({}, leader_alive=False) == {}

    # Even the same PGID cannot distinguish a recycled numeric PID without a
    # portable birth token, so a conservative cleanup permanently retires it.
    assert tracker.refresh(_snapshot(((200, 1, 200),)), leader_alive=False) == {}


def test_signal_active_tree_uses_owned_groups_then_exact_remaining_pids(
    monkeypatch,
) -> None:
    group_calls = []
    pid_calls = []
    monkeypatch.setattr(
        runner.os, "killpg", lambda pgid, sig: group_calls.append((pgid, sig))
    )
    monkeypatch.setattr(
        runner.os, "kill", lambda pid, sig: pid_calls.append((pid, sig))
    )
    active = _snapshot(
        (
            (100, 42, 100),
            (200, 100, 200),
            (201, 200, 200),
            # Its PGID leader was never captured as a descendant, so only the
            # exact PID is safe to signal.
            (250, 201, 999),
            # Exact descendants in the runner's own group are also PID-only.
            (260, 201, 42),
            # Defensive input: the runner itself is never a signal target.
            (41, 1, 42),
        )
    )
    known_pgids = {pid: record.process_group_id for pid, record in active.items()}

    runner._signal_active_tree(
        active,
        known_pgids,
        runner.signal.SIGTERM,
        runner_pid=41,
        runner_pgid=42,
    )

    assert group_calls == [
        (100, runner.signal.SIGTERM),
        (200, runner.signal.SIGTERM),
    ]
    assert pid_calls == [
        (250, runner.signal.SIGTERM),
        (260, runner.signal.SIGTERM),
    ]


class _FakeProcess:
    pid = 100

    def __init__(self) -> None:
        self.wait_timeouts = []

    def poll(self) -> Optional[int]:
        return None

    def wait(self, timeout=None) -> int:
        self.wait_timeouts.append(timeout)
        return 0


def test_timeout_cleanup_rescans_known_tree_and_escalates_exact_targets(
    monkeypatch,
) -> None:
    snapshots = iter(
        (
            _snapshot(
                (
                    (100, 41, 100),
                    (200, 100, 200),
                    (201, 200, 200),
                    (250, 201, 999),
                    (999, 1, 999),
                    (900, 1, 900),
                )
            ),
            _snapshot(
                (
                    (100, 41, 100),
                    # Known rows remain tracked after being reparented.
                    (200, 1, 200),
                    (201, 200, 200),
                    (250, 1, 999),
                    # A late child and a late setsid group are discovered only
                    # through exact edges from live known descendants.
                    (202, 201, 200),
                    (300, 201, 300),
                    (999, 1, 999),
                    (900, 1, 900),
                )
            ),
            _snapshot(
                (
                    (100, 41, 100),
                    (200, 1, 200),
                    (201, 200, 200),
                    (202, 201, 200),
                    (250, 1, 999),
                    (300, 1, 300),
                    (999, 1, 999),
                    (900, 1, 900),
                )
            ),
        )
    )
    clock = iter((0.0, 0.0, 3.0))
    group_calls = []
    pid_calls = []
    monkeypatch.setattr(runner, "_read_process_snapshot", lambda: next(snapshots))
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(runner.os, "getpid", lambda: 41)
    monkeypatch.setattr(runner.os, "getpgrp", lambda: 42)
    monkeypatch.setattr(
        runner.os, "killpg", lambda pgid, sig: group_calls.append((pgid, sig))
    )
    monkeypatch.setattr(
        runner.os, "kill", lambda pid, sig: pid_calls.append((pid, sig))
    )
    process = _FakeProcess()

    runner._terminate_process_tree(process)  # type: ignore[arg-type]

    assert group_calls == [
        (100, runner.signal.SIGTERM),
        (200, runner.signal.SIGTERM),
        (200, runner.signal.SIGTERM),
        (300, runner.signal.SIGTERM),
        (100, runner.signal.SIGKILL),
        (200, runner.signal.SIGKILL),
        (300, runner.signal.SIGKILL),
    ]
    assert pid_calls == [
        (250, runner.signal.SIGTERM),
        (250, runner.signal.SIGKILL),
    ]
    assert process.wait_timeouts == [runner.TERMINATE_GRACE_SECONDS]
    assert all(target > 0 for target, _sig in group_calls + pid_calls)
    assert all(target not in {41, 42, 900, 999} for target, _sig in group_calls)
    assert all(target not in {41, 42, 900, 999} for target, _sig in pid_calls)


@pytest.mark.parametrize("invalid", (0, -1, True))
def test_signal_helpers_reject_non_positive_or_boolean_targets(
    monkeypatch, invalid
) -> None:
    monkeypatch.setattr(
        runner.os,
        "killpg",
        lambda _pgid, _sig: pytest.fail("unsafe group signal was attempted"),
    )
    monkeypatch.setattr(
        runner.os,
        "kill",
        lambda _pid, _sig: pytest.fail("unsafe PID signal was attempted"),
    )

    with pytest.raises(ValueError, match="positive integer"):
        runner._kill_process_group(invalid, runner.signal.SIGTERM, 42)
    with pytest.raises(ValueError, match="positive integer"):
        runner._kill_process(invalid, runner.signal.SIGTERM, 41)


def test_main_cleans_exact_process_tree_when_wait_is_interrupted(
    monkeypatch,
) -> None:
    node_id = next(iter(runner.ALLOWED_NODE_IDS))
    process = _FakeProcess()
    cleaned = []

    def interrupted_wait(timeout=None):
        assert timeout == runner.TEST_TIMEOUT_SECONDS
        raise KeyboardInterrupt

    process.wait = interrupted_wait  # type: ignore[method-assign]
    monkeypatch.setattr(
        runner.subprocess, "Popen", lambda *args, **kwargs: process
    )
    monkeypatch.setattr(
        runner, "_terminate_process_tree", lambda candidate: cleaned.append(candidate)
    )

    with pytest.raises(KeyboardInterrupt):
        runner.main([node_id])

    assert cleaned == [process]
