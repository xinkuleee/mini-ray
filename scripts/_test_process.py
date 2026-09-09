"""Private POSIX pytest process boundary shared by reviewed entry points."""

from __future__ import annotations

from dataclasses import dataclass
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Set, Tuple


TEST_TIMEOUT_SECONDS = 30.0
TERMINATE_GRACE_SECONDS = 2.0
PROCESS_SNAPSHOT_TIMEOUT_SECONDS = 0.25
TIMEOUT_EXIT_CODE = 124
_PS_PROCESS_TREE_COMMAND = ("ps", "-axo", "pid=,ppid=,pgid=")


def _require_posix_execution() -> None:
    """Reject unsupported cleanup before creating an isolated pytest child."""

    if (os.name != "posix" or not callable(getattr(os, "getpgrp", None))
            or not callable(getattr(os, "killpg", None))
            or not hasattr(signal, "SIGKILL")):
        raise RuntimeError(
            "bounded test execution requires POSIX process groups (Linux/macOS). "
            "Native Windows process-tree cleanup is unsupported; --list remains available."
        )


def _child_environment(environment: Mapping[str, str]) -> dict[str, str]:
    """Keep ambient pytest options and plugins outside both reviewed runners."""

    env = dict(environment)
    env.pop("PYTEST_ADDOPTS", None)
    env.pop("PYTEST_PLUGINS", None)
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    return env


@dataclass(frozen=True)
class _ProcessRecord:
    """One numeric row from a read-only process-table snapshot."""

    pid: int
    parent_pid: int
    process_group_id: int


def _positive_id(value: int, *, kind: str) -> int:
    """Reject values which POSIX could interpret as broad signal targets."""

    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("{} must be a positive integer".format(kind))
    return value


def _parse_process_snapshot(output: str) -> Dict[int, _ProcessRecord]:
    """Parse ``ps`` output, ignoring any row that is not unambiguously safe."""

    records: Dict[int, _ProcessRecord] = {}
    for line in output.splitlines():
        columns = line.split()
        if len(columns) != 3:
            continue
        try:
            pid, parent_pid, process_group_id = (int(column) for column in columns)
        except ValueError:
            continue
        # PID and PGID are signal targets and must be strictly positive.  PPID
        # zero is a valid kernel-parent sentinel, but can never be a traversal
        # root in this runner.
        if pid <= 0 or parent_pid < 0 or process_group_id <= 0:
            continue
        records[pid] = _ProcessRecord(pid, parent_pid, process_group_id)
    return records


def _parse_proc_stat(value: str, expected_pid: int) -> Optional[_ProcessRecord]:
    """Read Linux stat identity, allowing spaces and parentheses in comm."""

    prefix, closing, suffix = value.rpartition(")")
    pid_text, opening, _comm = prefix.partition(" (")
    columns = suffix.split()
    if (not opening or not closing or len(columns) < 3
            or len(columns[0]) != 1 or columns[0] not in "RSDZTtXxKWPI"
            or not pid_text.isascii() or not pid_text.isdecimal()
            or any(not part.isascii() or not part.isdecimal() for part in columns[1:3])):
        return None
    pid, parent_pid, process_group_id = (int(part) for part in (pid_text, *columns[1:3]))
    if pid != expected_pid or pid <= 0 or process_group_id <= 0:
        return None
    return _ProcessRecord(pid, parent_pid, process_group_id)


def _read_proc_process_snapshot(root: Path = Path("/proc")) -> Dict[int, _ProcessRecord]:
    """Read only numeric PID/stat entries, without requiring a procps binary."""

    deadline = time.monotonic() + PROCESS_SNAPSHOT_TIMEOUT_SECONDS
    records: Dict[int, _ProcessRecord] = {}
    for entry in root.iterdir():
        if time.monotonic() >= deadline:
            # Never return a timed-out partial scan as a complete snapshot.
            raise subprocess.TimeoutExpired("/proc/[pid]/stat", PROCESS_SNAPSHOT_TIMEOUT_SECONDS)
        name = entry.name
        if not name.isascii() or not name.isdecimal() or name.startswith("0"):
            continue
        pid = int(name)
        try:
            value = (entry / "stat").read_text(encoding="utf-8", errors="surrogateescape")
        except OSError:
            # Exit races and permission-denied entries supply no trusted identity.
            continue
        record = _parse_proc_stat(value, pid)
        if record is not None:
            records[pid] = record
    if time.monotonic() >= deadline:
        raise subprocess.TimeoutExpired("/proc/[pid]/stat", PROCESS_SNAPSHOT_TIMEOUT_SECONDS)
    return records


def _read_process_snapshot() -> Dict[int, _ProcessRecord]:
    """Take numeric identities from Linux procfs or the macOS ps interface."""

    if sys.platform.startswith("linux"):
        return _read_proc_process_snapshot()

    completed = subprocess.run(
        _PS_PROCESS_TREE_COMMAND,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=PROCESS_SNAPSHOT_TIMEOUT_SECONDS,
    )
    return _parse_process_snapshot(completed.stdout)


def _descendant_closure(
    records: Mapping[int, _ProcessRecord], roots: Iterable[int]
) -> Dict[int, _ProcessRecord]:
    """Return only rows reachable by following exact parent PID edges."""

    children: Dict[int, list[int]] = {}
    for record in records.values():
        children.setdefault(record.parent_pid, []).append(record.pid)

    pending = [root for root in roots if root > 0]
    visited: Set[int] = set()
    descendants: Dict[int, _ProcessRecord] = {}
    while pending:
        parent_pid = pending.pop()
        if parent_pid in visited:
            continue
        visited.add(parent_pid)
        record = records.get(parent_pid)
        if record is not None:
            descendants[parent_pid] = record
        pending.extend(children.get(parent_pid, ()))
    return descendants


class _TrackedProcessTree:
    """Remember identities captured before TERM, including reparented rows.

    Snapshot records contain only PID/PPID/PGID, without a birth token.
    A remembered PID is therefore trusted on a later scan only
    while its PGID is unchanged.  New rows are admitted solely through a PPID
    edge from one of those live, trusted rows.  Dead remembered PIDs are never
    used as traversal roots, which avoids adopting an unrelated tree after PID
    reuse.
    """

    def __init__(self, leader_pid: int) -> None:
        self.leader_pid = _positive_id(leader_pid, kind="pytest leader PID")
        # start_new_session=True makes this equality an invariant controlled by
        # this runner, even if the first snapshot races with leader exit.
        self._known_pgids: Dict[int, int] = {leader_pid: leader_pid}
        self._retired_pids: Set[int] = set()

    @property
    def known_pgids(self) -> Mapping[int, int]:
        return self._known_pgids

    def refresh(
        self,
        records: Mapping[int, _ProcessRecord],
        *,
        leader_alive: bool,
    ) -> Dict[int, _ProcessRecord]:
        # Once a known PID disappears from a complete snapshot, never trust a
        # later process with that numeric PID.  With only portable PID/PPID/PGID
        # columns there is no birth token that could disprove PID reuse.
        eligible_records = {
            pid: record
            for pid, record in records.items()
            if pid not in self._retired_pids
        }
        trusted_roots = {
            pid
            for pid, expected_pgid in self._known_pgids.items()
            if pid in eligible_records
            and eligible_records[pid].process_group_id == expected_pgid
        }
        # Popen.poll() is stronger identity evidence for our direct child than
        # the process table.  Seeding its exact PID also catches a child row if
        # the snapshot omitted the leader during an exit race.
        if leader_alive:
            trusted_roots.add(self.leader_pid)

        active = _descendant_closure(eligible_records, trusted_roots)
        for pid in self._known_pgids:
            if pid == self.leader_pid and leader_alive:
                continue
            if pid not in active:
                self._retired_pids.add(pid)
        for pid, record in active.items():
            if pid == self.leader_pid:
                # Never weaken the start_new_session invariant based on a
                # surprising/reused process row.
                continue
            self._known_pgids[pid] = record.process_group_id

        leader_record = records.get(self.leader_pid)
        if leader_alive:
            if (
                leader_record is not None
                and leader_record.process_group_id == self.leader_pid
            ):
                active[self.leader_pid] = leader_record
            else:
                active[self.leader_pid] = _ProcessRecord(
                    self.leader_pid, os.getpid(), self.leader_pid
                )
        return active


def _signal_targets(
    active: Mapping[int, _ProcessRecord],
    known_pgids: Mapping[int, int],
    *,
    runner_pid: int,
    runner_pgid: int,
) -> Tuple[Dict[int, Set[int]], Set[int]]:
    """Split exact descendants into safe groups and remaining PID targets."""

    groups: Dict[int, Set[int]] = {}
    for pid, record in active.items():
        pgid = record.process_group_id
        # A group is safe only when its leader was itself captured as an exact
        # descendant in that self-led group.  Inherited/foreign groups are
        # handled by individual PID below.
        if (
            pgid != runner_pgid
            and known_pgids.get(pgid) == pgid
            and pgid > 0
        ):
            groups.setdefault(pgid, set()).add(pid)

    grouped_pids = {pid for members in groups.values() for pid in members}
    remaining_pids = {
        pid
        for pid in active
        if pid > 0 and pid != runner_pid and pid not in grouped_pids
    }
    return groups, remaining_pids


def _kill_process_group(process_group_id: int, sig: int, runner_pgid: int) -> None:
    process_group_id = _positive_id(process_group_id, kind="process group ID")
    if process_group_id == runner_pgid:
        return
    try:
        os.killpg(process_group_id, sig)
    except ProcessLookupError:
        pass


def _kill_process(pid: int, sig: int, runner_pid: int) -> None:
    pid = _positive_id(pid, kind="process ID")
    if pid == runner_pid:
        return
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        pass


def _signal_active_tree(
    active: Mapping[int, _ProcessRecord],
    known_pgids: Mapping[int, int],
    sig: int,
    *,
    runner_pid: int,
    runner_pgid: int,
    signaled_group_members: Optional[Dict[int, Set[int]]] = None,
    signaled_pids: Optional[Set[int]] = None,
) -> None:
    groups, remaining_pids = _signal_targets(
        active,
        known_pgids,
        runner_pid=runner_pid,
        runner_pgid=runner_pgid,
    )
    for pgid in sorted(groups):
        members = groups[pgid]
        prior_members = (signaled_group_members or {}).get(pgid, set())
        if signaled_group_members is None or not members.issubset(prior_members):
            _kill_process_group(pgid, sig, runner_pgid)
        if signaled_group_members is not None:
            signaled_group_members.setdefault(pgid, set()).update(members)

    for pid in sorted(remaining_pids):
        if signaled_pids is None or pid not in signaled_pids:
            _kill_process(pid, sig, runner_pid)
        if signaled_pids is not None:
            signaled_pids.add(pid)


def _terminate_process_tree(process: subprocess.Popen[bytes]) -> None:
    """Terminate the pytest session plus descendants which called setsid()."""

    leader_pid = _positive_id(process.pid, kind="pytest leader PID")
    runner_pid = _positive_id(os.getpid(), kind="runner PID")
    runner_pgid = _positive_id(os.getpgrp(), kind="runner process group ID")
    tracker = _TrackedProcessTree(leader_pid)
    signaled_group_members: Dict[int, Set[int]] = {}
    signaled_pids: Set[int] = set()

    try:
        records = _read_process_snapshot()
    except (OSError, subprocess.SubprocessError):
        records = {}
    active = tracker.refresh(records, leader_alive=process.poll() is None)
    _signal_active_tree(
        active,
        tracker.known_pgids,
        signal.SIGTERM,
        runner_pid=runner_pid,
        runner_pgid=runner_pgid,
        signaled_group_members=signaled_group_members,
        signaled_pids=signaled_pids,
    )

    deadline = time.monotonic() + TERMINATE_GRACE_SECONDS
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        process.poll()
        try:
            records = _read_process_snapshot()
        except (OSError, subprocess.SubprocessError):
            time.sleep(min(0.05, remaining))
            continue
        active = tracker.refresh(records, leader_alive=process.poll() is None)
        if not active:
            break
        # A child may fork or call setsid while TERM is in flight.  Re-signal a
        # group only when a newly discovered exact member joined it.
        _signal_active_tree(
            active,
            tracker.known_pgids,
            signal.SIGTERM,
            runner_pid=runner_pid,
            runner_pgid=runner_pgid,
            signaled_group_members=signaled_group_members,
            signaled_pids=signaled_pids,
        )
        time.sleep(min(0.05, remaining))

    # A fresh final snapshot avoids signaling stale PIDs after the grace period.
    # If the process snapshot is unavailable, only the Popen-owned leader remains strong
    # enough identity evidence for a safe final signal.
    try:
        records = _read_process_snapshot()
    except (OSError, subprocess.SubprocessError):
        records = {}
    active = tracker.refresh(records, leader_alive=process.poll() is None)
    _signal_active_tree(
        active,
        tracker.known_pgids,
        signal.SIGKILL,
        runner_pid=runner_pid,
        runner_pgid=runner_pgid,
    )

    # The leader is our direct child.  Reaping it prevents a zombie without
    # making cleanup wait indefinitely if the process table became unavailable.
    if process.poll() is None:
        try:
            process.wait(timeout=TERMINATE_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            pass


def run_pytest(selectors: tuple[str, ...], marker: str, *, root: Path) -> int:
    """Run an already validated finite selection with the fixed cleanup bound."""
    if not selectors or marker not in {"unit", "loopback_smoke", "multiprocess_smoke"}:
        raise ValueError("bounded execution requires explicit selectors and marker")
    _require_posix_execution()
    command = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
               "-m", marker, *selectors]
    process = subprocess.Popen(command, cwd=str(root), start_new_session=True,
                               env=_child_environment(os.environ))
    try:
        return process.wait(timeout=TEST_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        print("bounded test exceeded 30s; applying process-tree cleanup", file=sys.stderr)
        _terminate_process_tree(process)
        return TIMEOUT_EXIT_CODE
    except BaseException:
        _terminate_process_tree(process)
        raise
