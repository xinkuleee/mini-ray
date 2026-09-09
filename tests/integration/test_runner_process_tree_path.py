"""One Linux process-tree cleanup experiment; no mini-ray runtime is imported.

Cost review: one test-owned Python parent and one Python grandchild, each in
its own POSIX session/group; no threads, sockets, services or data objects.
Control source is below 32 KiB and the readiness frame is exactly 48 bytes;
this is a payload bound, not a claim about Python interpreter RSS. Both
readiness waits are at most 2 s. The ready parent must exceed a 0.1 s wait,
then the existing runner performs real TERM cleanup across both groups. The
normal experiment must finish within 5 s; the external exact runner retains
its 30 s bound and existing cleanup grace on failure.

The grandchild exits on TERM. Its parent only records TERM, then reaps the
grandchild before exiting, so the runner must reach the detached group too.
Parent-owned emergency cleanup is failure (nonzero exit), never success. A
5 s alarm also bounds an orphaned grandchild if startup or the runner fails.
No name search or unowned PID is signaled. This proves timeout-tree cleanup;
the CLI interruption branch has separate pure contracts, not a real SIGINT
claim from this experiment.
"""

from __future__ import annotations

import os
from pathlib import Path
import select
import struct
import subprocess
import sys
import time

import pytest

from scripts import _test_process as bounded


pytestmark = pytest.mark.multiprocess_smoke

_GRANDCHILD = """
import os, signal, struct
def terminate(signum, frame):
    raise SystemExit(0)
signal.signal(signal.SIGTERM, terminate)
signal.alarm(5)
os.write(1, struct.pack('!QQQ', os.getpid(), os.getppid(), os.getpgrp()))
signal.pause()
raise SystemExit(21)
"""

_PARENT = """
import os, select, signal, struct, subprocess, sys
stopping = False
def terminate(signum, frame):
    global stopping
    stopping = True
signal.signal(signal.SIGTERM, terminate)
child = None
try:
    child = subprocess.Popen(
        [sys.executable, '-I', '-u', '-c', GRANDCHILD],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    if not select.select([child.stdout], [], [], 2.0)[0]:
        raise RuntimeError('grandchild did not become ready')
    identity = os.read(child.stdout.fileno(), 24)
    if len(identity) != 24:
        raise RuntimeError('grandchild readiness frame is incomplete')
    pid, ppid, pgid = struct.unpack('!QQQ', identity)
    if (pid, ppid, pgid) != (child.pid, os.getpid(), child.pid):
        raise RuntimeError('grandchild identity differs from its owned handle')
    os.write(1, struct.pack('!QQQ', os.getpid(), os.getppid(), os.getpgrp()) + identity)
    # Fail and reap locally before the runner's 2 s TERM grace expires.
    result = child.wait(timeout=1.5)
    raise SystemExit(0 if stopping and result == 0 else 22)
finally:
    if child is not None:
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=0.5)
        child.stdout.close()
""".replace("GRANDCHILD", repr(_GRANDCHILD))


def test_timeout_cleanup_reaps_owned_parent_and_detached_grandchild():
    if not sys.platform.startswith("linux"):
        pytest.skip("this process-table experiment requires Linux procfs")
    bounded._require_posix_execution()
    assert len(_PARENT.encode("utf-8")) < 32 * 1024
    started = time.monotonic()
    process = subprocess.Popen(
        [sys.executable, "-I", "-u", "-c", _PARENT],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    identities = ()
    try:
        assert select.select([process.stdout], [], [], 2.0)[0], "parent readiness timed out"
        payload = os.read(process.stdout.fileno(), 48)
        assert len(payload) == 48, "parent exited or sent incomplete readiness"
        parent, parent_ppid, parent_pgid, child, child_ppid, child_pgid = struct.unpack("!QQQQQQ", payload)
        identities = (parent, child)
        assert parent == process.pid and parent_ppid == os.getpid()
        assert child_ppid == parent and child != parent
        assert parent_pgid == parent and child_pgid == child
        assert os.getpid() not in identities and os.getpgrp() not in identities
        records = bounded._read_process_snapshot()
        tracker = bounded._TrackedProcessTree(process.pid)
        active = tracker.refresh(records, leader_alive=process.poll() is None)
        assert set(active) == set(identities)
        assert tracker.known_pgids == {parent: parent, child: child}
        with pytest.raises(subprocess.TimeoutExpired):
            process.wait(timeout=0.1)
        bounded._terminate_process_tree(process)
        assert process.wait(timeout=1.0) == 0, "normal cleanup did not reap the detached child"
        assert time.monotonic() - started < 5.0
    finally:
        if process.poll() is None:
            bounded._terminate_process_tree(process)
        process.wait(timeout=1.0)
        process.stdout.close()
    assert len(identities) == 2
    for pid in identities:
        assert not (Path("/proc") / str(pid)).exists(), "owned PID remains, including a zombie"
        with pytest.raises(ProcessLookupError):
            os.killpg(pid, 0)
