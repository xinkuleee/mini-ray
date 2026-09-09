"""Direct resource authority contracts retained from RuntimeState facade.

One ResourceLedger, at most two allocation tokens, no Node/Worker/Core,
threads, sockets, processes, waits or user execution. These prove resource
accounting only; a loop releasing tokens is not evidence of Node death.
Existing test_cpu_yield_accounting covers CPU-only yield and unblock debt.
"""

import multiprocessing.process
import socket
import subprocess
import threading
import time

import pytest

from miniray.errors import AllocationAlreadyReleasedError, AllocationTokenError
from miniray.resources import AllocationState, AllocationToken, ResourceLedger, ResourceVector

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def no_runtime(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("ledger contract attempted runtime work")
    for kind, method in ((threading.Thread, "start"), (threading.Thread, "join"),
                         (threading.Timer, "start"), (threading.Event, "wait"),
                         (threading.Condition, "wait"), (multiprocessing.process.BaseProcess, "start")):
        monkeypatch.setattr(kind, method, forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)


@pytest.mark.parametrize("reacquire", (False, True))
def test_release_after_yield_fences_delayed_reacquire_and_token_reuse(reacquire):
    total = ResourceVector({"CPU": 1, "GPU": 1, "custom": 1})
    ledger = ResourceLedger(total)
    token = ledger.allocate(total, AllocationToken("retired-parent"))
    assert ledger.yield_cpu(token)
    if reacquire:
        assert ledger.reacquire_cpu(token)
    assert ledger.release(token)
    completed = ledger.snapshot()
    assert completed.available == total and completed.cpu_debt == 0
    assert ledger.record(token).state is AllocationState.RELEASED
    assert ledger.record(token).held_resources == ResourceVector.empty()
    assert not ledger.release(token)
    assert not ledger.reacquire_cpu(token) and not ledger.yield_cpu(token)
    with pytest.raises(AllocationAlreadyReleasedError):
        ledger.allocate(total, token)
    assert ledger.snapshot() == completed


def test_exact_allocation_request_replay_never_rebinds_resources():
    total = ResourceVector({"CPU": 2, "GPU": 1})
    request = ResourceVector({"CPU": 1})
    ledger = ResourceLedger(total)
    token = ledger.allocate(request, AllocationToken("exact-parent"))
    before = ledger.snapshot()
    assert ledger.allocate(request, token) == token and ledger.snapshot() == before
    with pytest.raises(AllocationTokenError):
        ledger.allocate(ResourceVector({"CPU": 2}), token)
    assert ledger.snapshot() == before
    assert ledger.release(token) and not ledger.release(token)
    assert ledger.available == total
