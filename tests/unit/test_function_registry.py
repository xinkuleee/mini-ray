"""GCS function-authority contracts, with explicit execution classes.

The seven synchronous contracts are unit tests. The original concurrency
case is opt-in L1: exactly two owned request threads use a bounded Barrier,
one actual registry/RLock and a tiny immutable payload. There is no GCS
server, function execution, socket, timer or process. One exact node ID
must run through the 30-second bounded runner after static review.
Do not restore a module-level unit marker: pytest markers are additive.
"""

from __future__ import annotations

import hashlib
import multiprocessing.process
import queue
import socket
import subprocess
import threading
import time

import pytest

from miniray import protocol
from miniray.control import (
    FunctionRegistrationConflictError as ControlConflictError,
    FunctionRegistry as ControlFunctionRegistry,
    FunctionSnapshot as ControlFunctionSnapshot,
    UnknownFunctionError as ControlUnknownFunctionError,
)
from miniray.function_registry import (
    FunctionRegistrationConflictError,
    FunctionRegistry,
    FunctionSnapshot,
    UnknownFunctionError,
)
from miniray.ids import JobID


def _key(name: str = "function") -> protocol.FunctionKey:
    return protocol.FunctionKey(
        JobID(bytes.fromhex("17" * 16)), __name__, name, "v1"
    )


@pytest.mark.unit
def test_register_is_exactly_idempotent() -> None:
    registry = FunctionRegistry()

    assert registry.register(_key(), bytearray(b"payload"))
    assert not registry.register(_key(), memoryview(b"payload"))
    assert registry.contains(_key())
    assert registry.get(_key()) == b"payload"


@pytest.mark.unit
def test_conflicting_registration_preserves_original_payload() -> None:
    registry = FunctionRegistry()
    key = _key()
    registry.register(key, b"original")

    with pytest.raises(
        FunctionRegistrationConflictError, match="different payload"
    ):
        registry.register(key, b"replacement")

    assert registry.get(key) == b"original"


@pytest.mark.unit
def test_unknown_lookup_raises_typed_error() -> None:
    with pytest.raises(UnknownFunctionError, match="unknown function"):
        FunctionRegistry().get(_key("missing"))


@pytest.mark.unit
def test_definition_round_trip_rebuilds_the_checksum() -> None:
    registry = FunctionRegistry()
    definition = protocol.FunctionDefinition.from_payload(_key(), b"callable")

    assert registry.register_definition(definition)
    assert registry.get_definition(definition.key) == definition
    assert not registry.register_definition(definition)


@pytest.mark.unit
def test_function_definition_rejects_a_drifted_checksum() -> None:
    with pytest.raises(protocol.ProtocolError, match="checksum mismatch"):
        protocol.FunctionDefinition(_key(), b"callable", "0" * 64)


@pytest.mark.unit
def test_snapshot_is_stable_payload_free_metadata() -> None:
    registry = FunctionRegistry()
    registry.register("zeta", b"z")
    registry.register("alpha", b"alpha")

    assert registry.snapshot() == (
        FunctionSnapshot(
            "alpha",
            len(b"alpha"),
            hashlib.sha256(b"alpha").hexdigest(),
        ),
        FunctionSnapshot(
            "zeta", len(b"z"), hashlib.sha256(b"z").hexdigest()
        ),
    )


@pytest.mark.loopback_smoke
def test_concurrent_identical_registration_creates_exactly_once(monkeypatch) -> None:
    """Two concurrent callers; not a live GCS or execution-cache test.

    Each caller reaches the same Barrier before invoking the unchanged
    register method. There is no test lock around registry work and no claim
    that one particular low-level Lock.acquire schedule was observed.
    """
    from miniray import transport

    registry = FunctionRegistry()
    identity = _key()
    original_lock = registry._lock
    barrier = threading.Barrier(3, timeout=1.0)
    outcomes = queue.Queue(maxsize=2)
    errors = queue.Queue(maxsize=4)
    overflow = False
    baseline = set(threading.enumerate())
    threads = ()
    attempted_starts = []
    real_start = threading.Thread.start

    def retain_error(error):
        nonlocal overflow
        try:
            errors.put_nowait(error)
        except queue.Full:
            overflow = True

    def forbidden(*_args, **_kwargs):
        error = AssertionError("registry L1 attempted runtime infrastructure")
        retain_error(error)
        raise error

    def start(thread):
        if (len(threads) != 2 or not any(thread is item for item in threads)
                or any(thread is item for item in attempted_starts)):
            forbidden()
        attempted_starts.append(thread)
        return real_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start)
    monkeypatch.setattr(threading.Timer, "__init__", forbidden)
    monkeypatch.setattr(multiprocessing.process.BaseProcess, "start", forbidden)
    monkeypatch.setattr(multiprocessing.process.BaseProcess, "join", forbidden)
    monkeypatch.setattr(transport.TCPServer, "__init__", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    for name in ("socket", "socketpair", "create_connection"):
        monkeypatch.setattr(socket, name, forbidden)
    monkeypatch.setattr(time, "sleep", forbidden)

    def register() -> None:
        try:
            barrier.wait(timeout=1.0)
            created = registry.register(identity, b"payload")
            outcomes.put_nowait((threading.current_thread(), created))
        except BaseException as exc:
            retain_error(exc)

    threads = tuple(threading.Thread(
        target=register, name="miniray-test-function-register-{}".format(index), daemon=True,
    ) for index in range(2))
    try:
        for thread in threads:
            thread.start()
        barrier.wait(timeout=1.0)
        deadline = time.monotonic() + 2.0
        for thread in threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))
        assert not any(thread.is_alive() for thread in threads)
        assert errors.empty() and not overflow
        observed = tuple(outcomes.get_nowait() for _ in range(2))
        for _ in observed:
            outcomes.task_done()
        assert outcomes.empty() and outcomes.unfinished_tasks == 0
        assert {thread for thread, _ in observed} == set(threads)
        assert sorted(created for _, created in observed) == [False, True]
        assert registry._lock is original_lock
        assert registry.get(identity) == b"payload"
        assert registry.snapshot() == (FunctionSnapshot(
            identity, len(b"payload"), hashlib.sha256(b"payload").hexdigest(),
        ),)
    finally:
        barrier.abort()
        deadline = time.monotonic() + 1.0
        for thread in threads:
            if thread.ident is not None:
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
        # Do not reacquire a registry lock if a failed daemon owns it. Exact
        # thread joins, not metadata clearing, are the cleanup authority here.
        assert not any(thread.is_alive() for thread in threads)
        assert not (set(threading.enumerate()) - baseline)
        assert errors.empty() and not overflow


@pytest.mark.unit
def test_control_module_preserves_compatibility_exports() -> None:
    assert ControlFunctionRegistry is FunctionRegistry
    assert ControlFunctionSnapshot is FunctionSnapshot
    assert ControlConflictError is FunctionRegistrationConflictError
    assert ControlUnknownFunctionError is UnknownFunctionError
