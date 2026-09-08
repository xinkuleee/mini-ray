"""Pure validation for the exact ``init`` startup rollback seam."""

from __future__ import annotations

import signal
from types import SimpleNamespace

import pytest

import miniray as ray
from miniray import api, protocol
from miniray.ids import NodeID, WorkerID


pytestmark = pytest.mark.unit


class _StartupConnection:
    def __init__(self, reply: object = None) -> None:
        self.reply = reply
        self.close_calls = 0

    def poll(self, _timeout: float) -> bool:
        return True

    def recv(self) -> object:
        return self.reply

    def close(self) -> None:
        self.close_calls += 1


class _StartupProcess:
    def __init__(self, pid: int, *, start_error: BaseException | None = None) -> None:
        self.pid = pid
        self.exitcode = None
        self.sentinel = object()
        self.start_error = start_error
        self.start_calls = 0

    def start(self) -> None:
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error

    def join(self, _timeout: float = 0) -> None:
        return None

    def is_alive(self) -> bool:
        return self.start_calls > 0 and self.start_error is None

    def terminate(self) -> None:
        self.exitcode = -signal.SIGTERM

    def close(self) -> None:
        return None


def _startup() -> protocol.NodeStartup:
    return protocol.NodeStartup(
        NodeID.random(),
        4001,
        ("127.0.0.1", 14001),
        (WorkerID.random(),),
        (4101,),
        (("127.0.0.1", 14101),),
    )


def test_gcs_process_constructor_failure_closes_pipe_and_trace_collector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receive = _StartupConnection()
    send = _StartupConnection()
    collector = SimpleNamespace(
        start=lambda: ("127.0.0.1", 14999),
        stop_calls=0,
    )

    def stop_collector() -> None:
        collector.stop_calls += 1

    collector.stop = stop_collector

    class _Context:
        def Pipe(self, *, duplex: bool):
            assert not duplex
            return receive, send

        def Process(self, **_kwargs: object):
            raise RuntimeError("injected GCS Process constructor failure")

    monkeypatch.setattr(api, "TraceCollector", lambda: collector)
    monkeypatch.setattr(api.mp, "get_context", lambda method: _Context())

    with pytest.raises(RuntimeError, match="GCS Process constructor failure"):
        ray.init(enable_tracing=True)

    assert receive.close_calls == 1
    assert send.close_calls == 1
    assert collector.stop_calls == 1
    assert not ray.is_initialized()


def test_trace_collector_start_failure_stops_constructed_collector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = SimpleNamespace(stop_calls=0)

    def start_collector() -> object:
        raise RuntimeError("trace collector start failed")

    def stop_collector() -> None:
        collector.stop_calls += 1

    collector.start = start_collector
    collector.stop = stop_collector
    monkeypatch.setattr(api, "TraceCollector", lambda: collector)
    monkeypatch.setattr(
        api.mp,
        "get_context",
        lambda _method: pytest.fail("collector failure must precede context"),
    )

    with pytest.raises(RuntimeError, match="trace collector start failed"):
        ray.init(enable_tracing=True)

    assert collector.stop_calls == 1
    assert not ray.is_initialized()


def test_gcs_pipe_constructor_failure_stops_trace_collector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = SimpleNamespace(
        start=lambda: ("127.0.0.1", 14999),
        stop_calls=0,
    )

    def stop_collector() -> None:
        collector.stop_calls += 1

    collector.stop = stop_collector

    class _Context:
        def Pipe(self, *, duplex: bool):
            assert not duplex
            raise RuntimeError("GCS Pipe constructor failed")

        def Process(self, **_kwargs: object):
            pytest.fail("Pipe failure must precede Process construction")

    monkeypatch.setattr(api, "TraceCollector", lambda: collector)
    monkeypatch.setattr(api.mp, "get_context", lambda method: _Context())

    with pytest.raises(RuntimeError, match="GCS Pipe constructor failed"):
        ray.init(enable_tracing=True)

    assert collector.stop_calls == 1
    assert not ray.is_initialized()


def test_gcs_start_failure_closes_process_pipe_and_trace_collector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receive = _StartupConnection()
    send = _StartupConnection()
    process = _StartupProcess(4000, start_error=RuntimeError("GCS start failed"))
    collector = SimpleNamespace(
        start=lambda: ("127.0.0.1", 14999),
        stop_calls=0,
    )

    def stop_collector() -> None:
        collector.stop_calls += 1

    collector.stop = stop_collector

    class _Context:
        def Pipe(self, *, duplex: bool):
            assert not duplex
            return receive, send

        def Process(self, **_kwargs: object):
            return process

    force_stops: list[object] = []
    closed: list[object] = []
    monkeypatch.setattr(api, "TraceCollector", lambda: collector)
    monkeypatch.setattr(api.mp, "get_context", lambda method: _Context())
    monkeypatch.setattr(
        api,
        "_force_stop_process",
        lambda candidate: force_stops.append(candidate),
    )
    monkeypatch.setattr(api, "_close_process", closed.append)

    with pytest.raises(RuntimeError, match="GCS start failed"):
        ray.init(enable_tracing=True)

    assert force_stops == [process]
    assert closed == [process]
    assert receive.close_calls == 1
    assert send.close_calls == 1
    assert collector.stop_calls == 1
    assert not ray.is_initialized()


def test_node_process_constructor_failure_closes_current_and_gcs_pipes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gcs_startup = protocol.GCSStartup(4000, ("127.0.0.1", 14000))
    gcs_receive = _StartupConnection((True, gcs_startup))
    gcs_send = _StartupConnection()
    node_receive = _StartupConnection()
    node_send = _StartupConnection()
    gcs_process = _StartupProcess(gcs_startup.gcs_pid)

    class _Context:
        pipe_calls = 0
        process_calls = 0

        def Pipe(self, *, duplex: bool):
            assert not duplex
            self.pipe_calls += 1
            if self.pipe_calls == 1:
                return gcs_receive, gcs_send
            return node_receive, node_send

        def Process(self, **_kwargs: object):
            self.process_calls += 1
            if self.process_calls == 1:
                return gcs_process
            raise RuntimeError("injected Node Process constructor failure")

    gcs_rollbacks: list[object] = []
    monkeypatch.setattr(api.mp, "get_context", lambda method: _Context())
    monkeypatch.setattr(
        api,
        "_shutdown_gcs",
        lambda process, startup: gcs_rollbacks.append((process, startup)),
    )

    with pytest.raises(RuntimeError, match="Node Process constructor failure"):
        ray.init(enable_tracing=False)

    assert node_receive.close_calls == 1
    assert node_send.close_calls == 1
    assert gcs_receive.close_calls == 1
    assert gcs_send.close_calls == 2
    assert gcs_rollbacks == [(gcs_process, gcs_startup)]
    assert not ray.is_initialized()


def test_node_start_failure_reaps_unpublished_process_and_closes_all_pipes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gcs_startup = protocol.GCSStartup(4000, ("127.0.0.1", 14000))
    gcs_receive = _StartupConnection((True, gcs_startup))
    gcs_send = _StartupConnection()
    node_receive = _StartupConnection()
    node_send = _StartupConnection()
    gcs_process = _StartupProcess(gcs_startup.gcs_pid)
    node_process = _StartupProcess(4100, start_error=RuntimeError("Node start failed"))

    class _Context:
        pipe_calls = 0
        process_calls = 0

        def Pipe(self, *, duplex: bool):
            assert not duplex
            self.pipe_calls += 1
            if self.pipe_calls == 1:
                return gcs_receive, gcs_send
            return node_receive, node_send

        def Process(self, **_kwargs: object):
            self.process_calls += 1
            return gcs_process if self.process_calls == 1 else node_process

    force_stops: list[tuple[object, bool]] = []
    process_closes: list[object] = []
    gcs_rollbacks: list[object] = []
    monkeypatch.setattr(api.mp, "get_context", lambda method: _Context())
    monkeypatch.setattr(
        api,
        "_force_stop_process",
        lambda process, *, process_group=False: force_stops.append(
            (process, process_group)
        ),
    )
    monkeypatch.setattr(api, "_close_process", process_closes.append)
    monkeypatch.setattr(
        api,
        "_shutdown_gcs",
        lambda process, startup: gcs_rollbacks.append((process, startup)),
    )

    with pytest.raises(RuntimeError, match="Node start failed"):
        ray.init(enable_tracing=False)

    assert force_stops == [(node_process, True)]
    assert process_closes == [node_process]
    assert gcs_rollbacks == [(gcs_process, gcs_startup)]
    assert node_receive.close_calls == 1
    assert node_send.close_calls == 1
    assert gcs_receive.close_calls == 1
    assert gcs_send.close_calls == 2
    assert not ray.is_initialized()


def test_cleanup_failure_cannot_skip_later_gcs_collector_or_pipe_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receive = _StartupConnection()
    send = _StartupConnection()
    process = _StartupProcess(4000, start_error=RuntimeError("start failed"))
    collector = SimpleNamespace(
        start=lambda: ("127.0.0.1", 14999),
        stop_calls=0,
    )

    def stop_collector() -> None:
        collector.stop_calls += 1

    collector.stop = stop_collector

    class _Context:
        def Pipe(self, *, duplex: bool):
            return receive, send

        def Process(self, **_kwargs: object):
            return process

    close_calls: list[object] = []
    monkeypatch.setattr(api, "TraceCollector", lambda: collector)
    monkeypatch.setattr(api.mp, "get_context", lambda method: _Context())
    monkeypatch.setattr(
        api,
        "_force_stop_process",
        lambda candidate: (_ for _ in ()).throw(RuntimeError("cleanup failed")),
    )
    monkeypatch.setattr(api, "_close_process", close_calls.append)

    with pytest.raises(RuntimeError, match="start failed"):
        ray.init(enable_tracing=True)

    assert close_calls == [process]
    assert collector.stop_calls == 1
    assert receive.close_calls == 1
    assert send.close_calls == 1
    assert not ray.is_initialized()


def test_startup_checkpoint_fails_only_at_the_exact_node_index() -> None:
    startup = _startup()
    api._startup_node_ready_checkpoint(0, startup, None)
    api._startup_node_ready_checkpoint(0, startup, 1)
    with pytest.raises(RuntimeError, match="node 1 reported readiness"):
        api._startup_node_ready_checkpoint(1, startup, 1)


@pytest.mark.parametrize(
    "value, error",
    ((True, TypeError), ("1", TypeError), (-1, ValueError), (2, ValueError)),
)
def test_init_rejects_invalid_startup_checkpoint_without_starting(
    value: object, error: type[Exception]
) -> None:
    with pytest.raises(error, match="_test_fail_after_node_ready"):
        ray.init(
            num_nodes=2,
            enable_tracing=False,
            _test_fail_after_node_ready=value,
        )
    assert not ray.is_initialized()


@pytest.mark.parametrize("denied_signal", (signal.SIGTERM, signal.SIGKILL))
def test_process_group_permission_race_never_escapes_or_broadens_target(
    denied_signal: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    process_group_id = 43210
    calls: list[tuple[int, int]] = []

    def killpg(target: int, sent_signal: int) -> None:
        calls.append((target, sent_signal))
        if sent_signal == denied_signal:
            raise PermissionError("Darwin terminal process-group race")

    monkeypatch.setattr(api.os, "killpg", killpg)
    monkeypatch.setattr(api, "_TERMINATE_GRACE_SECONDS", 0.0)

    # The cleanup syscall race must not prevent rollback of later Nodes/GCS.
    api._terminate_process_group(process_group_id)
    later_cleanup_ran = True

    assert later_cleanup_ran
    assert calls[0] == (process_group_id, signal.SIGTERM)
    assert (process_group_id, signal.SIGKILL) in calls
    assert {target for target, _sent_signal in calls} == {process_group_id}


class _PermissionRaceProcess:
    pid = 43210

    def __init__(self) -> None:
        self.alive = True
        self.exitcode = None
        self.join_calls = 0
        self.terminate_calls = 0

    def join(self, _timeout: float) -> None:
        self.join_calls += 1
        # First join is the graceful pre-signal wait.  After group SIGTERM and
        # the denied SIGKILL, the second join reaps the exact managed Node.
        if self.join_calls == 2:
            self.alive = False
            self.exitcode = -signal.SIGTERM

    def is_alive(self) -> bool:
        return self.alive

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.alive = False
        self.exitcode = -signal.SIGTERM


def test_force_stop_reaps_child_and_allows_later_rollback_after_killpg_eperm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = _PermissionRaceProcess()
    signals: list[tuple[int, int]] = []
    rollback_events: list[str] = []

    def killpg(target: int, sent_signal: int) -> None:
        signals.append((target, sent_signal))
        if sent_signal == signal.SIGKILL:
            raise PermissionError("Darwin terminal process-group race")

    monkeypatch.setattr(api.os, "killpg", killpg)
    monkeypatch.setattr(api, "_TERMINATE_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(
        api, "_process_group_exists", lambda target: target == process.pid
    )

    assert api._force_stop_process(process, process_group=True)
    rollback_events.append("later-gcs-cleanup")

    assert signals == [
        (process.pid, signal.SIGTERM),
        (process.pid, signal.SIGKILL),
    ]
    assert not process.is_alive()
    assert process.join_calls == 2
    assert process.terminate_calls == 0
    assert rollback_events == ["later-gcs-cleanup"]


@pytest.mark.parametrize("unsafe", (0, -1, True, "43210"))
def test_process_group_cleanup_rejects_non_exact_signal_targets(unsafe: object) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        api._terminate_process_group(unsafe)  # type: ignore[arg-type]
