"""Pure marker dispatch and isolation contracts; every process effect is fake."""

from types import SimpleNamespace
import pytest

from scripts import run_baseline as runner
from scripts import _test_process as process_kernel

pytestmark = pytest.mark.unit
_PURE = "tests/unit/test_single_output_contract.py"
_MP = "tests/integration/test_task_path.py::test_one_node_one_worker_task_path"


@pytest.mark.parametrize("marker", ("unit", "loopback_smoke", "multiprocess_smoke"))
@pytest.mark.parametrize("outcome", ("passed", "failed", "collection-error", "no-tests", "timeout", "interrupt", "wait-error"))
def test_modes_share_isolated_deadline_environment_and_cleanup(monkeypatch, marker, outcome):
    launches, cleaned = [], []
    class Child:
        pid = 12345
        def wait(self, timeout):
            assert timeout == process_kernel.TEST_TIMEOUT_SECONDS == 30.0
            if outcome == "timeout":
                raise process_kernel.subprocess.TimeoutExpired("pytest", timeout)
            if outcome == "interrupt":
                raise KeyboardInterrupt()
            if outcome == "wait-error":
                raise RuntimeError("wait failed")
            return {"passed": 0, "failed": 1, "collection-error": 2, "no-tests": 5}[outcome]
    child = Child()
    monkeypatch.setattr(process_kernel, "_require_posix_execution", lambda: None)
    monkeypatch.setattr(process_kernel.subprocess, "Popen", lambda command, **kw: launches.append((command, kw)) or child)
    monkeypatch.setattr(process_kernel, "_terminate_process_tree", cleaned.append)
    monkeypatch.setenv("PYTEST_ADDOPTS", "-n auto tests")
    monkeypatch.setenv("PYTEST_PLUGINS", "unreviewed")
    if outcome in {"interrupt", "wait-error"}:
        with pytest.raises(KeyboardInterrupt if outcome == "interrupt" else RuntimeError):
            process_kernel.run_pytest((_MP,), marker, root=runner.PROJECT_ROOT)
    else:
        assert process_kernel.run_pytest((_MP,), marker, root=runner.PROJECT_ROOT) == {
            "passed": 0, "failed": 1, "collection-error": 2, "no-tests": 5, "timeout": 124}[outcome]
    (command, options), = launches
    assert command == [process_kernel.sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-m", marker, _MP]
    assert options["start_new_session"] and options["cwd"] == str(runner.PROJECT_ROOT)
    assert options["env"]["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    assert "PYTEST_ADDOPTS" not in options["env"] and "PYTEST_PLUGINS" not in options["env"]
    assert cleaned == ([child] if outcome in {"timeout", "interrupt", "wait-error"} else [])


def test_empty_selection_and_unknown_marker_cannot_start_child(monkeypatch):
    monkeypatch.setattr(process_kernel.subprocess, "Popen", lambda *a, **kw: pytest.fail("unexpected child"))
    for selectors, marker in (((), "unit"), ((_MP,), "heavy")):
        with pytest.raises(ValueError):
            process_kernel.run_pytest(selectors, marker, root=runner.PROJECT_ROOT)


def test_native_windows_fails_before_launch_or_process_inspection(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("unsupported execution caused a process effect")
    monkeypatch.setattr(process_kernel, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(process_kernel.subprocess, "Popen", forbidden)
    monkeypatch.setattr(process_kernel.subprocess, "run", forbidden)
    with pytest.raises(RuntimeError, match="POSIX process groups"):
        process_kernel.run_pytest((_MP,), "multiprocess_smoke", root=runner.PROJECT_ROOT)


def test_smoke_and_migration_markers_cannot_conflict_or_widen_whole_file():
    data = {"schema_version": 2, "edition": "base", "pure": [_PURE],
            "smoke": [{"selector": _MP, "marker": "multiprocess_smoke"}],
            "known_non_unit_in_pure": [], "reviewed_migrations": []}
    for marker in ("unit", "heavy", "multiprocess_smoke or unit"):
        bad = {**data, "smoke": [{"selector": _MP, "marker": marker}]}
        with pytest.raises(ValueError):
            runner._validate_manifest(bad)
    for marker in ("loopback_smoke", "multiprocess_smoke"):
        with pytest.raises(ValueError):
            runner._selection({"selector": _PURE, "marker": marker}, runner.PROJECT_ROOT)


def test_known_nonunit_remains_documented_and_not_silently_promoted():
    manifest = runner._load_manifest()
    excluded = "tests/unit/test_owner_reconstruction.py::test_concurrent_exact_requests_commit_once_and_replay_one_reply"
    assert excluded in manifest.known_non_unit_in_pure
    assert all(item.selector != excluded for item in manifest.smoke)
    with pytest.raises(ValueError, match="registered"):
        runner._case(manifest, excluded)