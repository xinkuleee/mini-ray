"""Baseline selection and delegation with fake process boundaries only."""

import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import run_baseline as runner


pytestmark = pytest.mark.unit
_PURE = "tests/unit/test_single_output_contract.py"
_SMOKE = "tests/integration/test_task_path.py::test_one_node_one_worker_task_path"


def _manifest():
    return runner.BaselineManifest((_PURE,), (_SMOKE,))


def test_manifest_uses_explicit_paths_and_rejects_widened_selection():
    assert runner._validate_manifest({"schema_version": 1, "pure": [_PURE], "smoke": [_SMOKE]}) == _manifest()
    for bad in (
        {"pure": [], "smoke": [_SMOKE]},
        {"pure": ["tests/unit"], "smoke": [_SMOKE]},
        {"pure": ["tests/unit/*.py"], "smoke": [_SMOKE]},
        {"pure": [_PURE, _PURE], "smoke": [_SMOKE]},
        {"pure": [_PURE], "smoke": ["all"]},
        {"pure": [_PURE], "smoke": ["tests/integration/test_task_path.py"]},
    ):
        with pytest.raises(ValueError):
            runner._validate_manifest({"schema_version": 1, **bad})


def test_list_does_not_import_tests_or_start_processes(monkeypatch, capsys):
    monkeypatch.setattr(runner, "_load_manifest", _manifest)
    def forbidden(*args, **kwargs):
        pytest.fail("listing must not execute a process or inspect its tree")
    monkeypatch.setattr(runner.bounded, "_require_posix_execution", forbidden)
    monkeypatch.setattr(runner.subprocess, "Popen", forbidden)
    monkeypatch.setattr(runner.bounded, "main", forbidden)
    before = set(sys.modules)
    assert runner.main(["--list"]) == 0
    assert set(sys.modules) == before
    output = capsys.readouterr().out
    assert _PURE in output and _SMOKE in output and "not acceptance evidence" in output


def test_smoke_delegates_only_an_exact_baseline_member(monkeypatch):
    monkeypatch.setattr(runner, "_load_manifest", _manifest)
    calls = []
    monkeypatch.setattr(runner.bounded, "main", lambda args: calls.append(args) or 7)
    assert runner.main(["--smoke", _SMOKE]) == 7
    assert calls == [[_SMOKE]]
    for args in (["--smoke", "all"], ["--pure", "--smoke", _SMOKE],
                 ["--pure", "-m", "heavy"], []):
        with pytest.raises(SystemExit) as exc:
            runner.main(args)
        assert exc.value.code == 2
    assert calls == [[_SMOKE]]


def test_pure_is_one_explicit_unit_child_with_sanitized_environment(monkeypatch):
    monkeypatch.setattr(runner, "_load_manifest", _manifest)
    monkeypatch.setattr(runner.bounded, "_require_posix_execution", lambda: None)
    monkeypatch.setenv("PYTEST_ADDOPTS", "-n auto tests")
    monkeypatch.setenv("PYTEST_PLUGINS", "unreviewed")
    calls = []
    waits = []
    process = SimpleNamespace(wait=lambda timeout: waits.append(timeout) or 5)
    monkeypatch.setattr(runner.subprocess, "Popen", lambda command, **kwargs: calls.append((command, kwargs)) or process)
    assert runner.main(["--pure"]) == 5
    (command, options), = calls
    assert command == [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-m", "unit", _PURE]
    assert options["start_new_session"] and options["cwd"] == str(runner.PROJECT_ROOT)
    assert options["env"]["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    assert "PYTEST_ADDOPTS" not in options["env"] and "PYTEST_PLUGINS" not in options["env"]
    assert waits == [runner.bounded.TEST_TIMEOUT_SECONDS]


@pytest.mark.parametrize("error", [subprocess.TimeoutExpired("pytest", 30), KeyboardInterrupt()])
def test_pure_timeout_and_interruption_reuse_process_tree_cleanup(monkeypatch, error):
    monkeypatch.setattr(runner.bounded, "_require_posix_execution", lambda: None)
    def wait(timeout):
        assert timeout == runner.bounded.TEST_TIMEOUT_SECONDS
        raise error
    process = SimpleNamespace(wait=wait)
    monkeypatch.setattr(runner.subprocess, "Popen", lambda *args, **kwargs: process)
    cleaned = []
    monkeypatch.setattr(runner.bounded, "_terminate_process_tree", cleaned.append)
    if isinstance(error, subprocess.TimeoutExpired):
        assert runner._run_pure(_manifest()) == runner.bounded.TIMEOUT_EXIT_CODE
    else:
        with pytest.raises(KeyboardInterrupt):
            runner._run_pure(_manifest())
    assert cleaned == [process]


def test_unsupported_platform_rejects_before_child_creation(monkeypatch):
    monkeypatch.setattr(runner, "_load_manifest", _manifest)
    def unsupported():
        raise RuntimeError("POSIX process groups required")
    monkeypatch.setattr(runner.bounded, "_require_posix_execution", unsupported)
    monkeypatch.setattr(runner.subprocess, "Popen", lambda *args, **kwargs: pytest.fail("unexpected child"))
    with pytest.raises(SystemExit) as exc:
        runner.main(["--pure"])
    assert exc.value.code == 2
