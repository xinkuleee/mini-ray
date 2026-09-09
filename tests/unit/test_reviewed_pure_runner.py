"""Retained JSON/path/env contracts from the retired historical pure runner."""

import json
from pathlib import Path
import pytest

from scripts import run_baseline as runner

pytestmark = pytest.mark.unit
_WHOLE = "tests/unit/test_bounded_runner.py"
_SMOKE = "tests/integration/test_task_path.py::test_one_node_one_worker_task_path"


def _data():
    return {"schema_version": 2, "edition": "base", "pure": [_WHOLE],
            "smoke": [{"selector": _SMOKE, "marker": "multiprocess_smoke"}],
            "known_non_unit_in_pure": [], "reviewed_migrations": []}


@pytest.mark.parametrize("body", ("not JSON", '{"schema_version":2,"schema_version":2}', '{}'))
def test_malformed_json_and_duplicate_fields_prevent_child(monkeypatch, body):
    class ManifestInput:
        def read_text(self, *, encoding):
            assert encoding == "utf-8"
            return body
    monkeypatch.setattr(runner, "MANIFEST_PATH", ManifestInput())
    monkeypatch.setattr(runner.bounded, "run_pytest", lambda *a, **kw: pytest.fail("unexpected child"))
    with pytest.raises(SystemExit) as exc:
        runner.main(["--pure"])
    assert exc.value.code == 2


@pytest.mark.parametrize("selector", ("", "tests/unit", "tests/unit/*.py", "../test_outside.py",
    "/tmp/test_outside.py", "-m heavy", _WHOLE + "::", _WHOLE + "::test_x\nother"))
def test_directory_glob_option_and_malformed_selectors_fail_closed(selector):
    with pytest.raises(ValueError):
        runner._selector(selector, runner.PROJECT_ROOT)


def test_missing_path_rejects_without_import():
    with pytest.raises(ValueError, match="missing or outside"):
        runner._selector("tests/unit/test_nonexistent_reviewed_fixture.py", runner.PROJECT_ROOT)


def test_known_exclusions_are_exact_unique_cases_in_pure_files():
    data = _data()
    exact = _WHOLE + "::test_execution_platform_requires_posix_cleanup_capabilities"
    data["known_non_unit_in_pure"] = [exact]
    assert runner._validate_manifest(data).known_non_unit_in_pure == (exact,)
    for exclusions in ([exact, exact], [_WHOLE], [_SMOKE]):
        data["known_non_unit_in_pure"] = exclusions
        with pytest.raises(ValueError):
            runner._validate_manifest(data)


def test_environment_is_detached_and_cannot_inject_pytest_options_or_plugins():
    ambient = {"PATH": "/task/bin", "PYTEST_ADDOPTS": "-n auto tests", "PYTEST_PLUGINS": "external",
               "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "0", "TASK_NOTE": "preserve"}
    before = dict(ambient)
    assert runner.bounded._child_environment(ambient) == {
        "PATH": "/task/bin", "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1", "TASK_NOTE": "preserve"}
    assert ambient == before


@pytest.mark.parametrize("arguments", (["tests/unit"], ["-m", "unit"], ["--list", "--maxfail=1"],
    ["--collect-only"], ["--li"], ["--manifest", "another.json"], ["--timeout", "60"]))
def test_cli_cannot_override_manifest_marker_or_budget(arguments):
    with pytest.raises(SystemExit) as exc:
        runner.main(arguments)
    assert exc.value.code == 2