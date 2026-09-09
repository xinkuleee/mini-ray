"""Pure pre-collection safety-policy checks; no pytest child or collection."""

from pathlib import Path
from types import SimpleNamespace

import pytest

import conftest as guard


pytestmark = pytest.mark.unit
_FILE = "tests/unit/test_ids_resources.py"


def _config(selectors=(), **options):
    values = dict(file_or_dir=list(selectors), help=False, version=False, pyargs=False)
    values.update(options)
    return SimpleNamespace(
        option=SimpleNamespace(**values),
        invocation_params=SimpleNamespace(dir=guard._PROJECT_ROOT),
    )


@pytest.mark.parametrize("selectors", (
    (), (".",), ("tests",), ("tests/unit",), ("tests/integration",),
    ("tests/unit/test_*.py",), ("tests/unit/test_missing_review.py",),
    (_FILE, "tests/integration"), ("../conftest.py",), ("conftest.py",),
))
def test_broad_or_missing_selection_fails_before_any_collection(selectors):
    with pytest.raises(pytest.UsageError, match="explicit baseline"):
        guard.pytest_configure(_config(selectors))


def test_explicit_relative_absolute_and_exact_scope_is_preserved():
    selectors = [_FILE, str(guard._PROJECT_ROOT / _FILE), _FILE + "::test_ids_are_strong_and_immutable"]
    before = tuple(selectors)
    guard.pytest_configure(_config(selectors))
    assert tuple(selectors) == before
    # Relative paths are resolved from pytest's invocation directory, not
    # silently interpreted as a new project-root discovery request.
    guard._validate_explicit_scope(("test_ids_resources.py",), guard._PROJECT_ROOT / "tests/unit")


@pytest.mark.parametrize("option", ("help", "version"))
def test_inspection_commands_do_not_require_collection_scope(option):
    guard.pytest_configure(_config(**{option: True}))


@pytest.mark.parametrize("options", ({"collectonly": True}, {"markexpr": "unit"}, {"pyargs": True}))
def test_marker_collection_probe_and_package_mode_do_not_bypass_guard(options):
    with pytest.raises(pytest.UsageError):
        guard.pytest_configure(_config(**options))
    if options.get("pyargs"):
        with pytest.raises(pytest.UsageError):
            guard.pytest_configure(_config((_FILE,), **options))


def test_symlink_escape_is_not_an_explicit_project_test(monkeypatch):
    original = Path.resolve

    def escaped(path, *args, **kwargs):
        if path == guard._PROJECT_ROOT / _FILE:
            return guard._PROJECT_ROOT.parent / "outside.py"
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", escaped)
    with pytest.raises(pytest.UsageError):
        guard.pytest_configure(_config((_FILE,)))
