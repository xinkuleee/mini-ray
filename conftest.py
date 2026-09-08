"""Reject accidental broad pytest collection while safety review is incomplete.

This hook does not select, skip or import test modules. Explicit files are a
deliberate opt-in, not a safety certificate: their imports/fixtures still need
review. The reviewed-pure runner supplies a fixed, reviewed file/node list.
"""

from pathlib import Path

import pytest


_PROJECT_ROOT = Path(__file__).resolve().parent
_SCOPE_ERROR = (
    "mini-ray's complete default test gate has not passed safety review. "
    "Use python scripts/run_reviewed_pure.py --list, then the reviewed-pure "
    "runner, or explicitly name previously reviewed test files/node IDs. "
    "Directory/package collection is disabled; -m unit or --collect-only "
    "alone does not make imports/fixtures safe."
)


def _validate_explicit_scope(selectors, invocation_dir):
    if not selectors:
        raise pytest.UsageError(_SCOPE_ERROR)
    tests_root = (_PROJECT_ROOT / "tests").resolve()
    for selector in selectors:
        relative = selector.split("::", 1)[0]
        if not relative or any(character in relative for character in "*?[]"):
            raise pytest.UsageError(_SCOPE_ERROR)
        path = (Path(invocation_dir) / relative).resolve()
        if (tests_root not in path.parents or path.suffix != ".py"
                or not path.is_file()):
            raise pytest.UsageError(_SCOPE_ERROR + " Invalid file: " + relative)


def pytest_configure(config):
    # Configuring a command happens before test-module collection. Inspection
    # commands need no test scope and must not launch or collect a test.
    if getattr(config.option, "help", False) or getattr(config.option, "version", False):
        return
    if getattr(config.option, "pyargs", False):
        raise pytest.UsageError(_SCOPE_ERROR)
    _validate_explicit_scope(config.option.file_or_dir, config.invocation_params.dir)
