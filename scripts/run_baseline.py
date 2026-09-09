"""Run the first-stage explicit baseline; never discover or run all smokes.

The manifest is selection, not test evidence. Execution reuses the existing
POSIX process-tree boundary; listing never imports test modules or starts work.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import subprocess
import sys

if __package__:
    from . import run_bounded_test as bounded
else:
    import run_bounded_test as bounded


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = Path(__file__).with_name("baseline_manifest.json")
_PURE_FILE = re.compile(r"tests/unit/test_[A-Za-z0-9_]+\.py")


@dataclass(frozen=True)
class BaselineManifest:
    pure: tuple[str, ...]
    smoke: tuple[str, ...]


def _validate_manifest(data: object) -> BaselineManifest:
    if type(data) is not dict or set(data) != {"schema_version", "pure", "smoke"}:
        raise ValueError("baseline manifest requires schema_version, pure, and smoke")
    if type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise ValueError("unsupported baseline manifest schema")
    root = PROJECT_ROOT.resolve()
    selected = {}
    for mode in ("pure", "smoke"):
        values = data[mode]
        if type(values) is not list or not values or any(type(item) is not str for item in values):
            raise ValueError("each baseline mode needs a nonempty explicit selector list")
        if len(values) != len(set(values)):
            raise ValueError("baseline selectors must be unique within their mode")
        for item in values:
            if mode == "pure":
                if _PURE_FILE.fullmatch(item) is None:
                    raise ValueError("pure baseline requires exact unit test files")
            elif "::" not in item or item not in bounded.ALLOWED_NODE_IDS | bounded.ALLOWED_LOOPBACK_NODE_IDS:
                raise ValueError("smoke baseline requires an exact bounded-runner selector")
            target = (root / item.split("::", 1)[0]).resolve()
            if root not in target.parents or not target.is_file():
                raise ValueError("baseline test file is missing or outside the project")
        selected[mode] = tuple(values)
    return BaselineManifest(**selected)


def _load_manifest() -> BaselineManifest:
    return _validate_manifest(json.loads(MANIFEST_PATH.read_text(encoding="utf-8")))


def _run_pure(manifest: BaselineManifest) -> int:
    bounded._require_posix_execution()
    if not manifest.pure:
        raise ValueError("pure execution requires explicit selectors")
    command = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
               "-m", "unit", *manifest.pure]
    process = subprocess.Popen(
        command, cwd=str(PROJECT_ROOT), start_new_session=True,
        env=bounded._child_environment(os.environ),
    )
    try:
        return process.wait(timeout=bounded.TEST_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        print("Baseline pure batch exceeded its bounded execution deadline.", file=sys.stderr)
        bounded._terminate_process_tree(process)
        return bounded.TIMEOUT_EXIT_CODE
    except BaseException:
        bounded._terminate_process_tree(process)
        raise


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--list", action="store_true", help="list static selectors without execution")
    mode.add_argument("--pure", action="store_true", help="run the single bounded pure batch")
    mode.add_argument("--smoke", metavar="EXACT", help="run one baseline smoke by its complete selector")
    args = parser.parse_args(argv)
    try:
        manifest = _load_manifest()
    except (OSError, ValueError, TypeError) as exc:
        parser.error("invalid baseline manifest: {}".format(exc))
    if args.list:
        print("First-stage baseline selectors; listing is not acceptance evidence.")
        for name, selectors in (("pure", manifest.pure), ("smoke", manifest.smoke)):
            print("{} ({}):".format(name, len(selectors)))
            for selector in selectors:
                print("  " + selector)
        return 0
    if args.smoke is not None:
        if args.smoke not in manifest.smoke:
            parser.error("--smoke requires one exact selector from the baseline manifest")
        return bounded.main([args.smoke])
    try:
        return _run_pure(manifest)
    except RuntimeError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
