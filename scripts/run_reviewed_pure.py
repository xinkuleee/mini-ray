"""Run the explicit reviewed pure subset, never the complete default gate.

Selection is data, not discovery: no test imports, globbing, collection probe,
or pytest argument passthrough occurs before the single isolated child starts.
The manifest describes historical review scope; changes to selected modules,
their fixtures/imports, or project configuration still require review.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Optional, Sequence, Tuple

if __package__:
    from . import run_bounded_test as bounded
else:
    import run_bounded_test as bounded


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = Path(__file__).with_name("reviewed_pure_manifest.json")
_FILE = re.compile(r"tests/unit/test_[A-Za-z0-9_]+\.py")
_NODE = re.compile(r"test_[A-Za-z0-9_]+(?:\[[^\[\]\r\n]+\])?")
_NOTICE = "REVIEWED PURE SUBSET — NOT the complete default unit gate."


@dataclass(frozen=True)
class ReviewedPureManifest:
    name: str
    reviewed_on: str
    review_notice: str
    evidence_source: str
    historical_passed: int
    historical_deselected: int
    historical_seconds: float
    selectors: Tuple[str, ...]
    known_excluded_node_ids: Tuple[str, ...]

    @property
    def whole_files(self) -> Tuple[str, ...]:
        return tuple(item for item in self.selectors if "::" not in item)

    @property
    def exact_node_ids(self) -> Tuple[str, ...]:
        return tuple(item for item in self.selectors if "::" in item)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("manifest contains duplicate JSON field: {}".format(key))
        result[key] = value
    return result


def _text(value: object, name: str) -> str:
    if type(value) is not str or not value.strip():
        raise ValueError("{} must be nonempty text".format(name))
    return value


def _integer(value: object, name: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError("{} must be an integer >= {}".format(name, minimum))
    return value


def _selector(value: object, root: Path, *, exact: bool = False) -> str:
    selector = _text(value, "selector")
    relative, separator, node = selector.partition("::")
    if _FILE.fullmatch(relative) is None or (separator and _NODE.fullmatch(node) is None):
        raise ValueError("selection must name an explicit tests/unit test file or exact test node: {}".format(selector))
    if exact and not separator:
        raise ValueError("exclusion must name an exact test node: {}".format(selector))
    # A path lookup never imports the module. Resolving catches symlink escapes
    # as well as misspellings before pytest could fall back to wider discovery.
    target = (root / relative).resolve()
    if root not in target.parents or not target.is_file():
        raise ValueError("selected test file is missing or outside the project: {}".format(relative))
    return selector


def _validate_manifest(data: object, *, root: Optional[Path] = None) -> ReviewedPureManifest:
    root = PROJECT_ROOT.resolve() if root is None else root.resolve()
    required = {"schema_version", "scope", "name", "reviewed_on", "review_notice",
                "marker", "historical_evidence", "whole_file_count", "exact_node_id_count",
                "selectors", "known_excluded_node_ids"}
    if type(data) is not dict or set(data) != required:
        raise ValueError("reviewed-pure manifest has missing or unknown fields")
    if type(data["schema_version"]) is not int or data["schema_version"] != 1:
        raise ValueError("unsupported reviewed-pure manifest schema")
    if data["scope"] != "reviewed-pure-subset" or data["marker"] != "unit":
        raise ValueError("manifest must remain an explicitly unit-marked reviewed subset")
    reviewed_on = _text(data["reviewed_on"], "reviewed_on")
    date.fromisoformat(reviewed_on)
    raw_selectors, raw_exclusions = data["selectors"], data["known_excluded_node_ids"]
    if type(raw_selectors) is not list or not raw_selectors:
        # pytest with no selectors discovers testpaths; never permit that.
        raise ValueError("reviewed selection must be a nonempty explicit list")
    if type(raw_exclusions) is not list:
        raise ValueError("known exclusions must be an explicit list")
    selectors = tuple(_selector(item, root) for item in raw_selectors)
    exclusions = tuple(_selector(item, root, exact=True) for item in raw_exclusions)
    if len(set(selectors)) != len(selectors) or len(set(exclusions)) != len(exclusions):
        raise ValueError("duplicate selector or exclusion in reviewed manifest")
    whole_files = {item for item in selectors if "::" not in item}
    exact_ids = tuple(item for item in selectors if "::" in item)
    if any(item.split("::", 1)[0] in whole_files for item in exact_ids):
        raise ValueError("whole-file and exact-node selectors must not overlap")
    if any(item.split("::", 1)[0] not in whole_files for item in exclusions):
        raise ValueError("known exclusions must belong to a selected whole file")
    if len(whole_files) != _integer(data["whole_file_count"], "whole_file_count"):
        raise ValueError("whole-file count does not match reviewed scope")
    if len(exact_ids) != _integer(data["exact_node_id_count"], "exact_node_id_count"):
        raise ValueError("exact-node count does not match reviewed scope")
    evidence = data["historical_evidence"]
    if type(evidence) is not dict or set(evidence) != {"source", "passed", "deselected", "seconds"}:
        raise ValueError("historical evidence must have source, passed, deselected and seconds")
    passed = _integer(evidence["passed"], "historical passed", minimum=1)
    deselected = _integer(evidence["deselected"], "historical deselected")
    seconds = evidence["seconds"]
    if type(seconds) not in (int, float) or not 0 < seconds < float("inf"):
        raise ValueError("historical seconds must be finite and positive")
    if len(exclusions) != deselected:
        raise ValueError("known exclusions must identify every recorded deselected test")
    return ReviewedPureManifest(
        _text(data["name"], "name"), reviewed_on, _text(data["review_notice"], "review_notice"),
        _text(evidence["source"], "evidence source"), passed, deselected, float(seconds), selectors, exclusions,
    )


def _load_manifest() -> ReviewedPureManifest:
    data = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    return _validate_manifest(data)


def _pytest_command(manifest: ReviewedPureManifest) -> list[str]:
    if not manifest.selectors:
        raise ValueError("cannot run pytest without explicit reviewed selectors")
    return [
        sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-m", "unit",
        *("--deselect=" + item for item in manifest.known_excluded_node_ids),
        *manifest.selectors,
    ]


def _describe(manifest: ReviewedPureManifest, *, list_selectors: bool) -> None:
    print(_NOTICE, flush=True)
    print("{}: {} whole files + {} exact nodes; reviewed {}.".format(
        manifest.name, len(manifest.whole_files), len(manifest.exact_node_ids), manifest.reviewed_on,
    ), flush=True)
    print("Historical evidence: {} passed, {} deselected in {:.2f}s ({}); not a result of this invocation.".format(
        manifest.historical_passed, manifest.historical_deselected, manifest.historical_seconds, manifest.evidence_source,
    ), flush=True)
    print(manifest.review_notice, flush=True)
    if list_selectors:
        print("Selected paths (no test modules imported):")
        for selector in manifest.selectors:
            print("  " + selector)
        print("Known non-unit exclusions (also explicitly deselected):")
        for selector in manifest.known_excluded_node_ids:
            print("  " + selector)
        print("Also excluded: all unlisted selectors and all non-unit-marked cases.")
        print("Whole-file selection still imports that reviewed module at pytest collection time.")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run only the frozen reviewed pure subset; not the full default gate.",
        allow_abbrev=False,
    )
    parser.add_argument("--list", action="store_true", help="show reviewed scope and exclusions without starting pytest")
    args = parser.parse_args(argv)
    try:
        manifest = _load_manifest()
    except (OSError, ValueError, TypeError) as exc:
        parser.error("invalid reviewed-pure manifest: {}".format(exc))
    _describe(manifest, list_selectors=args.list)
    if args.list:
        return 0
    try:
        bounded._require_posix_execution()
    except RuntimeError as exc:
        parser.error(str(exc))
    command = _pytest_command(manifest)
    process = subprocess.Popen(
        command, cwd=str(PROJECT_ROOT), start_new_session=True,
        env=bounded._child_environment(os.environ),
    )
    try:
        result = process.wait(timeout=bounded.TEST_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        print("Reviewed subset exceeded {:.0f}s execution deadline; applying bounded process-tree cleanup.".format(
            bounded.TEST_TIMEOUT_SECONDS,
        ), file=sys.stderr, flush=True)
        bounded._terminate_process_tree(process)
        return bounded.TIMEOUT_EXIT_CODE
    except BaseException:
        bounded._terminate_process_tree(process)
        raise
    print("Reviewed subset {} (pytest exit {}); complete default gate remains unverified.".format(
        "passed" if result == 0 else "did not pass", result,
    ), flush=True)
    return result


if __name__ == "__main__":
    raise SystemExit(main())
