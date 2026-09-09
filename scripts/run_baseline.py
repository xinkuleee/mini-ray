"""Run this branch's gate or one reviewed migration; never discover tests."""

from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re

if __package__:
    from . import _test_process as bounded
else:
    import _test_process as bounded

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = Path(__file__).with_name("baseline_manifest.json")
_FILE = re.compile(r"tests/(?:unit|integration)/test_[A-Za-z0-9_]+\.py")
_NODE = re.compile(r"test_[A-Za-z0-9_]+(?:\[[^\[\]\r\n]+\])?")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_MARKERS = {"unit", "loopback_smoke", "multiprocess_smoke"}
_CONFIG_INPUTS = ("conftest.py", "pyproject.toml", "uv.lock",
                  "scripts/run_baseline.py", "scripts/_test_process.py")


@dataclass(frozen=True)
class Selection:
    selector: str
    marker: str


@dataclass(frozen=True)
class ReviewedMigration:
    selection: Selection
    work_package_id: str
    review_source_commit: str
    reviewed_tree_hash: str
    reviewed_files: tuple[tuple[str, str], ...]
    cost_review: str


@dataclass(frozen=True)
class BaselineManifest:
    edition: str
    pure: tuple[str, ...]
    smoke: tuple[Selection, ...]
    known_non_unit_in_pure: tuple[str, ...] = ()
    reviewed_migrations: tuple[ReviewedMigration, ...] = ()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("manifest contains duplicate JSON field: " + key)
        result[key] = value
    return result


def _text(value, name):
    if type(value) is not str or not value.strip():
        raise ValueError(name + " must be nonempty text")
    return value


def _path(relative: str, root: Path) -> Path:
    _text(relative, "path")
    if ("\\" in relative or relative.startswith("/") or ":" in relative
            or any(part in {"", ".", ".."} for part in relative.split("/"))):
        raise ValueError("input must be a canonical repository-relative path")
    target = (root / relative).resolve()
    if root not in target.parents or not target.is_file():
        raise ValueError("input file is missing or outside the project: " + relative)
    return target


def _selector(value, root: Path, *, exact=False) -> str:
    selector = _text(value, "selector")
    relative, separator, node = selector.partition("::")
    if _FILE.fullmatch(relative) is None or (separator and _NODE.fullmatch(node) is None):
        raise ValueError("selection requires an explicit test file or exact test case")
    if exact and not separator:
        raise ValueError("selection requires an exact test case")
    _path(relative, root)
    return selector


def _selection(data, root: Path, *, exact=False) -> Selection:
    if type(data) is not dict or set(data) != {"selector", "marker"}:
        raise ValueError("selection requires selector and marker only")
    marker = data["marker"]
    if type(marker) is not str or marker not in _MARKERS:
        raise ValueError("selection has an unsupported marker")
    selector = _selector(data["selector"], root, exact=exact or marker != "unit")
    if "::" not in selector and not selector.startswith("tests/unit/"):
        raise ValueError("whole-file selection requires a reviewed unit file")
    return Selection(selector, marker)


def _module_paths(module: str, root: Path) -> tuple[Path, ...]:
    """Resolve local modules and package initializers without importing them."""
    parts = module.split(".") if module else []
    found = []
    for base in (root, root / "src"):
        for end in range(1, len(parts) + 1):
            package = base.joinpath(*parts[:end], "__init__.py")
            if package.is_file():
                found.append(package)
        if parts:
            source = base.joinpath(*parts).with_suffix(".py")
            if source.is_file():
                found.append(source)
    return tuple(found)


def _import_closure(relative: str, root: Path) -> set[str]:
    """Conservative local Python import closure; no imports or collection."""
    pending = [_path(relative, root)]
    for directory in (root / relative).parents:
        if directory == root.parent:
            break
        config = directory / "conftest.py"
        if config.is_file():
            pending.append(config)
    seen = set()
    while pending:
        path = pending.pop()
        name = path.relative_to(root).as_posix()
        if name in seen:
            continue
        seen.add(name)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=name)
        parts = path.relative_to(root).with_suffix("").parts
        if parts[0] == "src":
            parts = parts[1:]
        package = parts[:-1]
        for end in range(1, len(package) + 1):
            pending.extend(_module_paths(".".join(package[:end]), root))
        for node in ast.walk(tree):
            modules = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                prefix = package[:len(package) - node.level + 1] if node.level else ()
                suffix = (node.module or "").split(".") if node.module else []
                module = ".".join((*prefix, *suffix))
                modules = [module, *(module + "." + alias.name for alias in node.names if alias.name != "*")]
            for module in modules:
                for candidate in _module_paths(module, root):
                    pending.append(_path(candidate.relative_to(root).as_posix(), root))
    return seen


def _tree_hash(files) -> str:
    payload = json.dumps(sorted(files.items()), ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def review_inputs(selector: str, *, root: Path | None = None) -> dict[str, str]:
    """Return review inputs without granting review or changing the registry.

    The manifest cannot hash its own registry bytes; selection/marker and
    metadata are validated separately. Dynamic imports and data files require
    explicit extra reviewed_files. A frozen checkout needs no .git directory.
    """
    root = (root or PROJECT_ROOT).resolve()
    relative = _selector(selector, root).split("::", 1)[0]
    paths = _import_closure(relative, root)
    for name in _CONFIG_INPUTS:
        if (root / name).is_file():
            paths.add(name)
    return {name: hashlib.sha256(_path(name, root).read_bytes()).hexdigest() for name in sorted(paths)}


def _migration(data, root: Path) -> ReviewedMigration:
    fields = {"selector", "marker", "purpose", "work_package_id", "review_source_commit",
              "reviewed_tree_hash", "reviewed_files", "cost_review"}
    if type(data) is not dict or set(data) != fields or data["purpose"] != "migration":
        raise ValueError("migration has missing, unknown or invalid fields")
    selection = _selection({key: data[key] for key in ("selector", "marker")}, root)
    commit = _text(data["review_source_commit"], "review_source_commit")
    if _COMMIT.fullmatch(commit) is None:
        raise ValueError("review_source_commit requires a full commit SHA")
    files = data["reviewed_files"]
    if type(files) is not dict or not files:
        raise ValueError("reviewed_files must be a nonempty input/hash mapping")
    for relative, digest in files.items():
        if type(digest) is not str or _SHA256.fullmatch(digest) is None:
            raise ValueError("reviewed file requires a SHA256")
        if relative == "scripts/baseline_manifest.json":
            raise ValueError("manifest review cannot hash its own registry bytes")
        _path(relative, root)
    tree_hash = data["reviewed_tree_hash"]
    if tree_hash != _tree_hash(files):
        raise ValueError("reviewed_tree_hash does not bind reviewed_files")
    return ReviewedMigration(selection, _text(data["work_package_id"], "work_package_id"),
                             commit, tree_hash, tuple(sorted(files.items())),
                             _text(data["cost_review"], "cost_review"))


def _verify_review(migration: ReviewedMigration, root: Path) -> None:
    reviewed = dict(migration.reviewed_files)
    discovered = review_inputs(migration.selection.selector, root=root)
    if not discovered.keys() <= reviewed.keys():
        raise ValueError("migration import/config closure changed; re-review required")
    for name, digest in reviewed.items():
        if hashlib.sha256(_path(name, root).read_bytes()).hexdigest() != digest:
            raise ValueError("migration input hash changed; re-review required: " + name)


def _validate_manifest(data, *, root: Path | None = None) -> BaselineManifest:
    root = (root or PROJECT_ROOT).resolve()
    fields = {"schema_version", "edition", "pure", "smoke", "known_non_unit_in_pure", "reviewed_migrations"}
    if type(data) is not dict or set(data) != fields:
        raise ValueError("baseline manifest has missing or unknown fields")
    if type(data["schema_version"]) is not int or data["schema_version"] != 2:
        raise ValueError("unsupported baseline manifest schema")
    if data["edition"] not in ("base", "enhanced"):
        raise ValueError("edition must identify base or enhanced provenance")
    for name in ("pure", "smoke", "known_non_unit_in_pure", "reviewed_migrations"):
        if type(data[name]) is not list or (name in {"pure", "smoke"} and not data[name]):
            raise ValueError(name + " must be an explicit list")
    pure = tuple(_selector(item, root) for item in data["pure"])
    if any("::" in item or not item.startswith("tests/unit/") for item in pure):
        raise ValueError("pure gate requires exact unit file paths")
    smoke = tuple(_selection(item, root, exact=True) for item in data["smoke"])
    if any(item.marker == "unit" for item in smoke):
        raise ValueError("smoke gate requires a smoke marker")
    exclusions = tuple(_selector(item, root, exact=True) for item in data["known_non_unit_in_pure"])
    if len(set(exclusions)) != len(exclusions) or any(item.split("::", 1)[0] not in pure for item in exclusions):
        raise ValueError("known non-unit exclusions must be unique cases in pure files")
    migrations = tuple(_migration(item, root) for item in data["reviewed_migrations"])
    selected = [Selection(item, "unit") for item in pure] + list(smoke) + [item.selection for item in migrations]
    names = [item.selector for item in selected]
    if len(set(names)) != len(names):
        raise ValueError("duplicate or conflicting gate/migration selector")
    whole = {item.selector for item in selected if "::" not in item.selector}
    for item in selected:
        parent = item.selector.split("::", 1)[0]
        if "::" in item.selector and parent in whole:
            if item.selector not in exclusions or item.marker == "unit":
                raise ValueError("whole-file and exact-case registrations overlap")
    return BaselineManifest(data["edition"], pure, smoke, exclusions, migrations)


def _load_manifest() -> BaselineManifest:
    return _validate_manifest(json.loads(MANIFEST_PATH.read_text(encoding="utf-8"), object_pairs_hook=_unique_object))


def _case(manifest: BaselineManifest, selector: str) -> Selection:
    if selector in manifest.pure:
        return Selection(selector, "unit")
    for item in manifest.smoke:
        if item.selector == selector:
            return item
    for migration in manifest.reviewed_migrations:
        if migration.selection.selector == selector:
            _verify_review(migration, PROJECT_ROOT.resolve())
            return migration.selection
    raise ValueError("--case requires an exact registered gate or migration selector")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--list", action="store_true", help="list selections without importing tests")
    mode.add_argument("--pure", action="store_true", help="run the bounded pure delivery gate")
    mode.add_argument("--smoke", metavar="EXACT", help="run one delivery smoke")
    mode.add_argument("--case", metavar="EXACT", help="run one registered gate or migration file/case")
    args = parser.parse_args(argv)
    try:
        manifest = _load_manifest()
        if args.list:
            print(manifest.edition + " selections; listing is not acceptance evidence.")
            for name in manifest.pure:
                print("delivery_gate unit", name)
            for item in manifest.smoke:
                print("delivery_gate", item.marker, item.selector)
            for migration in manifest.reviewed_migrations:
                print("reviewed_migration", migration.selection.marker, migration.selection.selector)
            for selector in manifest.known_non_unit_in_pure:
                print("non_unit_excluded_from_pure", selector)
            return 0
        if args.pure:
            selectors, marker = manifest.pure, "unit"
        elif args.smoke:
            item = next((item for item in manifest.smoke if item.selector == args.smoke), None)
            if item is None:
                raise ValueError("--smoke requires an exact delivery-gate smoke")
            selectors, marker = (item.selector,), item.marker
        else:
            item = _case(manifest, args.case)
            selectors, marker = (item.selector,), item.marker
        bounded._require_posix_execution()
    except (OSError, SyntaxError, ValueError, TypeError, RuntimeError) as exc:
        parser.error(str(exc))
    return bounded.run_pytest(selectors, marker, root=PROJECT_ROOT)


if __name__ == "__main__":
    raise SystemExit(main())