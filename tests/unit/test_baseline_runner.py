"""Pure manifest selection, registry identity and boundary dispatch contracts."""

from copy import deepcopy
import json
from pathlib import Path
import sys

import pytest

from scripts import run_baseline as runner

pytestmark = pytest.mark.unit
_PURE = "tests/unit/test_single_output_contract.py"
_SMOKE = "tests/integration/test_task_path.py::test_one_node_one_worker_task_path"


def _data():
    return {"schema_version": 2, "edition": "base", "pure": [_PURE],
            "smoke": [{"selector": _SMOKE, "marker": "multiprocess_smoke"}],
            "known_non_unit_in_pure": [], "reviewed_migrations": []}


def test_schema_rejects_unknown_fields_old_schema_duplicates_and_broad_selection():
    assert runner._validate_manifest(_data()).pure == (_PURE,)
    for changes in ({"schema_version": 1}, {"schema_version": True}, {"unknown": 1},
                    {"edition": "other"}, {"pure": []}, {"pure": ["tests/unit"]},
                    {"pure": ["tests/unit/*.py"]}, {"pure": [_PURE, _PURE]},
                    {"pure": [_PURE + "::test_something"]}, {"smoke": []},
                    {"smoke": [{"selector": _SMOKE.split("::")[0], "marker": "multiprocess_smoke"}]}):
        data = _data()
        data.update(changes)
        with pytest.raises(ValueError):
            runner._validate_manifest(data)
    with pytest.raises(ValueError, match="duplicate JSON"):
        json.loads('{"schema_version":2,"schema_version":2}', object_pairs_hook=runner._unique_object)


def test_list_never_imports_tests_checks_processes_or_runs_migrations(monkeypatch, capsys):
    monkeypatch.setattr(runner, "_load_manifest", lambda: runner._validate_manifest(_data()))
    def forbidden(*args, **kwargs):
        pytest.fail("listing performed an execution effect")
    monkeypatch.setattr(runner.bounded, "_require_posix_execution", forbidden)
    monkeypatch.setattr(runner.bounded, "run_pytest", forbidden)
    monkeypatch.setattr(runner, "_verify_review", forbidden)
    before = set(sys.modules)
    assert runner.main(["--list"]) == 0
    assert set(sys.modules) == before
    output = capsys.readouterr().out
    assert "delivery_gate" in output and "not acceptance evidence" in output


@pytest.mark.parametrize("mode,selection,marker", (
    ("--pure", _PURE, "unit"), ("--case", _PURE, "unit"),
    ("--smoke", _SMOKE, "multiprocess_smoke"), ("--case", _SMOKE, "multiprocess_smoke"),
))
def test_modes_dispatch_only_manifest_selection_to_same_kernel(monkeypatch, mode, selection, marker):
    monkeypatch.setattr(runner, "_load_manifest", lambda: runner._validate_manifest(_data()))
    monkeypatch.setattr(runner.bounded, "_require_posix_execution", lambda: None)
    calls = []
    monkeypatch.setattr(runner.bounded, "run_pytest", lambda selectors, mark, **kw: calls.append((selectors, mark, kw)) or 7)
    args = [mode] if mode == "--pure" else [mode, selection]
    assert runner.main(args) == 7
    assert calls == [((selection,), marker, {"root": runner.PROJECT_ROOT})]


@pytest.mark.parametrize("args", ([], ["--li"], ["--pure", "-m", "heavy"],
    ["--case", "tests/unit"], ["--smoke", _PURE], ["--case", _PURE + "::test_unregistered"],
    ["--case", _SMOKE, "--maxfail=1"], ["--pure", "--smoke", _SMOKE]))
def test_unregistered_and_extra_arguments_reject_before_kernel(monkeypatch, args):
    monkeypatch.setattr(runner, "_load_manifest", lambda: runner._validate_manifest(_data()))
    monkeypatch.setattr(runner.bounded, "run_pytest", lambda *a, **kw: pytest.fail("unexpected child"))
    with pytest.raises(SystemExit) as exc:
        runner.main(args)
    assert exc.value.code == 2


def test_unsupported_platform_rejects_before_child_creation(monkeypatch):
    monkeypatch.setattr(runner, "_load_manifest", lambda: runner._validate_manifest(_data()))
    def unsupported():
        raise RuntimeError("POSIX process groups required")
    monkeypatch.setattr(runner.bounded, "_require_posix_execution", unsupported)
    monkeypatch.setattr(runner.bounded, "run_pytest", lambda *a, **kw: pytest.fail("unexpected child"))
    with pytest.raises(SystemExit) as exc:
        runner.main(["--pure"])
    assert exc.value.code == 2


def _review_tree(tmp_path):
    for name, source in {
        "tests/unit/test_gate.py": "import pytest\npytestmark=pytest.mark.unit\ndef test_gate(): pass\n",
        "tests/integration/test_gate.py": "import pytest\npytestmark=pytest.mark.multiprocess_smoke\ndef test_gate(): pass\n",
        "tests/unit/test_migration.py": "import pytest\nfrom tests.support.helper import value\npytestmark=pytest.mark.unit\ndef test_migration(): assert value == 1\n",
        "tests/support/helper.py": "value = 1\n",
        "conftest.py": "# root pytest configuration\n",
        "pyproject.toml": "# execution configuration\n",
        "uv.lock": "version = 1\n",
    }.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source, encoding="utf-8")
    selector = "tests/unit/test_migration.py"
    files = runner.review_inputs(selector, root=tmp_path)
    record = {"selector": selector, "marker": "unit", "purpose": "migration", "work_package_id": "K1",
              "review_source_commit": "1" * 40, "reviewed_tree_hash": runner._tree_hash(files),
              "reviewed_files": files, "cost_review": "Pure value assertion; no threads/processes/network."}
    data = {"schema_version": 2, "edition": "base", "pure": ["tests/unit/test_gate.py"],
            "smoke": [{"selector": "tests/integration/test_gate.py::test_gate", "marker": "multiprocess_smoke"}],
            "known_non_unit_in_pure": [], "reviewed_migrations": [record]}
    return data, record


def test_registry_binds_helper_config_and_checkout_bytes_without_git(tmp_path, monkeypatch):
    data, record = _review_tree(tmp_path)
    assert not (tmp_path / ".git").exists()
    assert "tests/support/helper.py" in record["reviewed_files"]
    assert "conftest.py" in record["reviewed_files"]
    manifest = runner._validate_manifest(data, root=tmp_path)
    monkeypatch.setattr(runner, "PROJECT_ROOT", tmp_path)
    assert runner._case(manifest, record["selector"]).marker == "unit"
    assert record["selector"] not in manifest.pure
    (tmp_path / "tests/support/helper.py").write_text("value = 2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="hash changed"):
        runner._case(manifest, record["selector"])


def test_added_import_and_unreviewed_config_prevent_migration(tmp_path):
    data, record = _review_tree(tmp_path)
    record["reviewed_files"].pop("conftest.py")
    record["reviewed_tree_hash"] = runner._tree_hash(record["reviewed_files"])
    migration = runner._validate_manifest(data, root=tmp_path).reviewed_migrations[0]
    with pytest.raises(ValueError, match="closure changed"):
        runner._verify_review(migration, tmp_path)


def test_one_manifest_load_checks_shared_paths_once_but_next_load_rechecks(tmp_path, monkeypatch):
    data, record = _review_tree(tmp_path)
    second = deepcopy(record)
    record["selector"] += "::test_migration"
    second["selector"] += "::test_second_reviewed_case"
    data["reviewed_migrations"].append(second)
    original = runner._path
    paths = []

    def checked(relative, root):
        paths.append(relative)
        return original(relative, root)

    monkeypatch.setattr(runner, "_path", checked)
    runner._validate_manifest(data, root=tmp_path)
    assert len(paths) == len(set(paths))
    assert paths.count("tests/support/helper.py") == 1
    (tmp_path / "tests/support/helper.py").unlink()
    with pytest.raises(ValueError, match="missing or outside"):
        runner._validate_manifest(data, root=tmp_path)
    assert paths.count("tests/support/helper.py") == 2


def test_verify_reads_discovered_and_extra_hash_inputs_once_per_invocation(tmp_path, monkeypatch):
    data, record = _review_tree(tmp_path)
    extra = tmp_path / "golden.json"
    extra.write_bytes(b'{"value":1}\n')
    record["reviewed_files"]["golden.json"] = runner._review_input_hash(extra.read_bytes())
    record["reviewed_tree_hash"] = runner._tree_hash(record["reviewed_files"])
    migration = runner._validate_manifest(data, root=tmp_path).reviewed_migrations[0]
    original = Path.read_bytes
    reads = []

    def read(path):
        reads.append(path.relative_to(tmp_path).as_posix())
        return original(path)

    monkeypatch.setattr(Path, "read_bytes", read)
    runner._verify_review(migration, tmp_path)
    assert len(reads) == len(set(reads)) == len(record["reviewed_files"])
    assert reads.count("tests/support/helper.py") == reads.count("golden.json") == 1
    (tmp_path / "tests/support/helper.py").write_text("value = 2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="hash changed"):
        runner._verify_review(migration, tmp_path)
    assert reads.count("tests/support/helper.py") == 2


def test_review_identity_survives_only_checkout_crlf_to_lf_changes(tmp_path):
    data, record = _review_tree(tmp_path)
    golden = tmp_path / "golden.json"
    golden.write_bytes(b'{"value": 1}\r\n')
    record["reviewed_files"]["golden.json"] = runner._review_input_hash(golden.read_bytes())
    record["reviewed_tree_hash"] = runner._tree_hash(record["reviewed_files"])
    migration = runner._validate_manifest(data, root=tmp_path).reviewed_migrations[0]
    before = dict(migration.reviewed_files)
    for newline in (b"\r\n", b"\n"):
        for relative in before:
            path = tmp_path / relative
            path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", newline))
        runner._verify_review(migration, tmp_path)
        assert runner._tree_hash(before) == migration.reviewed_tree_hash
    golden.write_bytes(b'{"value": 2}\n')
    with pytest.raises(ValueError, match="hash changed.*golden.json"):
        runner._verify_review(migration, tmp_path)


@pytest.mark.parametrize("changed", (
    b"value = 1\r", b"value = 1", b"value = 1 \n", b"value = 2\n",
    b"\xef\xbb\xbfvalue = 1\n", b"value = 1\n\n",
))
def test_review_hash_preserves_every_non_crlf_byte(changed):
    import hashlib

    original = b"value = 1\n"
    expected = hashlib.sha256(original).hexdigest()
    assert runner._review_input_hash(original) == expected
    assert runner._review_input_hash(b"value = 1\r\n") == expected
    assert runner._review_input_hash(changed) != expected


def test_registry_duplicates_conflicting_marker_bad_hash_and_self_hash_reject(tmp_path):
    data, record = _review_tree(tmp_path)
    invalid = deepcopy(data)
    invalid["reviewed_migrations"].append(deepcopy(record))
    with pytest.raises(ValueError, match="duplicate"):
        runner._validate_manifest(invalid, root=tmp_path)
    for update in ({"marker": "heavy"}, {"reviewed_tree_hash": "0" * 64},
                   {"review_source_commit": "HEAD"}, {"unknown": True}):
        invalid = deepcopy(data)
        invalid["reviewed_migrations"][0].update(update)
        with pytest.raises(ValueError):
            runner._validate_manifest(invalid, root=tmp_path)
    invalid = deepcopy(data)
    invalid["reviewed_migrations"][0]["selector"] = data["pure"][0]
    with pytest.raises(ValueError, match="duplicate"):
        runner._validate_manifest(invalid, root=tmp_path)


def test_symlink_escape_and_noncanonical_review_paths_reject(monkeypatch):
    original = Path.resolve
    def escaped(path, *args, **kwargs):
        if path == runner.PROJECT_ROOT / _PURE:
            return runner.PROJECT_ROOT.parent / "outside.py"
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, "resolve", escaped)
    with pytest.raises(ValueError, match="outside"):
        runner._validate_manifest(_data())
    for name in ("../outside.py", "/tmp/outside.py", "C:/outside.py", "tests/./unit/test_x.py"):
        with pytest.raises(ValueError):
            runner._path(name, runner.PROJECT_ROOT)


@pytest.mark.parametrize("event_type,branch,manifest_edition,is_pr", (
    ("push", "teaching-base", "base", False),
    ("push", "teaching-enhanced", "enhanced", False),
    ("pull_request", "teaching-base", "base", True),
    ("workflow_dispatch", "teaching-enhanced", "enhanced", False),
    ("push", "main", "base", False),
))
def test_ci_binds_branch_event_and_actual_checkout_without_real_children(
    tmp_path, monkeypatch, event_type, branch, manifest_edition, is_pr,
):
    from types import SimpleNamespace
    from scripts import _ci_baseline as ci

    monkeypatch.chdir(tmp_path)
    (tmp_path / "scripts").mkdir()
    manifest_bytes = b'{"edition":"' + manifest_edition.encode() + b'"}'
    (tmp_path / "scripts/baseline_manifest.json").write_bytes(manifest_bytes)
    source_sha, event_sha, checked_sha = "1" * 40, "2" * 40, "3" * 40
    event = {"pull_request": {"head": {"ref": "fix-candidate", "sha": source_sha},
                              "base": {"ref": branch}}} if is_pr else {}
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps(event), encoding="utf-8")
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_path))
    monkeypatch.setenv("GITHUB_EVENT_NAME", event_type)
    monkeypatch.setenv("GITHUB_REF", "refs/pull/7/merge" if is_pr else "refs/heads/" + branch)
    monkeypatch.setenv("GITHUB_SHA", event_sha)
    monkeypatch.setattr(ci, "_load_manifest", lambda: runner.BaselineManifest(
        manifest_edition, (_PURE,), (runner.Selection(_SMOKE, "multiprocess_smoke"),)))
    launches = []
    monkeypatch.setattr(ci.subprocess, "check_output", lambda *a, **kw: checked_sha + "\n")
    monkeypatch.setattr(ci.subprocess, "run", lambda command, **kw: launches.append(command) or SimpleNamespace(returncode=0))
    if branch == "main":
        with pytest.raises(SystemExit, match="teaching branch"):
            ci.main()
        assert not launches
        return
    assert ci.main() == 0
    identity = json.loads((tmp_path / "baseline-evidence/source-identity.json").read_text())
    assert identity["candidate_sha"] == (source_sha if is_pr else event_sha)
    assert identity["event_sha"] == event_sha
    assert identity["checked_out_sha"] == identity["tested_source_commit"] == checked_sha
    assert identity["checkout_kind"] == ("merge_preview" if is_pr else "branch_commit")
    assert identity["edition"] == manifest_edition
    assert identity["pr_target_branch_if_applicable"] == (branch if is_pr else None)
    assert identity["dispatch_ref_if_applicable"] == (
        "refs/heads/" + branch if event_type == "workflow_dispatch" else None)
    assert len(launches) == 2 and launches[0][-1] == "--pure"
    assert launches[1][-2:] == ["--smoke", _SMOKE]
