"""Pure mode-selection and marker contracts for the bounded test runner.

Smoke source is inspected as AST, never imported or collected.  All process
creation and cleanup operations are fakes, including timeout/interruption.
"""

from __future__ import annotations

import ast

import pytest

from scripts import run_bounded_test as runner


pytestmark = pytest.mark.unit

_MP_ID = "tests/integration/test_task_path.py::test_one_node_one_worker_task_path"
_LOOPBACK_ID = (
    "tests/unit/test_inline_recovery.py::"
    "test_owner_death_and_intent_admission_linearize_atomically"
)
_MIXED_FILES = (
    "test_stored_intent_gate.py",
    "test_inline_recovery.py",
    "test_publication_owner_death_control.py",
    "test_stored_publication_node_server.py",
    "test_inline_publication_node_server.py",
    "test_owner_death_fence_control.py",
    "test_node_dependency_pull.py",
)


def _marker_name(decorator: ast.expr) -> str | None:
    value = decorator.func if isinstance(decorator, ast.Call) else decorator
    if (
        isinstance(value, ast.Attribute)
        and isinstance(value.value, ast.Attribute)
        and value.value.attr == "mark"
        and isinstance(value.value.value, ast.Name)
        and value.value.value.id == "pytest"
    ):
        return value.attr
    return None


@pytest.mark.parametrize("node_id,expected", (
    (_MP_ID, "multiprocess_smoke"),
    (_LOOPBACK_ID, "loopback_smoke"),
))
def test_exact_allowlist_infers_mode_without_changing_original_cli(
    node_id: str, expected: str,
) -> None:
    assert runner._smoke_marker(node_id) == expected
    assert runner._pytest_command(node_id) == [
        runner.sys.executable, "-m", "pytest", "-m", expected, node_id, "-q"
    ]


def test_smoke_allowlists_are_disjoint_and_cover_only_their_mode() -> None:
    assert not runner.ALLOWED_NODE_IDS & runner.ALLOWED_LOOPBACK_NODE_IDS
    for node_id in runner.ALLOWED_NODE_IDS:
        assert runner._smoke_marker(node_id) == "multiprocess_smoke"
    for node_id in runner.ALLOWED_LOOPBACK_NODE_IDS:
        assert runner._smoke_marker(node_id) == "loopback_smoke"


def test_conflicting_allowlists_fail_before_starting_any_process(monkeypatch) -> None:
    monkeypatch.setattr(runner, "ALLOWED_LOOPBACK_NODE_IDS", frozenset({_MP_ID}))
    with pytest.raises(ValueError, match="both smoke allowlists"):
        runner._pytest_command(_MP_ID)


@pytest.mark.parametrize("selector", (
    "tests/unit", "tests/unit/test_inline_recovery.py",
    "loopback_smoke", "tests/unit/test_inline_recovery.py::test_unknown",
    "tests/unit/test_publication_owner_death_control.py::"
    "test_live_background_converges_owner_wide_and_publication_sagas",
))
def test_unreviewed_file_group_and_missing_parameter_are_rejected(
    monkeypatch, selector: str,
) -> None:
    monkeypatch.setattr(
        runner.subprocess, "Popen",
        lambda *_args, **_kwargs: pytest.fail("invalid selection started pytest"),
    )
    with pytest.raises(ValueError, match="exact allowlisted"):
        runner._pytest_command(selector)
    with pytest.raises(SystemExit) as caught:
        runner.main([selector])
    assert caught.value.code == 2


@pytest.mark.parametrize("extra", ("-m", "--maxfail=1", _MP_ID))
def test_cli_accepts_one_selector_and_no_pytest_passthrough(
    monkeypatch, extra: str,
) -> None:
    monkeypatch.setattr(
        runner.subprocess, "Popen",
        lambda *_args, **_kwargs: pytest.fail("extra selector started pytest"),
    )
    with pytest.raises(SystemExit) as caught:
        runner.main([_LOOPBACK_ID, extra])
    assert caught.value.code == 2


@pytest.mark.parametrize("node_id", (_MP_ID, _LOOPBACK_ID))
@pytest.mark.parametrize("outcome", ("passed", "failed", "timeout", "interrupt"))
def test_modes_share_isolated_process_deadline_and_exact_cleanup(
    monkeypatch, node_id: str, outcome: str,
) -> None:
    launches = []
    cleaned = []

    class _Process:
        pid = 12345

        def wait(self, timeout):
            assert timeout == 30.0 == runner.TEST_TIMEOUT_SECONDS
            if outcome == "timeout":
                raise runner.subprocess.TimeoutExpired("pytest", timeout)
            if outcome == "interrupt":
                raise KeyboardInterrupt
            return 0 if outcome == "passed" else 2

    process = _Process()

    def launch(command, **options):
        launches.append((command, options))
        return process

    monkeypatch.setattr(runner.subprocess, "Popen", launch)
    monkeypatch.setattr(
        runner, "_terminate_process_tree", lambda value: cleaned.append(value)
    )
    if outcome == "interrupt":
        with pytest.raises(KeyboardInterrupt):
            runner.main([node_id])
    else:
        result = runner.main([node_id])
        assert result == (124 if outcome == "timeout" else 0 if outcome == "passed" else 2)
    assert launches == [(runner._pytest_command(node_id), {
        "cwd": str(runner.PROJECT_ROOT), "start_new_session": True,
    })]
    assert cleaned == ([process] if outcome in {"timeout", "interrupt"} else [])


@pytest.mark.parametrize("filename", _MIXED_FILES)
def test_mixed_files_have_no_inherited_unit_marker_and_every_case_is_classified(
    filename: str,
) -> None:
    path = runner.PROJECT_ROOT / "tests" / "unit" / filename
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    assert not any(
        isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "pytestmark"
                for target in node.targets)
        for node in tree.body
    )
    allowed_names = {
        node_id.split("::", 1)[1].split("[", 1)[0]
        for node_id in runner.ALLOWED_LOOPBACK_NODE_IDS
        if node_id.split("::", 1)[0] == "tests/unit/" + filename
    }
    observed_smokes = set()
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
            continue
        modes = [
            marker for decorator in node.decorator_list
            if (marker := _marker_name(decorator))
            in {"unit", "loopback_smoke", "multiprocess_smoke", "heavy"}
        ]
        assert len(modes) == 1, node.name
        expected = "loopback_smoke" if node.name in allowed_names else "unit"
        assert modes == [expected], node.name
        if expected == "loopback_smoke":
            observed_smokes.add(node.name)
    assert observed_smokes == allowed_names


def test_parameterized_background_cases_are_exact_single_fault_selectors() -> None:
    relative = "tests/unit/test_publication_owner_death_control.py"
    path = runner.PROJECT_ROOT / relative
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    function = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef)
        and node.name == "test_live_background_converges_owner_wide_and_publication_sagas"
    )
    parameterize = next(
        decorator for decorator in function.decorator_list
        if _marker_name(decorator) == "parametrize"
    )
    assert ast.literal_eval(parameterize.args[0]) == "failed_domain"
    domains = ast.literal_eval(parameterize.args[1])
    assert domains == ("fence", "publication")
    assert {relative + "::" + function.name + "[" + domain + "]" for domain in domains} <= (
        runner.ALLOWED_LOOPBACK_NODE_IDS
    )


def test_real_timer_cases_are_only_opt_in_bounded_smokes():
    relative = "tests/integration/test_core_gc_retry_timer_lifecycle.py"
    assert not (runner.PROJECT_ROOT / "tests/unit/test_core_gc_retry_timer_lifecycle.py").exists()
    tree = ast.parse((runner.PROJECT_ROOT / relative).read_text(encoding="utf-8"))
    markers = [
        _marker_name(node.value) for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets)
    ]
    assert markers == ["loopback_smoke"]
    names = {node.name for node in tree.body
             if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")}
    assert len(names) == 3
    for name in names:
        assert relative + "::" + name in runner.ALLOWED_LOOPBACK_NODE_IDS


@pytest.mark.parametrize("filename,count", (
    ("test_core_dispatch_concurrency.py", 2),
    ("test_core_shutdown_concurrency.py", 3),
))
def test_core_concurrency_stays_out_of_the_default_unit_gate(filename, count):
    relative = "tests/integration/" + filename
    tree = ast.parse((runner.PROJECT_ROOT / relative).read_text(encoding="utf-8"))
    markers = [
        _marker_name(node.value) for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets)
    ]
    assert markers == ["loopback_smoke"]
    names = {node.name for node in tree.body
             if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")}
    assert len(names) == count
    assert {relative + "::" + name for name in names} <= runner.ALLOWED_LOOPBACK_NODE_IDS
