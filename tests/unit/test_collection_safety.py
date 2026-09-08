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
    with pytest.raises(pytest.UsageError, match="complete default test gate"):
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


# These AST checks preserve an explicitly reviewed classification, not a proof
# that future bodies/imports/fixtures are safe. They never import those modules.
def _classification_source(filename):
    import ast

    path = guard._PROJECT_ROOT / "tests" / "unit" / filename
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return ast, tree


def _mode_marker(value):
    import ast

    decorator = value.func if isinstance(value, ast.Call) else value
    if (isinstance(decorator, ast.Attribute) and isinstance(decorator.value, ast.Attribute)
            and decorator.value.attr == "mark" and isinstance(decorator.value.value, ast.Name)
            and decorator.value.value.id == "pytest"
            and decorator.attr in {"unit", "heavy", "loopback_smoke", "multiprocess_smoke"}):
        return decorator.attr
    return None


@pytest.mark.parametrize("filename,unit_names,heavy_names,loopback_names", (
    ("test_function_registry.py", (
        "test_register_is_exactly_idempotent",
        "test_conflicting_registration_preserves_original_payload",
        "test_unknown_lookup_raises_typed_error",
        "test_definition_round_trip_rebuilds_the_checksum",
        "test_function_definition_rejects_a_drifted_checksum",
        "test_snapshot_is_stable_payload_free_metadata",
        "test_control_module_preserves_compatibility_exports",
    ), (), (
        "test_concurrent_identical_registration_creates_exactly_once",
    )),
    ("test_worker_side_core_contract.py", (
        "test_non_owning_trace_view_forwards_identity_and_component_without_close",
        "test_parent_execution_context_derives_child_ids_from_parent_and_index",
        "test_worker_lazily_constructs_one_embedded_core_and_fences_other_jobs",
        "test_worker_shutdown_before_first_push_never_constructs_core",
        "test_embedded_core_prepublication_failure_uses_local_abort",
        "test_foreign_job_is_fenced_before_start_lease_or_user_decode",
        "test_owner_proxy_rejections_echo_full_borrow_source_and_task_hold",
        "test_worker_shutdown_is_unclean_when_embedded_core_cannot_drain",
        "test_closed_admission_still_serves_exact_completed_push_replay",
        "test_execution_context_cleanup_and_core_child_specs",
        "test_worker_binding_rejects_driver_lifecycle_operations_without_mutation",
        "test_remote_function_cloudpickle_rebuilds_runtime_cache_and_lock",
        "test_real_owner_core_retain_fence_preserves_replay_query_and_release",
        "test_worker_stored_pin_proxy_uses_only_an_existing_owner_core",
    ), (), (
        "test_worker_shutdown_drains_task_before_embedded_core_and_clean_ack",
        "test_worker_shutdown_timeout_keeps_core_open_for_inflight_parent",
        "test_worker_shutdown_fences_only_new_owner_retains",
        "test_runtime_binding_isolates_worker_thread_and_restores_driver",
    )),
))
def test_mixed_classification_keeps_original_cases_without_inherited_unit(filename, unit_names, heavy_names, loopback_names):
    ast, tree = _classification_source(filename)
    assert not any(
        isinstance(node, ast.Name) and node.id == "pytestmark" and isinstance(node.ctx, ast.Store)
        for node in ast.walk(tree)
    ), "a mixed module must not add unit to its heavy cases through inheritance"
    functions = {node.name: node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")}
    assert set(unit_names).isdisjoint(heavy_names)
    assert set(loopback_names).isdisjoint(set(unit_names) | set(heavy_names))
    assert set(functions) == set(unit_names) | set(heavy_names) | set(loopback_names), "preserve and explicitly classify every original test"
    for name, function in functions.items():
        modes = [mode for value in function.decorator_list if (mode := _mode_marker(value)) is not None]
        assert modes == ["unit" if name in unit_names else "loopback_smoke" if name in loopback_names else "heavy"], name
    if filename == "test_worker_side_core_contract.py":
        function = functions["test_worker_binding_rejects_driver_lifecycle_operations_without_mutation"]
        parameterize = next(value for value in function.decorator_list
                            if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute)
                            and value.func.attr == "parametrize")
        assert len(parameterize.args[1].elts) == 3
        assert next(ast.literal_eval(item.value) for item in parameterize.keywords if item.arg == "ids") == ("init", "shutdown", "trace")


def test_original_threaded_node_drain_cases_remain_nonunit_after_bounded_review():
    ast, tree = _classification_source("test_node_shutdown_drain.py")
    assignments = [node for node in tree.body
                   if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets)]
    assert len(assignments) == 1 and _mode_marker(assignments[0].value) == "loopback_smoke"
    functions = {node.name: node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")}
    assert set(functions) == {
        "test_inflight_localization_blocks_drain_and_cannot_late_grant",
        "test_exact_cached_replays_are_counted_and_balance_after_begin_drain",
    }
    assert all(not [_mode_marker(value) for value in function.decorator_list
                    if _mode_marker(value) is not None] for function in functions.values())


def test_node_blocking_classification_preserves_six_unit_cases_and_one_bounded_race():
    ast, tree = _classification_source("test_node_blocking_get_authority.py")
    assert not any(
        isinstance(node, ast.Name) and node.id == "pytestmark" and isinstance(node.ctx, ast.Store)
        for node in ast.walk(tree)
    ), "the real thread race must not inherit unit"
    expected = {
        "test_node_yields_only_cpu_and_replays_one_episode_idempotently": "unit",
        "test_unblock_is_immediate_with_debt_and_completion_reconciles_once": "unit",
        "test_unblock_before_block_tombstones_ambiguous_episode": "unit",
        "test_sequence_and_execution_identity_fence_every_resource_mutation": "unit",
        "test_zero_cpu_lease_tracks_episode_without_changing_resources": "unit",
        "test_completion_and_worker_loss_clean_yielded_allocations": "unit",
        "test_concurrent_block_and_completion_linearize_without_leaking": "loopback_smoke",
    }
    functions = {node.name: node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")}
    assert set(functions) == set(expected)
    for name, function in functions.items():
        assert [_mode_marker(value) for value in function.decorator_list] == [expected[name]], name


def test_core_blocking_original_six_functions_seven_cases_use_reviewed_pure_composition():
    ast, tree = _classification_source("test_core_blocking_get_notifications.py")
    assignments = [node for node in tree.body
                   if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets)]
    assert len(assignments) == 1 and _mode_marker(assignments[0].value) == "unit"
    functions = {node.name: node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")}
    assert set(functions) == {
        "test_ready_get_and_zero_timeout_pending_get_do_not_notify",
        "test_all_ready_get_many_and_ready_error_do_not_notify",
        "test_wait_timeout_or_error_always_closes_the_blocking_episode",
        "test_get_inside_an_existing_blocking_scope_is_one_episode",
        "test_get_many_uses_one_outer_episode_for_multiple_actual_waits",
        "test_get_many_error_closes_its_single_outer_episode",
    }
    case_count = 0
    for name, function in functions.items():
        assert all(_mode_marker(value) is None for value in function.decorator_list), name
        parameters = [value for value in function.decorator_list
                      if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute)
                      and value.func.attr == "parametrize"]
        if name == "test_wait_timeout_or_error_always_closes_the_blocking_episode":
            assert len(parameters) == 1
            assert ast.literal_eval(parameters[0].args[0]) == ("wait_error", "expected_error", "message")
            assert len(parameters[0].args[1].elts) == 2
            case_count += 2
        else:
            assert parameters == [], name
            case_count += 1
    assert case_count == 7


def test_contained_cycle_concurrency_cannot_inherit_the_pure_policy_marker():
    ast, tree = _classification_source("test_contained_cycle_policy.py")
    assert not any(isinstance(node, ast.Name) and node.id == "pytestmark"
                   and isinstance(node.ctx, ast.Store) for node in ast.walk(tree))
    functions = {node.name: node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")}
    assert len(functions) == 10
    concurrent = "test_prepared_edges_close_the_concurrent_preflight_race"
    assert concurrent in functions
    for name, function in functions.items():
        modes = [_mode_marker(value) for value in function.decorator_list]
        assert modes == ["heavy" if name == concurrent else "unit"], name


def test_node_death_api_runtime_keeps_two_real_thread_cases_out_of_unit():
    ast, tree = _classification_source("test_node_death_api_runtime.py")
    assert not any(isinstance(node, ast.Name) and node.id == "pytestmark"
                   and isinstance(node.ctx, ast.Store) for node in ast.walk(tree))
    functions = {node.name: node for node in tree.body
                 if isinstance(node, ast.FunctionDef) and node.name.startswith("test_")}
    heavy = {
        "test_monitor_dispatch_does_not_block_on_another_death_transaction",
        "test_zero_exit_waits_for_finalize_ack_decision_before_expected_fact",
    }
    assert len(functions) == 12 and heavy <= functions.keys()
    for name, function in functions.items():
        assert [_mode_marker(value) for value in function.decorator_list] == ["heavy" if name in heavy else "unit"], name


def test_trace_export_keeps_four_real_filesystem_contracts_separate_from_pure_rejections():
    # This preserves the reviewed classification, not future body/import
    # safety. Parse source only; never import or collect the heavy contracts.
    ast, tree = _classification_source("test_trace_export.py")
    assert not any(isinstance(node, ast.Name) and node.id == "pytestmark"
                   and isinstance(node.ctx, ast.Store) for node in ast.walk(tree))
    expected = {
        "test_public_export_is_deterministic_and_preserves_semantic_schema": "heavy",
        "test_export_overwrites_old_tail_and_empty_snapshot_creates_empty_file": "heavy",
        "test_export_failure_leaves_the_previous_file_and_removes_temporary_file": "heavy",
        "test_export_rejects_missing_parent_and_directory_targets": "heavy",
        "test_export_rejects_invalid_path_values": "unit",
        "test_export_requires_an_initialized_driver_runtime": "unit",
    }
    functions = {node.name: node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")}
    assert set(functions) == set(expected), "keep every original trace-export contract"
    for name, function in functions.items():
        modes = [mode for value in function.decorator_list if (mode := _mode_marker(value)) is not None]
        assert modes == [expected[name]], name
        if expected[name] == "unit":
            assert "tmp_path" not in {argument.arg for argument in function.args.args}
    invalid = functions["test_export_rejects_invalid_path_values"]
    parameterize = next(value for value in invalid.decorator_list
                        if isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute)
                        and value.func.attr == "parametrize")
    assert ast.literal_eval(parameterize.args[0]) == ("path", "error", "message")
    assert len(parameterize.args[1].elts) == 3
