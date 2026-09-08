"""Pure contracts for the explicit reviewed-subset verification entry.

Read only its JSON manifest and stat explicitly selected paths. Every process
launch, wait and tree cleanup is replaced by an in-memory fake; no pytest
child, test collection, test-module import, socket, thread or process runs.
"""

from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import sys

import pytest

from scripts import run_reviewed_pure as runner


pytestmark = pytest.mark.unit

_WHOLE = "tests/unit/test_bounded_runner.py"
_EXACT = ("tests/unit/test_bounded_test_modes.py::"
          "test_smoke_allowlists_are_disjoint_and_cover_only_their_mode")


@pytest.fixture(autouse=True)
def _no_children(monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("reviewed-pure runner contract attempted a real child or process signal")

    monkeypatch.setattr(runner.subprocess, "Popen", forbidden)
    monkeypatch.setattr(runner.bounded, "_terminate_process_tree", forbidden)


def _small_data():
    return {
        "schema_version": 1,
        "scope": "reviewed-pure-subset",
        "name": "two-selector-fixture",
        "reviewed_on": "2026-09-06",
        "review_notice": "Historical review scope, not a permanent safety certificate.",
        "marker": "unit",
        "historical_evidence": {"source": "fixture", "passed": 2, "deselected": 0, "seconds": 0.1},
        "whole_file_count": 1,
        "exact_node_id_count": 1,
        "selectors": [_WHOLE, _EXACT],
        "known_excluded_node_ids": [],
    }


def test_manifest_freezes_reviewed_scope_and_explicit_reviewed_extensions():
    manifest = runner._load_manifest()
    assert manifest.name == "reviewed-foreign-reconstruction-ack-contracts" and manifest.reviewed_on == "2026-09-08"
    assert len(manifest.selectors) == len(set(manifest.selectors)) == 455
    assert len(manifest.whole_files) == 201 and len(manifest.exact_node_ids) == 254
    assert len({item.split("::", 1)[0] for item in manifest.exact_node_ids}) == 36
    assert len(manifest.known_excluded_node_ids) == 12
    assert manifest.historical_passed > 0 and manifest.historical_deselected == 12
    assert manifest.historical_seconds > 0
    assert set(manifest.known_excluded_node_ids) <= runner.bounded.ALLOWED_LOOPBACK_NODE_IDS
    assert "tests/unit/test_reviewed_pure_runner.py" in manifest.selectors
    assert {
        "tests/unit/test_core_task_retry.py",
        "tests/unit/test_multi_return_submission_transaction.py",
        "tests/unit/test_pg_retry_atomicity.py",
        "tests/unit/test_lease_locality.py",
        "tests/unit/test_core_lease_locality.py",
        "tests/unit/test_lease_cancellation.py",
        "tests/unit/test_dispatch_kinds.py",
        "tests/unit/test_core_worker_crash_recovery.py",
        "tests/unit/test_drop_object_replica.py",
        "tests/unit/test_node_lease_execution.py",
        "tests/unit/test_core_worker_death_consumer.py",
        "tests/unit/test_trace_contract.py",
        "tests/unit/test_publication_trace_observation.py",
        "tests/unit/test_core_lost_blocking_lock_order.py",
        "tests/unit/test_get_notification_deadline.py",
        "tests/unit/test_blocking_notifier_entry_failure.py",
        "tests/unit/test_core_blocking_get_notifications.py",
        "tests/unit/test_foreign_reconstruction_runtime.py",
        "tests/unit/test_owner_reconstruction_completion_race.py",
    } <= set(manifest.whole_files)
    assert {
        "tests/unit/test_worker_side_core_contract.py::test_execution_context_cleanup_and_core_child_specs",
        "tests/unit/test_worker_side_core_contract.py::test_worker_binding_rejects_driver_lifecycle_operations_without_mutation",
        "tests/unit/test_worker_side_core_contract.py::test_remote_function_cloudpickle_rebuilds_runtime_cache_and_lock",
        "tests/unit/test_worker_side_core_contract.py::test_real_owner_core_retain_fence_preserves_replay_query_and_release",
        "tests/unit/test_worker_side_core_contract.py::test_worker_stored_pin_proxy_uses_only_an_existing_owner_core",
        "tests/unit/test_public_multi_return_runtime.py::test_task_lifecycle_tables_use_task_id_not_slot_zero",
        "tests/unit/test_public_multi_return_runtime.py::test_dependency_lineage_is_task_scoped_and_releases_with_last_sibling",
        "tests/unit/test_public_multi_return_runtime.py::test_collecting_stored_sibling_does_not_own_or_move_task_lineage",
        "tests/unit/test_foreign_stored_task_dependencies.py::test_confirmed_death_of_already_acknowledged_owner_cancels_grant",
        "tests/unit/test_foreign_stored_task_dependencies.py::test_noncustody_stale_is_quarantined_until_authoritative_owner_death",
        "tests/unit/test_public_multi_return_runtime.py::test_core_stored_replay_drift_cannot_overwrite_any_descriptor",
        "tests/unit/test_public_multi_return_runtime.py::test_invalid_success_manifest_publishes_no_sibling",
        "tests/unit/test_public_multi_return_runtime.py::test_owner_terminal_preflight_failure_does_not_mutate_recovery_or_siblings",
        "tests/unit/test_contained_edge_runtime.py::test_pending_closed_output_waits_for_finish_then_stored_publication_is_collected",
        "tests/unit/test_public_multi_return_runtime.py::test_public_remote_returns_single_ref_or_ordered_tuple",
        "tests/unit/test_public_multi_return_runtime.py::test_submission_registers_all_entries_waiters_refs_and_task_lineage",
        "tests/unit/test_public_multi_return_runtime.py::test_system_retry_advances_all_siblings_once_and_preserves_task_key",
        "tests/unit/test_public_multi_return_runtime.py::test_retry_owner_preflight_failure_consumes_no_recovery_budget",
        "tests/unit/test_worker_export_pin_rollback.py::test_multi_return_contained_refs_use_one_unified_publication_and_slot_scoped_gc",
        "tests/unit/test_worker_export_pin_rollback.py::test_target_single_contained_slot_keeps_full_identity_without_publishing_other_slot",
        "tests/unit/test_public_multi_return_runtime.py::test_ordered_mixed_success_publishes_and_wakes_all_siblings_atomically",
        "tests/unit/test_public_multi_return_runtime.py::test_application_error_publishes_same_terminal_error_to_all_siblings",
        "tests/unit/test_contained_edge_runtime.py::test_shutdown_retries_retained_edge_obligation_before_clean",
        "tests/unit/test_contained_edge_runtime.py::test_stale_reply_cannot_release_committed_publication_edges",
        "tests/unit/test_worker_export_pin_rollback.py::test_failed_unpin_is_visible_and_reference_mailbox_retries_it",
        "tests/unit/test_worker_export_pin_rollback.py::test_shutdown_sync_retry_keeps_export_obligation_until_tombstone",
        "tests/unit/test_worker_export_pin_rollback.py::test_stale_retry_event_cannot_duplicate_a_newer_export_release_round",
        "tests/unit/test_contained_edge_runtime.py::test_pending_outer_close_collects_only_after_reply_installs_edge",
        "tests/unit/test_borrowed_object_refs.py::test_successful_borrowed_close_failure_stays_shutdown_visible",
        "tests/unit/test_borrowed_object_refs.py::test_owner_unreachable_and_foreign_operations_are_explicit",
        "tests/unit/test_borrowed_object_refs.py::test_owner_rejects_new_acquire_but_serves_existing_token_while_closing",
        "tests/unit/test_contained_edge_runtime.py::test_failed_edge_release_freezes_outer_metadata_until_retry_ack",
        "tests/unit/test_borrowed_object_refs.py::test_failed_restore_compensation_persists_and_replays_exact_identity",
        "tests/unit/test_borrowed_object_refs.py::test_borrowed_release_requires_exact_accepted_ack",
        "tests/unit/test_borrowed_object_refs.py::test_repeated_loads_acquire_distinct_tokens_and_remote_get_inline",
        "tests/unit/test_borrowed_object_refs.py::test_ambiguous_acquire_attempts_release_tombstone",
        "tests/unit/test_core_placement_group_scheduling.py::test_committed_participant_death_fences_complete_manifest_and_queued_task",
        "tests/unit/test_core_placement_group_scheduling.py::test_pg_task_first_lease_hop_targets_plan_and_never_spills_back",
        "tests/unit/test_core_placement_group_scheduling.py::test_pg_pending_capacity_retry_preserves_exact_targeted_lease",
        "tests/unit/test_core_placement_group_scheduling.py::test_pg_key_survives_system_retry_and_reconstruction",
        "tests/unit/test_spillback_runtime.py::test_core_preserves_identity_and_does_not_release_from_submitter",
        "tests/unit/test_spillback_runtime.py::test_core_does_not_release_after_ambiguous_push_timeout",
        "tests/unit/test_spillback_runtime.py::test_explicit_worker_rejection_requeues_exact_push",
        "tests/unit/test_foreign_stored_task_dependencies.py::test_execute_report_ambiguity_never_pushes_or_releases_hold_and_blocks_shutdown",
        "tests/unit/test_foreign_stored_task_dependencies.py::test_execute_replays_exact_report_then_pushes_once",
        "tests/unit/test_foreign_stored_task_dependencies.py::test_successful_report_survives_definite_push_failure",
    } <= set(manifest.exact_node_ids)
    assert "tests/unit/test_function_registry.py" not in manifest.selectors
    assert "not a permanent safety certificate" in manifest.review_notice


def test_list_reports_scope_and_known_exclusions_without_importing_tests_or_launching(capsys):
    before = {name for name in sys.modules if name.startswith(("tests.", "miniray"))}
    assert runner.main(["--list"]) == 0
    assert {name for name in sys.modules if name.startswith(("tests.", "miniray"))} == before
    output = capsys.readouterr().out
    assert "REVIEWED PURE SUBSET" in output and "NOT the complete default unit gate" in output
    assert "201 whole files + 254 exact nodes" in output
    assert "Historical evidence:" in output and "12 deselected" in output
    assert "not a result of this invocation" in output
    assert "Known non-unit exclusions" in output and "all unlisted selectors" in output
    manifest = runner._load_manifest()
    assert all(item in output for item in manifest.selectors + manifest.known_excluded_node_ids)


def test_pytest_command_uses_only_explicit_scope_unit_marker_and_known_deselections():
    manifest = runner._load_manifest()
    command = runner._pytest_command(manifest)
    assert command[:8] == [runner.sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-m", "unit"]
    assert command[8:20] == ["--deselect=" + item for item in manifest.known_excluded_node_ids]
    assert command[20:] == list(manifest.selectors)
    assert "--collect-only" not in command and "-n" not in command and "-o" not in command
    with pytest.raises(ValueError, match="without explicit"):
        runner._pytest_command(replace(manifest, selectors=()))


def test_environment_cannot_inject_pytest_options_or_plugins_and_is_not_mutated():
    ambient = {
        "PATH": "/task/bin", "PYTEST_ADDOPTS": "-n auto -m heavy tests",
        "PYTEST_PLUGINS": "unreviewed_plugin", "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "0",
        "TASK_NOTE": "preserve",
    }
    before = dict(ambient)
    actual = runner._child_environment(ambient)
    assert ambient == before
    assert actual == {"PATH": "/task/bin", "TASK_NOTE": "preserve", "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"}


@pytest.mark.parametrize("selector", (
    "", "tests/unit", "tests/unit/*.py", "../test_outside.py",
    "/tmp/test_outside.py", "tests/integration/test_task_path.py",
    "-m heavy", _WHOLE + "::",
))
def test_directory_glob_option_and_malformed_selectors_fail_closed(selector):
    data = _small_data()
    data["selectors"][0] = selector
    with pytest.raises(ValueError):
        runner._validate_manifest(data)


def test_empty_duplicate_overlap_and_wrong_counts_do_not_expand_discovery():
    mutations = (
        {"selectors": []},
        {"selectors": [_WHOLE, _WHOLE]},
        {"selectors": [_WHOLE, _WHOLE + "::test_process_snapshot_parser_keeps_only_unambiguous_positive_targets"]},
        {"whole_file_count": 2},
        {"exact_node_id_count": True},
        {"scope": "complete-default-gate"},
        {"marker": "heavy"},
        {"selectors": "tests/unit"},
    )
    for changes in mutations:
        data = _small_data()
        data.update(changes)
        with pytest.raises(ValueError):
            runner._validate_manifest(data)


def test_exclusions_must_be_exact_known_nodes_in_selected_whole_files():
    excluded = _WHOLE + "::test_process_snapshot_parser_keeps_only_unambiguous_positive_targets"
    data = _small_data()
    data["known_excluded_node_ids"] = [excluded]
    data["historical_evidence"]["deselected"] = 1
    assert runner._validate_manifest(data).known_excluded_node_ids == (excluded,)
    for exclusions in ([excluded, excluded], [_WHOLE], [_EXACT]):
        invalid = deepcopy(data)
        invalid["known_excluded_node_ids"] = exclusions
        with pytest.raises(ValueError):
            runner._validate_manifest(invalid)
    data["historical_evidence"]["deselected"] = 2
    with pytest.raises(ValueError, match="recorded deselected"):
        runner._validate_manifest(data)


def test_missing_or_symlink_escaped_selected_file_is_rejected_without_import(monkeypatch):
    with pytest.raises(ValueError, match="missing or outside"):
        runner._selector("tests/unit/test_nonexistent_reviewed_fixture.py", runner.PROJECT_ROOT)
    original_resolve = Path.resolve

    def escaped(path, *args, **kwargs):
        if path == runner.PROJECT_ROOT / _WHOLE:
            return runner.PROJECT_ROOT.parent / "outside-test.py"
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", escaped)
    with pytest.raises(ValueError, match="missing or outside"):
        runner._validate_manifest(_small_data())


@pytest.mark.parametrize("body", ("not JSON", '{"schema_version": 1, "schema_version": 1}', '{}'))
def test_malformed_json_or_duplicate_fields_prevent_any_child(monkeypatch, body):
    class _ManifestInput:
        def read_text(self, *, encoding):
            assert encoding == "utf-8"
            return body

    monkeypatch.setattr(runner, "MANIFEST_PATH", _ManifestInput())
    with pytest.raises(SystemExit) as raised:
        runner.main([])
    assert raised.value.code == 2


@pytest.mark.parametrize("arguments", (
    ["tests/unit"], ["-m", "unit"], ["--list", "--maxfail=1"],
    ["--collect-only"], ["--li"], ["--manifest", "another.json"],
))
def test_cli_has_no_selector_override_or_pytest_passthrough(arguments):
    with pytest.raises(SystemExit) as raised:
        runner.main(arguments)
    assert raised.value.code == 2


@pytest.mark.parametrize("outcome", ("passed", "failed", "collection-error", "no-tests", "timeout", "interrupt", "wait-error"))
def test_one_isolated_child_has_same_deadline_exitcode_and_cleanup_as_bounded_runner(monkeypatch, capsys, outcome):
    manifest = runner._validate_manifest(_small_data())
    launches, waits, cleaned = [], [], []

    class _Child:
        pid = 19001

        def wait(self, timeout):
            waits.append(timeout)
            assert timeout == runner.bounded.TEST_TIMEOUT_SECONDS == 30.0
            if outcome == "timeout":
                raise runner.subprocess.TimeoutExpired("pytest", timeout)
            if outcome == "interrupt":
                raise KeyboardInterrupt()
            if outcome == "wait-error":
                raise RuntimeError("unexpected wait failure")
            return {"passed": 0, "failed": 1, "collection-error": 2, "no-tests": 5}[outcome]

    child = _Child()

    def launch(command, **kwargs):
        launches.append((command, kwargs))
        assert len(launches) == 1
        return child

    monkeypatch.setattr(runner, "_load_manifest", lambda: manifest)
    monkeypatch.setattr(runner.subprocess, "Popen", launch)
    monkeypatch.setattr(runner.bounded, "_terminate_process_tree", cleaned.append)
    monkeypatch.setenv("PYTEST_ADDOPTS", "-n auto -m heavy")
    monkeypatch.setenv("PYTEST_PLUGINS", "unexpected_plugin")
    if outcome == "interrupt":
        with pytest.raises(KeyboardInterrupt):
            runner.main([])
    elif outcome == "wait-error":
        with pytest.raises(RuntimeError, match="wait failure"):
            runner.main([])
    else:
        result = runner.main([])
        assert result == {"passed": 0, "failed": 1, "collection-error": 2, "no-tests": 5, "timeout": 124}[outcome]
    assert launches == [(runner._pytest_command(manifest), {
        "cwd": str(runner.PROJECT_ROOT), "start_new_session": True,
        "env": runner._child_environment(runner.os.environ),
    })]
    assert waits == [30.0]
    assert cleaned == ([child] if outcome in ("timeout", "interrupt", "wait-error") else [])
    output = capsys.readouterr()
    assert "NOT the complete default unit gate" in output.out
    if outcome == "passed":
        assert "Reviewed subset passed" in output.out and "default gate remains unverified" in output.out
    if outcome == "timeout":
        assert "30s execution deadline" in output.err
