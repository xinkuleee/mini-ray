"""AST locks for reviewed Worker/put/GC safety classifications.

Read source only: these checks never import, collect or run the classified
modules. Markers preserve the reviewed execution cost, not semantic success or
future safety. In particular, stale pure success fixtures stay unit and must
be migrated separately; making them heavy would hide a protocol regression.
The two push-replay files use their separately migrated in-memory fixtures;
their original IDs remain in this classification inventory.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


pytestmark = pytest.mark.unit
_UNIT_DIRECTORY = Path(__file__).parent
_MODES = {"unit", "heavy", "loopback_smoke", "multiprocess_smoke"}


def _marker_name(value):
    value = value.func if isinstance(value, ast.Call) else value
    if (isinstance(value, ast.Attribute) and isinstance(value.value, ast.Attribute)
            and value.value.attr == "mark" and isinstance(value.value.value, ast.Name)
            and value.value.value.id == "pytest"):
        return value.attr
    return None


def _source(filename):
    path = _UNIT_DIRECTORY / filename
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    functions = {node.name: node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                 and node.name.startswith("test_")}
    return tree, functions


def _expanded_count(functions):
    count = 0
    for function in functions.values():
        cases = 1
        for decorator in function.decorator_list:
            if _marker_name(decorator) == "parametrize":
                assert isinstance(decorator, ast.Call) and len(decorator.args) >= 2
                values = decorator.args[1]
                assert isinstance(values, (ast.Tuple, ast.List))
                cases *= len(values.elts)
        count += cases
    return count


@pytest.mark.parametrize("filename,unit_names,heavy_names,case_count", (
    ("test_push_replay_state.py", (
        "test_remote_call_error_then_exact_replay_success",
        "test_connection_error_after_ambiguity_preserves_exact_push",
        "test_large_replay_round_keeps_bounded_delay_and_exact_push",
    ), (), 3),
    ("test_worker_push_obligations.py", (
        "test_start_ack_loss_then_closed_exact_replay_executes_once",
        "test_closed_admission_rejects_changed_and_new_pushes",
        "test_complete_ack_loss_keeps_obligation_and_cached_replay_clears_it",
    ), (
        "test_unresolved_obligation_blocks_clean_shutdown_then_late_replay_resolves",
    ), 4),
    ("test_put_runtime.py", (
        "test_put_ids_use_stable_separate_domain",
        "test_failed_large_put_never_leaves_pending_object",
        "test_large_put_without_survivor_is_typed_and_transport_error_does_not_failover",
    ), (
        "test_inline_put_is_ready_and_has_no_producer_lineage",
        "test_large_put_seals_on_home_node_and_publishes_descriptor",
        "test_large_put_reseals_same_identity_after_captured_home_dies",
        "test_put_rejects_object_refs_and_shutdown_state",
    ), 7),
    ("test_stored_physical_gc.py", (
        "test_owner_freezes_complete_stored_plan_and_fences_every_mutator",
        "test_legacy_collection_api_cannot_bypass_stored_replica_drops",
    ), (
        "test_two_node_partial_ack_replays_only_missing_drop",
        "test_wrong_drop_ack_identity_is_ignored_until_exact_replay",
        "test_shutdown_reports_unclean_then_second_convergence_is_clean",
        "test_collection_freeze_rejects_reconstruction_before_budget_mutation",
        "test_close_before_stored_success_records_lineage_before_gc",
        "test_owner_completion_failure_never_deletes_recovery_lineage",
    ), 8),
    ("test_worker_crash_supervisor.py", (
        "test_outcome_protocol_and_handler_require_full_identity",
        "test_completed_outcome_survives_death_and_returns_only_matching_descriptor",
        "test_sealed_output_before_worker_loss_is_reported_only_as_orphan",
        "test_outcome_reply_rejects_orphan_overlap_extra_and_reordering",
        "test_node_rejects_outcome_manifest_subset_extra_and_reordering",
        "test_death_and_complete_have_one_terminal_winner",
        "test_replacement_is_fresh_and_begin_drain_prevents_replacement",
        "test_begin_drain_fences_vacant_replacement_retry",
        "test_begin_drain_reaps_child_that_died_before_supervisor_observed_it",
        "test_granted_or_running_child_death_reclaims_once",
        "test_crash_failpoint_exits_after_complete_before_task_reply",
    ), (
        "test_replacement_failures_balance_counter_and_supervisor_retries",
        "test_concurrent_complete_and_detector_release_once",
    ), 15),
))
def test_mixed_worker_classification_keeps_every_original_case(filename, unit_names, heavy_names, case_count):
    tree, functions = _source(filename)
    assert not any(isinstance(node, ast.Name) and node.id == "pytestmark"
                   and isinstance(node.ctx, ast.Store) for node in ast.walk(tree)), "heavy must not inherit unit"
    assert set(unit_names).isdisjoint(heavy_names)
    assert set(functions) == set(unit_names) | set(heavy_names)
    for name, function in functions.items():
        modes = [mode for decorator in function.decorator_list
                 if (mode := _marker_name(decorator)) in _MODES]
        assert modes == ["unit" if name in unit_names else "heavy"], name
    assert _expanded_count(functions) == case_count


@pytest.mark.parametrize("filename,mode,case_count,names", (
    ("test_core_blocking_get_notifications.py", "unit", 7, (
        "test_ready_get_and_zero_timeout_pending_get_do_not_notify",
        "test_all_ready_get_many_and_ready_error_do_not_notify",
        "test_wait_timeout_or_error_always_closes_the_blocking_episode",
        "test_get_inside_an_existing_blocking_scope_is_one_episode",
        "test_get_many_uses_one_outer_episode_for_multiple_actual_waits",
        "test_get_many_error_closes_its_single_outer_episode",
    )),
    ("test_node_shutdown_drain.py", "loopback_smoke", 2, (
        "test_inflight_localization_blocks_drain_and_cannot_late_grant",
        "test_exact_cached_replays_are_counted_and_balance_after_begin_drain",
    )),
    ("test_public_put_retry_options.py", "unit", 10, (
        "test_put_is_a_public_single_value_api",
        "test_max_retries_rejects_non_negative_integer_violations",
        "test_max_retries_is_supported_only_for_remote_functions_and_options_copy",
        "test_task_spec_rejects_invalid_max_retries",
    )),
    ("test_targeted_worker_execution.py", "unit", 4, (
        "test_worker_validates_full_arity_but_serializes_only_target_values",
        "test_worker_rejects_wrong_full_return_arity_before_target_projection",
        "test_worker_exact_replay_never_reexecutes_and_mask_drift_is_rejected",
        "test_worker_rejects_start_or_completion_ack_with_target_drift",
    )),
    ("test_two_node_spillback_contract.py", "unit", 4, (
        "test_public_init_accepts_per_node_resources",
        "test_remote_options_accept_custom_resources_without_starting_runtime",
        "test_gcs_snapshot_feeds_hybrid_policy_and_selects_only_feasible_node",
        "test_spillback_preserves_lease_and_attempt_identity",
    )),
    ("test_worker_completion_paths.py", "unit", 10, (
        "test_worker_does_not_decode_execute_or_complete_a_rejected_start",
        "test_worker_completes_every_error_reply_before_returning_it",
        "test_worker_requires_node_address_before_binding_a_real_task",
        "test_rejected_start_does_not_bind_attempt_to_lease",
        "test_same_cache_key_rejects_a_changed_push_request",
        "test_completion_retries_transport_failures_three_times_without_rerun",
        "test_worker_rejects_completion_ack_with_changed_full_identity",
        "test_reconstructed_multi_return_executes_once_and_returns_full_manifest",
    )),
    ("test_worker_death_control.py", "unit", 8, (
        "test_registration_is_exactly_idempotent_and_node_incarnation_fenced",
        "test_unknown_worker_and_wrong_incarnation_death_are_rejected",
        "test_exact_death_replay_is_already_dead_and_field_drift_conflicts",
        "test_detection_id_is_globally_bound_to_one_worker_death_proof",
        "test_fresh_replacement_worker_id_registers_after_old_worker_dies",
        "test_global_death_journal_is_ordered_and_cursor_is_a_watermark",
        "test_journal_preserves_process_node_and_expected_exit_reasons",
        "test_gcs_exposes_only_typed_worker_control_handlers",
    )),
    ("test_worker_death_node_fanout.py", "unit", 3, (
        "test_process_exit_fans_out_live_workers_in_stable_order_once",
        "test_fanout_skips_already_dead_worker_and_other_node",
        "test_expected_node_exit_does_not_create_reference_cleanup_facts",
    )),
    ("test_worker_local_dependencies.py", "unit", 8, (
        "test_worker_reads_only_verified_local_dependency_before_execution",
        "test_worker_rejects_nonlocal_dependency_without_fetching",
    )),
    ("test_worker_pool_api.py", "unit", 12, (
        "test_runtime_context_flattens_workers_node_major_and_keeps_first_views",
        "test_node_runtime_context_requires_one_or_two_aligned_worker_slots",
        "test_init_rejects_non_integer_worker_pool_size_before_spawning",
        "test_init_rejects_worker_pool_size_outside_teaching_bound",
        "test_dispatch_lanes_follow_total_workers_with_a_teaching_cap_of_two",
        "test_node_constructor_has_the_same_pool_bound_as_public_init",
    )),
    ("test_worker_pool_node.py", "unit", 2, (
        "test_two_slots_each_hold_one_active_lease_and_third_waits",
        "test_one_worker_loss_reclaims_only_its_lease_and_preserves_other_slot",
    )),
    ("test_worker_retry_failpoint.py", "unit", 2, (
        "test_failpoint_emits_one_completed_system_error_before_decode",
        "test_failpoint_config_is_deliberately_bounded",
    )),
))
def test_whole_worker_classification_preserves_reviewed_scope(filename, mode, case_count, names):
    tree, functions = _source(filename)
    assignments = [node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets)]
    assert len(assignments) == 1 and _marker_name(assignments[0].value) == mode
    assert set(functions) == set(names)
    for name, function in functions.items():
        assert not [value for value in function.decorator_list if _marker_name(value) in _MODES], name
    assert _expanded_count(functions) == case_count
