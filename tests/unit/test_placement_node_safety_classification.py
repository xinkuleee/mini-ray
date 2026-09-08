"""Preserve the fixed placement/Node test inventory without importing tests.

Only the sixteen named source files are read and parsed. The original cases,
parameter values and reviewed markers are guarded independently from semantic
success; a stale pure fixture is still pure and must fail visibly when run.
This does not collect tests, start runtime work or certify future body changes.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


pytestmark = pytest.mark.unit


def _marker(value):
    expression = value.func if isinstance(value, ast.Call) else value
    if (isinstance(expression, ast.Attribute)
            and isinstance(expression.value, ast.Attribute)
            and expression.value.attr == "mark"
            and isinstance(expression.value.value, ast.Name)
            and expression.value.value.id == "pytest"):
        return expression.attr
    return None


_FILES = (
    ("test_core_placement_group_scheduling.py", None, {
        "unit": (
            "test_committed_participant_death_fences_complete_manifest_and_queued_task",
            "test_unrelated_or_expected_node_death_does_not_mark_pg_lost",
            "test_pg_lease_reply_must_echo_exact_scheduling_key",
            "test_worker_start_and_complete_echo_task_scheduling_key",
            "test_core_pg_create_and_remove_are_typed_gcs_rpcs",
            "test_core_create_replays_one_pg_transaction_until_created",
            "test_core_create_waits_for_removed_before_raising_rejection",
            "test_core_and_gcs_adapter_converge_ambiguous_prepare_with_one_pg_id",
            "test_core_and_gcs_adapter_raise_only_after_reject_abort_is_removed",
            "test_pg_control_admission_rejects_after_shutdown_fence",
            "test_core_remove_fences_tasks_and_replays_until_removed",
            "test_core_pg_task_admission_accepts_only_created_state",
            "test_rejected_removing_reply_is_terminal_not_an_in_progress_replay",
            "test_core_and_adapter_replay_pending_against_fresh_capacity",
            "test_core_replays_accepted_created_with_incomplete_manifest",
            "test_pg_task_first_lease_hop_targets_plan_and_never_spills_back",
            "test_pg_pending_capacity_retry_preserves_exact_targeted_lease",
            "test_pg_key_survives_system_retry_and_reconstruction",
        ),
        "heavy": (
            "test_pg_create_is_a_shutdown_visible_inflight_operation",
            "test_pg_submission_final_publication_fences_removal_and_rolls_back_hold",
            "test_pg_control_timeout_hands_off_at_shutdown_fence",
        ),
    }, {
        "test_core_create_replays_one_pg_transaction_until_created": (
            "first_phase",
            "(protocol.PlacementGroupPhaseStatus.PREPARING, protocol.PlacementGroupPhaseStatus.COMMITTING)",
            2,
        ),
        "test_pg_control_timeout_hands_off_at_shutdown_fence": (
            "operation", "('create', 'remove')", 2,
        ),
    }, (19, 4, 0)),
    ("test_core_startup_rollback.py", None, {
        "unit": (
            "test_early_constructor_failure_closes_transferred_trace_sink",
            "test_api_startup_rollback_uses_local_core_abort_not_semantic_shutdown",
        ),
        "loopback_smoke": (
            "test_lane_start_failure_stops_all_started_local_threads_without_rpc",
            "test_lane_start_raise_after_start_is_still_joined",
            "test_unpublished_abort_is_idempotent_and_never_syncs_gcs",
        ),
    }, {}, (2, 0, 3)),
    ("test_cross_node_dependency_pull_contract.py", "unit", {
        "unit": (
            "test_dependency_descriptor_is_typed_immutable_metadata_without_bytes",
            "test_lease_grant_proves_dependencies_are_sealed_on_granting_node",
            "test_dependency_lists_reject_duplicate_object_ids",
            "test_transfer_messages_keep_pin_identity_and_bound_chunk_ranges",
            "test_source_pin_uses_transfer_identity_and_is_released_idempotently",
            "test_target_pull_is_not_ready_until_checksum_verified_and_store_sealed",
        ),
    }, {}, (6, 0, 0)),
    ("test_large_argument_lift.py", "unit", {
        "unit": (
            "test_threshold_boundary_and_positional_keyword_order",
            "test_two_individually_small_arguments_cross_cumulative_budget",
            "test_lift_serializes_once_and_spec_carries_only_a_stored_descriptor",
            "test_nested_object_ref_argument_lifts_with_manifest_outside_bytes",
            "test_local_nested_lift_keeps_storage_gating_and_handle_lifetime_separate",
            "test_nested_lift_serializes_user_state_once",
            "test_nested_lift_system_retry_preserves_logical_holds_and_exact_stream",
            "test_nested_lift_whole_and_targeted_reconstruction_rebind_holds_without_reseal",
            "test_nested_lift_later_encode_failure_rolls_back_local_foreign_and_storage",
            "test_last_output_gc_collects_nested_argument_stream_and_child_lifetimes",
            "test_nested_lift_seal_failure_rolls_back_source_hold_and_internal_put",
            "test_lift_rollback_collects_partial_put_after_later_encode_failure",
            "test_lift_seal_failure_rolls_back_internal_put_metadata",
            "test_ready_lift_handle_binding_failure_is_handed_to_gc",
            "test_lift_lineage_outlives_execution_and_final_output_gc_collects_both",
            "test_lost_lifted_argument_is_terminal_instead_of_staying_blocked",
            "test_prepared_lease_and_push_keep_lifted_bytes_out_of_task_spec",
            "test_worker_embedded_core_uses_the_same_argument_lift_path",
        ),
    }, {
        "test_nested_lift_whole_and_targeted_reconstruction_rebind_holds_without_reseal": (
            "num_returns", "[1, 2]", 2,
        ),
        "test_prepared_lease_and_push_keep_lifted_bytes_out_of_task_spec": (
            "with_nested_ref", "[False, True]", 2,
        ),
    }, (20, 0, 0)),
    ("test_lease_cancellation.py", None, {
        "unit": (
            "test_cancel_before_request_tombstone_prevents_late_grant",
            "test_cancel_granted_lease_releases_once_and_replay_is_noop",
            "test_running_lease_cannot_be_cancelled",
            "test_core_cancel_transport_failure_keeps_object_pending",
            "test_core_cancel_ack_is_required_before_terminal_error",
            "test_known_grant_definite_push_failure_waits_for_cancel_ack",
            "test_known_grant_cancel_transport_loss_keeps_attempt_pending",
            "test_release_string_detail_never_substitutes_for_cancel_ack",
        ),
    }, {
        "test_known_grant_definite_push_failure_waits_for_cancel_ack": (
            "release_outcome",
            "(protocol.ReleaseReply(False, 'lease already released or unknown'), TransportTimeout('release acknowledgement was lost'))",
            2,
        ),
        "test_release_string_detail_never_substitutes_for_cancel_ack": (
            "detail",
            "('lease already released or unknown', 'safe: worker never started', 'ABANDONED')",
            3,
        ),
    }, (11, 0, 0)),
    ("test_multi_return_submission_transaction.py", "unit", {
        "unit": (
            "test_failure_immediately_after_owner_manifest_registration_aborts_all",
            "test_failure_during_inert_ref_creation_precedes_all_authority",
            "test_failure_after_recovery_registration_aborts_both_authorities",
            "test_failure_after_partial_ref_binding_leaves_no_token_or_finalizer",
            "test_enqueue_failure_rolls_back_count_refs_waiters_and_authorities",
            "test_dependency_holds_and_lineage_are_exactly_rolled_back",
        ),
    }, {
        "test_failure_during_inert_ref_creation_precedes_all_authority": (
            "fail_after", "[1, 2]", 2,
        ),
        "test_failure_after_partial_ref_binding_leaves_no_token_or_finalizer": (
            "fail_after", "[1, 2]", 2,
        ),
    }, (8, 0, 0)),
    ("test_node_actor_node_loss_migration.py", None, {
        "unit": (
            "test_survivor_accepts_cross_node_proof_without_local_predecessor",
            "test_migration_rejects_changed_proof_spec_without_spawning",
        ),
        "heavy": (
            "test_migration_rejects_reused_source_worker_incarnation",
        ),
    }, {
        "test_migration_rejects_reused_source_worker_incarnation": (
            "reuse", "('worker_id', 'worker_pid')", 2,
        ),
    }, (2, 2, 0)),
    ("test_node_actor_restart.py", None, {
        "unit": (
            "test_exact_process_exit_releases_once_and_replays_stable_gcs_report",
            "test_restart_reservation_requires_exact_tombstone_and_fresh_incarnation",
            "test_drain_fence_stops_supervisor_sweeps_death_and_rejects_restart",
        ),
        "heavy": (
            "test_actor_supervisor_consumes_only_ready_child_sentinel",
        ),
    }, {}, (3, 1, 0)),
    ("test_node_dependency_pull.py", None, {
        "unit": (
            "test_source_pin_chunk_and_release_validate_one_session",
            "test_target_seals_dependency_before_allocating_and_granting",
            "test_pin_release_retries_transport_errors_and_requires_valid_ack",
            "test_source_shutdown_cleanup_releases_every_active_pin",
            "test_get_object_fences_expectations_and_returns_producer_metadata",
            "test_local_same_bytes_from_another_epoch_never_satisfy_old_descriptor",
            "test_grant_constructor_failure_rolls_back_allocation_pin_and_slot",
            "test_epoch_change_after_localization_is_revalidated_before_target_pin",
            "test_grant_rollback_unpin_failure_becomes_retryable_cleanup",
            "test_terminal_unpin_failure_retries_and_does_not_double_release_resources",
            "test_permanent_unpin_failure_keeps_shutdown_resources_unclean",
        ),
        "loopback_smoke": (
            "test_concurrent_localizers_pull_once_and_second_uses_local_replica",
        ),
    }, {}, (11, 0, 1)),
    ("test_node_lease_execution.py", None, {
        "unit": (
            "test_start_and_complete_are_idempotent_and_release_once",
            "test_wrong_execution_identity_never_changes_lease_state",
            "test_release_only_abandons_a_granted_lease",
            "test_worker_exit_reclaims_running_lease_and_fences_late_completion",
        ),
    }, {}, (4, 0, 0)),
    ("test_node_owner_death_fence.py", None, {
        "unit": (
            "test_fence_wire_binds_typed_worker_death_and_exact_replica_identity",
            "test_atomic_scan_reports_ordered_typed_states_without_deleting_bytes",
            "test_owner_wide_sweep_deletes_every_unpinned_local_replica",
            "test_owner_wide_sweep_cleans_same_object_on_primary_and_secondary",
            "test_owner_wide_sweep_retries_pinned_replica_then_caches_final_ack",
            "test_owner_wide_fence_rejects_late_seal_and_new_source_pin",
            "test_owner_wide_sweep_does_not_cache_conflicting_local_metadata",
            "test_same_owner_allows_two_publication_scans_but_fences_conflicts",
            "test_installed_fence_permanently_rejects_owner_seal_and_localization",
        ),
        "heavy": (
            "test_pull_in_progress_linearizes_before_fence_and_is_witnessed_present",
            "test_owner_wide_sweep_fences_inflight_pull_before_late_seal",
        ),
    }, {
        "test_installed_fence_permanently_rejects_owner_seal_and_localization": (
            "reason",
            "[protocol.WorkerDeathReason.PROCESS_EXIT, protocol.WorkerDeathReason.NODE_EXIT]",
            2,
        ),
    }, (10, 2, 0)),
    ("test_node_placement_group_runtime.py", "unit", {
        "unit": (
            "test_participant_handlers_are_idempotent_and_fence_digest_drift",
            "test_pg_lease_uses_child_scope_for_yield_complete_and_removal",
            "test_cancel_and_crash_reclaim_the_original_child_scope",
            "test_begin_drain_fences_pg_and_releases_root_only_after_child_terminal",
            "test_removal_does_not_release_root_for_yielded_or_zero_resource_lease",
            "test_pg_prepare_and_terminal_root_release_report_latest_hint_outside_lock",
            "test_failed_pg_resource_report_never_rolls_back_local_ledger",
            "test_pg_request_larger_than_child_is_infeasible_without_root_fallback",
            "test_ordinary_root_lease_reports_grant_release_and_retries_failure",
        ),
    }, {
        "test_cancel_and_crash_reclaim_the_original_child_scope": (
            "terminal", "['cancel', 'crash']", 2,
        ),
        "test_removal_does_not_release_root_for_yielded_or_zero_resource_lease": (
            "requested", "[_rv(1), ResourceVector.empty()]", 2,
        ),
    }, (11, 0, 0)),
    ("test_node_worker_death_runtime.py", "unit", {
        "unit": (
            "test_start_registers_node_before_starting_or_supervising_workers",
            "test_ready_worker_is_published_only_after_exact_registration_ack",
            "test_rejected_registration_stops_child_without_publishing_it",
            "test_startup_pool_registration_failure_rolls_back_published_prefix",
            "test_replacement_registers_fresh_incarnation_before_publication",
            "test_replacement_registration_failure_leaves_retryable_vacancy",
            "test_unexpected_exit_reclaims_locally_and_replays_one_frozen_report",
            "test_intentional_worker_stop_does_not_publish_process_exit",
        ),
    }, {}, (8, 0, 0)),
    ("test_placement_child_ledgers.py", "unit", {
        "unit": (
            "test_prepare_charges_root_once_and_commit_creates_independent_children",
            "test_busy_committed_abort_fences_new_leases_until_finalize",
            "test_cpu_yielded_child_remains_live_until_terminal_release",
            "test_zero_resource_active_child_remains_live_until_terminal_release",
            "test_remove_and_abort_tombstones_are_idempotent",
            "test_ledger_lookup_requires_exact_committed_identity_and_bundle",
        ),
    }, {}, (6, 0, 0)),
    ("test_placement_group_control_runtime.py", "unit", {
        "unit": (
            "test_real_participants_rollback_partial_prepare_and_fence_exact_replay",
            "test_prepare_failure_checkpoint_waits_for_real_prefix_and_binds_identity",
            "test_init_rejects_invalid_prepare_failure_config_before_spawning",
            "test_committed_node_death_marks_pg_lost_and_replay_redrives_survivor_abort",
            "test_expected_unregister_never_invokes_pg_loss_seam",
            "test_happy_create_converges_prepare_then_commit_once",
            "test_create_publishes_bundle_keys_only_after_every_commit",
            "test_exact_pending_create_replay_replans_with_fresh_node_snapshot",
            "test_ambiguous_prepare_is_replayed_without_fabricating_rejection",
            "test_prepare_rejection_converges_abort_on_every_participant",
            "test_remove_closes_visibility_before_busy_participant_and_replay_converges",
            "test_remove_first_participant_failure_still_aborts_later_participant",
            "test_prepare_rejection_starts_complete_best_effort_abort_round",
            "test_wrong_typed_reply_identity_remains_an_outstanding_obligation",
            "test_conflicting_create_reports_existing_typed_phase",
            "test_remove_unknown_and_stale_report_typed_actual_phase",
            "test_independent_drain_cancels_pg_and_final_shutdown_exits_after_clean",
            "test_drain_cancels_committing_pg_without_external_create_replay",
            "test_different_drain_epoch_is_rejected_without_pg_side_effect",
            "test_shutdown_fence_rejects_conflicting_replay_with_existing_phase",
            "test_shutdown_exact_pending_replay_does_not_start_new_obligations",
        ),
    }, {
        "test_init_rejects_invalid_prepare_failure_config_before_spawning": (
            "value,error",
            "((object(), TypeError), (PlacementGroupPrepareFailureConfig(3), ValueError))",
            2,
        ),
        "test_remove_first_participant_failure_still_aborts_later_participant": (
            "first_failure", "['busy', 'timeout']", 2,
        ),
    }, (23, 0, 0)),
    ("test_placement_group_public_api.py", "unit", {
        "unit": (
            "test_create_returns_immutable_runtime_bound_committed_plan",
            "test_protocol_never_constructs_an_accepted_but_uncommitted_group",
            "test_remote_options_forward_exact_bundle_capability",
            "test_options_require_a_valid_group_and_bundle_pair",
            "test_handle_cannot_cross_runtime_and_remove_validates_identity",
            "test_removed_or_removing_handle_is_fenced_before_function_export_or_submit",
            "test_actor_pg_options_are_explicitly_rejected",
        ),
    }, {}, (7, 0, 0)),
)


@pytest.mark.parametrize(
    "filename,module_mode,classified,parameterized,expected_counts", _FILES,
)
def test_fixed_placement_node_cases_keep_reviewed_modes_and_parameters(
    filename, module_mode, classified, parameterized, expected_counts,
):
    path = Path(__file__).resolve().parent / filename
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    stores = [node for node in ast.walk(tree)
              if isinstance(node, ast.Name) and node.id == "pytestmark"
              and isinstance(node.ctx, ast.Store)]
    assignments = [node for node in tree.body
                   if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name)
                           and target.id == "pytestmark" for target in node.targets)]
    if module_mode is None:
        assert stores == [], "mixed files must not inherit the unit marker"
    else:
        assert len(stores) == len(assignments) == 1
        assert _marker(assignments[0].value) == module_mode
    functions = {node.name: node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                 and node.name.startswith("test_")}
    expected = {}
    for mode, names in classified.items():
        assert mode in {"unit", "heavy", "loopback_smoke"}
        for name in names:
            assert name not in expected, "duplicate classified test"
            expected[name] = mode
    assert set(functions) == set(expected), "do not remove or omit original cases"
    assert set(parameterized) <= set(functions)
    counts = {"unit": 0, "heavy": 0, "loopback_smoke": 0}
    for name, function in functions.items():
        modes = [_marker(value) for value in function.decorator_list
                 if _marker(value) in {
                     "unit", "heavy", "loopback_smoke", "multiprocess_smoke",
                 }]
        if module_mode is None:
            assert modes == [expected[name]], name
        else:
            assert modes == [] and expected[name] == module_mode, name
        parameters = [value for value in function.decorator_list
                      if _marker(value) == "parametrize"]
        if name in parameterized:
            labels, values_source, count = parameterized[name]
            assert len(parameters) == 1 and isinstance(parameters[0], ast.Call), name
            arguments = parameters[0].args
            assert len(arguments) == 2 and parameters[0].keywords == [], name
            assert ast.literal_eval(arguments[0]) == labels, name
            assert isinstance(arguments[1], (ast.List, ast.Tuple)), name
            assert len(arguments[1].elts) == count, name
            expected_values = ast.parse(values_source, mode="eval").body
            assert ast.dump(arguments[1], include_attributes=False) == ast.dump(
                expected_values, include_attributes=False,
            ), name
        else:
            assert parameters == [], name
            count = 1
        counts[expected[name]] += count
    assert tuple(counts[mode] for mode in ("unit", "heavy", "loopback_smoke")) == expected_counts
