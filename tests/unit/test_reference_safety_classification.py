"""AST locks for the sixteen reviewed reference-test classifications.

Only these fixed source files are read. No mixed module is imported or
collected, and no runtime fixture is started. This guards original names,
execution modes and expanded counts; it does not prove future body safety
or that the historical semantics still pass.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


pytestmark = pytest.mark.unit


def _marker_name(decorator):
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


_REVIEWED = (
    (
        "test_contained_edge_gc.py",
        (
            "test_legacy_boolean_collection_never_drops_outgoing_obligations",
            "test_atomic_collection_returns_edges_only_after_outer_tokens_vanish",
            "test_collection_release_obligations_remove_child_pins_idempotently",
            "test_export_commit_builds_exact_outer_edges_and_rollback_builds_none",
            "test_legacy_commit_without_outer_id_retains_pin_but_has_no_edge_metadata",
            "test_multi_return_rejection_rolls_back_every_discovered_export_pin",
            "test_export_session_retains_failed_unpin_for_explicit_replay",
        ),
        (),
        {},
        (7, 0),
    ),
    (
        "test_contained_pin_owner_identity.py",
        (
            "test_typed_hold_snapshot_keeps_authority_and_legacy_projection",
            "test_outgoing_edge_derives_exact_incoming_hold",
            "test_release_tombstone_is_full_identity_and_prevents_late_add",
            "test_death_cleanup_releases_only_matching_container_owner_holds",
            "test_dead_fence_rejects_late_typed_hold_but_never_guesses_legacy_owner",
            "test_death_cleanup_tombstone_blocks_exact_typed_replay",
        ),
        (),
        {},
        (6, 0),
    ),
    (
        "test_core_foreign_lineage_runtime.py",
        (
            "test_submission_registers_lineage_and_terminal_finish_keeps_hold",
            "test_reconstruction_replaces_hold_before_local_owner_attempt_advances",
            "test_ambiguous_replacement_never_advances_local_authorities",
            "test_final_output_collection_releases_foreign_lineage_hold",
            "test_registry_only_lineage_is_a_shutdown_barrier",
        ),
        (),
        {},
        (5, 0),
    ),
    (
        "test_core_lineage_holds.py",
        (
            "test_lineage_hold_outlives_execution_and_releases_with_producer",
            "test_nested_lineage_hold_outlives_execution_and_releases_with_producer",
        ),
        (),
        {},
        (2, 0),
    ),
    (
        "test_core_nested_task_arguments.py",
        (
            "test_nested_local_ref_is_held_but_never_gates_readiness",
            "test_nested_hold_survives_system_retry_and_releases_once",
            "test_ambiguous_foreign_nested_retain_is_recorded_before_rpc",
            "test_top_level_and_nested_foreign_handles_share_one_logical_hold",
        ),
        (
            "test_attempt_borrow_release_obligation_replays_exact_identity",
            "test_unreachable_owner_does_not_block_attempt_but_keeps_core_unclean",
        ),
        {},
        (4, 2),
    ),
    (
        "test_core_owner_integration.py",
        (
            "test_submission_registers_attempt_lineage_and_local_handle",
            "test_recovery_registration_preflight_fails_before_owner_or_hold_mutation",
            "test_object_ref_pickle_contains_only_logical_handle",
            "test_dependency_is_protected_until_dispatch_releases_hold",
            "test_reply_publishes_owner_state_before_waking_and_fences_stale_attempt",
            "test_shutdown_error_fences_a_late_successful_reply",
            "test_successful_reply_fences_a_late_shutdown_error",
        ),
        (),
        {},
        (7, 0),
    ),
    (
        "test_core_reconstruction_runtime.py",
        (
            "test_pure_mailbox_preserves_real_close_and_explicit_collection",
            "test_local_lost_get_commits_one_reconstruction_plan",
            "test_shutdown_fence_prevents_reconstruction_state_mutation",
            "test_reconstruction_terminal_error_converges_all_three_authorities",
            "test_recursive_chain_requeues_dependency_first_with_stable_ids",
            "test_nested_local_reconstruction_installs_fresh_lifetime_hold",
            "test_multi_return_all_lost_requeues_full_manifest_from_nonzero_sibling",
            "test_multi_return_partial_loss_opens_then_starts_only_lost_target",
            "test_partial_target_success_publishes_only_target_and_preserves_healthy",
            "test_late_loss_starts_second_session_with_distinct_lifecycle_key",
            "test_target_start_renews_foreign_lineage_before_owner_attempt_commit",
            "test_target_start_installs_renewed_foreign_guards_and_nested_hold",
            "test_target_open_waiting_backs_off_and_definitive_failure_wakes_targets",
            "test_targeted_explicit_system_error_queries_node_before_retry",
            "test_multi_return_reconstruction_error_wakes_every_sibling",
        ),
        (),
        {},
        (15, 0),
    ),
    (
        "test_core_task_retry.py",
        (
            "test_reconstruction_system_retry_success_and_next_loss_start_cleanly",
            "test_reconstruction_system_retry_exhaustion_clears_both_markers",
            "test_reconstruction_identity_disagreement_consumes_no_retry_budget",
            "test_explicit_system_error_retries_with_stable_logical_ids",
            "test_retry_exhaustion_publishes_current_attempt_error",
            "test_dependency_hold_survives_intermediate_retry",
        ),
        (),
        {},
        (6, 0),
    ),
    (
        "test_dead_worker_reference_cleanup.py",
        (
            "test_death_cleanup_is_narrow_and_only_reports_gc_candidates",
            "test_dead_fence_rejects_unseen_late_adds_and_preserves_normal_cascade",
            "test_dead_worker_replay_and_proof_conflicts_are_immutable",
        ),
        (),
        {},
        (3, 0),
    ),
    (
        "test_foreign_inline_task_dependencies.py",
        (
            "test_retained_hold_is_bound_idempotent_and_survives_parent_release",
            "test_release_before_retain_tombstones_and_generic_api_is_forbidden",
            "test_foreign_dependency_protocol_validates_full_identity_and_payload",
        ),
        (
            "test_registration_retain_is_outside_state_lock_and_shutdown_wins",
            "test_ambiguous_retain_rolls_back_exact_token_and_retains_obligation",
            "test_foreign_pending_stays_off_lane_then_inline_rewrites_and_executes",
            "test_pending_foreign_dependency_does_not_block_independent_ready_task",
            "test_foreign_error_or_lost_never_executes_user_path",
            "test_retry_keeps_same_foreign_guard_and_input_handle_may_close",
            "test_finish_is_once_and_partial_multi_guard_retry_skips_released",
            "test_finish_claim_and_protocol_send_are_mutually_fenced",
            "test_execute_cannot_send_after_finish_claim",
            "test_existing_protocol_fence_prevents_finish_release",
            "test_owner_shutdown_waits_for_active_retained_hold_then_closes",
        ),
        {
            "test_foreign_error_or_lost_never_executes_user_path": (
                ("state", "error_type",), 2,
            ),
        },
        (3, 12),
    ),
    (
        "test_foreign_lineage_runtime_service.py",
        (
            "test_all_owner_acks_precede_dependency_gate_and_local_commit_permission",
            "test_lost_dependency_is_reconstructed_at_owner_before_parent_becomes_ready",
            "test_nested_only_edge_renews_but_does_not_gate_dependency_readiness",
            "test_ambiguous_replace_preserves_exact_request_and_ack_bitmap",
            "test_owner_stopped_after_ambiguous_send_keeps_convergence_obligation",
            "test_later_already_replaced_ack_clears_prior_owner_stopped_failure",
            "test_partial_ack_and_definitive_failure_cannot_abandon_mixed_registry",
            "test_remote_owner_dead_reply_does_not_install_local_death_or_discharge",
            "test_committed_owner_death_is_terminal_and_clears_ambiguous_rpc",
            "test_ready_result_is_fenced_when_death_mutates_session_before_commit",
            "test_final_sibling_collection_releases_current_replacement_holds_once",
            "test_collection_ambiguity_replays_only_missing_release_and_blocks_shutdown",
            "test_dead_owner_discharges_final_release_without_rpc",
            "test_shutdown_converges_ambiguous_replacement_before_current_hold_release",
            "test_shutdown_keeps_unresolved_replace_and_does_not_release_stale_hold",
        ),
        (),
        {},
        (15, 0),
    ),
    (
        "test_foreign_reconstruction_runtime.py",
        (
            "test_foreign_reconstruction_failures_have_nondeath_typed_mapping",
            "test_foreign_lost_get_routes_start_to_owner_then_polls_same_object",
            "test_lost_ack_replay_returns_exact_cached_start_without_second_enqueue",
            "test_worker_proxy_routes_owned_reconstruction_to_embedded_core",
            "test_deadline_transport_loss_is_unavailable_until_death_fence",
        ),
        (),
        {
            "test_foreign_reconstruction_failures_have_nondeath_typed_mapping": (
                ("failure", "error_type",), 4,
            ),
        },
        (8, 0),
    ),
    (
        "test_foreign_stored_object_refs.py",
        (
            "test_get_owned_object_reply_is_a_strict_state_payload_sum_type",
            "test_stored_fetch_subtracts_resolution_time_and_never_starts_after_deadline",
        ),
        (
            "test_owner_and_worker_publish_only_canonical_live_stored_metadata",
            "test_nested_ref_restore_then_stored_get_uses_owner_metadata_and_node_bytes",
            "test_borrower_rejects_every_stored_reply_identity_or_integrity_mismatch",
        ),
        {
            "test_borrower_rejects_every_stored_reply_identity_or_integrity_mismatch": (
                ("corruption",), 9,
            ),
        },
        (2, 11),
    ),
    (
        "test_foreign_task_finish_barrier.py",
        (
            "test_lost_finish_gate_is_wire_pending_without_mutating_owner",
            "test_projection_captures_snapshot_and_gate_in_one_composition",
            "test_finish_gate_never_hides_an_already_ready_inline_value",
            "test_finish_projection_does_not_bypass_borrower_validation",
            "test_core_admission_distinguishes_deferred_from_started_outcome",
            "test_borrower_repolls_temporary_reconstruction_failure",
            "test_temporary_reconstruction_never_resets_public_timeout",
            "test_permanent_reconstruction_failures_still_surface",
        ),
        (),
        {
            "test_lost_finish_gate_is_wire_pending_without_mutating_owner": (
                ("retained",), 2,
            ),
            "test_projection_captures_snapshot_and_gate_in_one_composition": (
                ("retained",), 2,
            ),
            "test_finish_gate_never_hides_an_already_ready_inline_value": (
                ("retained",), 2,
            ),
            "test_borrower_repolls_temporary_reconstruction_failure": (
                ("failure",), 3,
            ),
            "test_permanent_reconstruction_failures_still_surface": (
                ("failure", "error",), 2,
            ),
        },
        (14, 0),
    ),
    (
        "test_foreign_wait_drop.py",
        (),
        (
            "test_foreign_wait_is_metadata_only_and_preserves_input_order",
            "test_foreign_wait_terminal_lost_is_ready_without_fetching_bytes",
            "test_owner_drop_validates_capability_updates_only_owner_and_exact_replays",
            "test_public_foreign_drop_routes_owner_handler_and_returns_node_outcome",
            "test_worker_drop_proxy_preserves_typed_credential_rejection",
        ),
        {},
        (0, 5),
    ),
    (
        "test_local_reference_lifecycle.py",
        (),
        (
            "test_gc_finalizer_enqueues_release_but_does_not_collect_owner_metadata",
            "test_object_ref_finalizer_does_not_keep_core_worker_alive",
        ),
        {},
        (0, 2),
    ),
)

_REVIEWED_LOOPBACK = {
    "test_local_reference_lifecycle.py": (
        "test_each_python_handle_has_a_distinct_local_token",
        "test_close_releases_exactly_once_and_invalidates_only_that_handle",
        "test_shutdown_drains_accepted_releases_and_late_close_is_a_noop",
        "test_unpickled_logical_handle_is_detached_until_borrower_protocol_exists",
    ),
}


@pytest.mark.parametrize(
    "filename,unit_names,heavy_names,parameterized,expected_counts", _REVIEWED,
)
def test_fixed_reference_cases_keep_original_names_counts_and_modes(
    filename, unit_names, heavy_names, parameterized, expected_counts,
):
    path = Path(__file__).resolve().parent / filename
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    assert not any(
        isinstance(node, ast.Name)
        and node.id == "pytestmark"
        and isinstance(node.ctx, ast.Store)
        for node in ast.walk(tree)
    ), "reviewed reference cases must have explicit per-function modes"
    original_functions = [
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    ]
    functions = {node.name: node for node in original_functions}
    assert len(functions) == len(original_functions), "no duplicate test names"
    assert len(set(unit_names)) == len(unit_names)
    assert len(set(heavy_names)) == len(heavy_names)
    loopback_names = _REVIEWED_LOOPBACK.get(filename, ())
    assert len(set(loopback_names)) == len(loopback_names)
    assert set(unit_names).isdisjoint(heavy_names)
    assert set(loopback_names).isdisjoint(set(unit_names) | set(heavy_names))
    assert set(functions) == set(unit_names) | set(heavy_names) | set(loopback_names), (
        "retain every original case and explicitly review any added case"
    )
    assert set(parameterized) <= set(functions)

    counts = {"unit": 0, "heavy": 0, "loopback_smoke": 0}
    mode_names = {"unit", "heavy", "loopback_smoke", "multiprocess_smoke"}
    for name, function in functions.items():
        expected_mode = ("unit" if name in unit_names else
                         "loopback_smoke" if name in loopback_names else "heavy")
        modes = [
            _marker_name(value) for value in function.decorator_list
            if _marker_name(value) in mode_names
        ]
        assert modes == [expected_mode], name
        parameters = [
            value for value in function.decorator_list
            if _marker_name(value) == "parametrize"
        ]
        if name in parameterized:
            expected_labels, expanded = parameterized[name]
            assert len(parameters) == 1, name
            parameter = parameters[0]
            assert isinstance(parameter, ast.Call) and len(parameter.args) == 2, name
            labels = ast.literal_eval(parameter.args[0])
            if isinstance(labels, str):
                labels = tuple(value.strip() for value in labels.split(","))
            assert tuple(labels) == expected_labels, name
            values = parameter.args[1]
            assert isinstance(values, (ast.Tuple, ast.List)), name
            assert len(values.elts) == expanded, name
        else:
            assert not parameters, name
            expanded = 1
        counts[expected_mode] += expanded
    assert (counts["unit"], counts["heavy"]) == expected_counts
    assert counts["loopback_smoke"] == len(loopback_names)
