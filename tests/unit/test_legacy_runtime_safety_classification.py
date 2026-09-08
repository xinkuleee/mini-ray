"""Lock the reviewed modes of thirteen fixed, previously mixed test files.

Read/parse only these sources; never import their fixtures or collect tests.
This preserves reviewed identities, explicit contract replacements, parameter
counts and safety classifications,
not the truth of historical assertions or safety after future body changes.
"""

import ast
from pathlib import Path

import pytest


pytestmark = pytest.mark.unit


_CASES = (
    ("test_multi_return_partial_seal_cleanup.py", True, {
        "unit": """
            test_partial_seal_orphan_requires_exact_drop_ack_before_retry
        """,
        "heavy": "",
    }, {}, (1, 0)),
    ("test_lease_completion_handshake.py", True, {
        "unit": """
            test_protocol_rejects_cross_task_execution_identity
            test_node_fences_wrong_worker_task_and_attempt_identity
            test_start_delivered_to_a_different_node_is_rejected
            test_granted_running_completed_releases_exactly_once
            test_completion_replay_cannot_change_terminal_status
            test_worker_retries_cached_completion_without_rerunning_callable
        """,
        "heavy": "",
    }, {}, (6, 0)),
    ("test_core_worker_crash_recovery.py", False, {
        "unit": """
            test_worker_lost_outcome_retries_with_new_attempt_and_stable_identity
            test_worker_lost_orphan_is_persisted_dropped_then_retried
            test_wrong_or_retryable_drop_ack_keeps_attempt_and_cleanup_obligation
            test_partial_drop_ack_replays_only_missing_replica_before_retry
            test_application_error_orphan_cleanup_is_terminal_without_retry
            test_replacement_rejection_queries_node_instead_of_replaying_old_worker
            test_worker_lost_budget_zero_publishes_worker_died
            test_nonterminal_outcome_preserves_exact_push
            test_completed_stored_result_publishes_while_worker_remains_alive
            test_completed_without_local_bytes_and_malformed_query_keep_exact_replay
            test_cleanup_pending_outcome_keeps_original_attempt_until_exact_ack
            test_completed_application_error_detail_loss_is_not_system_retried
            test_dependency_hold_survives_worker_loss_retry
        """,
        "heavy": "",
    }, {
        "test_nonterminal_outcome_preserves_exact_push": (("lease_state", "alive"), 2),
        "test_completed_without_local_bytes_and_malformed_query_keep_exact_replay": ("inline", 2),
        "test_cleanup_pending_outcome_keeps_original_attempt_until_exact_ack": ("lease_state,status,alive", 2),
    }, (16, 0)),
    ("test_contained_edge_runtime.py", False, {
        "unit": """
            test_task_reply_edge_validation_and_worker_commit_names_outer
            test_failed_edge_release_freezes_outer_metadata_until_retry_ack
            test_pending_outer_close_collects_only_after_reply_installs_edge
            test_shutdown_retries_retained_edge_obligation_before_clean
            test_stale_reply_cannot_release_committed_publication_edges
            test_pending_closed_output_waits_for_finish_then_stored_publication_is_collected
        """,
        "heavy": "",
        "loopback_smoke": """
            test_publish_installs_edge_before_wake_and_same_owner_release_does_not_deadlock
        """,
    }, {}, (6, 0, 1)),
    ("test_large_object_runtime.py", True, {
        "unit": """
            test_node_seals_and_reads_physical_bytes_idempotently
            test_worker_large_result_reply_contains_descriptor_not_bytes
            test_core_publishes_stored_location_then_fetches_from_node
            test_owner_fetch_retries_only_after_authoritative_same_attempt_route_change
            test_owner_fetch_reads_owner_snapshot_and_route_as_one_atomic_pair
            test_owner_fetch_discards_reply_after_attempt_advances_in_flight
            test_owner_stored_get_zero_timeout_never_starts_network_io
            test_owner_fetch_does_not_retry_transport_error_without_route_change
            test_owner_fetch_does_not_spin_when_unchanged_route_resolution_fails
            test_owner_local_fetch_rejects_malformed_or_drifted_reply
        """,
        "heavy": "",
    }, {
        "test_owner_local_fetch_rejects_malformed_or_drifted_reply": ("mutation", 10),
    }, (19, 0)),
    ("test_borrowed_object_refs.py", False, {
        "unit": """
            test_acquire_wire_normalizes_full_contained_hold_source
            test_owner_acquire_release_is_idempotent_and_fences_resurrection
            test_borrower_token_is_bound_to_transfer_and_borrower_identity
            test_release_before_acquire_tombstones_reordered_delivery
            test_export_session_is_lazy_transactional_and_keeps_committed_pin
            test_exporting_ref_without_bound_owner_endpoint_creates_no_pin
            test_worker_plain_result_does_not_require_server_or_embedded_core
            test_repeated_loads_acquire_distinct_tokens_and_remote_get_inline
            test_ambiguous_acquire_attempts_release_tombstone
            test_failed_restore_compensation_persists_and_replays_exact_identity
            test_borrowed_release_requires_exact_accepted_ack
            test_successful_borrowed_close_failure_stays_shutdown_visible
            test_owner_unreachable_and_foreign_operations_are_explicit
            test_owner_rejects_new_acquire_but_serves_existing_token_while_closing
        """,
        "heavy": "",
        "loopback_smoke": """
            test_shutdown_retries_unresolved_borrowed_release_before_clean
            test_shutdown_releases_live_borrowed_handle
        """,
    }, {
        "test_borrowed_release_requires_exact_accepted_ack": ("wrong_field", 6),
    }, (19, 0, 2)),
    ("test_worker_export_pin_rollback.py", False, {
        "unit": """
            test_multi_return_contained_refs_use_one_unified_publication_and_slot_scoped_gc
            test_target_single_contained_slot_keeps_full_identity_without_publishing_other_slot
            test_worker_session_hands_failed_rollback_to_core_before_returning
            test_failed_unpin_is_visible_and_reference_mailbox_retries_it
            test_shutdown_sync_retry_keeps_export_obligation_until_tombstone
            test_stale_retry_event_cannot_duplicate_a_newer_export_release_round
        """,
        "heavy": "",
        "loopback_smoke": """
            test_worker_drain_stays_unclean_while_export_release_cannot_converge
        """,
    }, {}, (6, 0, 1)),
    ("test_public_multi_return_runtime.py", False, {
        "unit": """
            test_public_option_is_bounded_function_only_and_copied
            test_static_multi_return_contained_refs_require_unified_publication
            test_ordered_mixed_success_publishes_and_wakes_all_siblings_atomically
            test_application_error_publishes_same_terminal_error_to_all_siblings
            test_public_remote_returns_single_ref_or_ordered_tuple
            test_submission_registers_all_entries_waiters_refs_and_task_lineage
            test_system_retry_advances_all_siblings_once_and_preserves_task_key
            test_retry_owner_preflight_failure_consumes_no_recovery_budget
            test_core_stored_replay_drift_cannot_overwrite_any_descriptor
            test_invalid_success_manifest_publishes_no_sibling
            test_owner_terminal_preflight_failure_does_not_mutate_recovery_or_siblings
            test_task_lifecycle_tables_use_task_id_not_slot_zero
            test_dependency_lineage_is_task_scoped_and_releases_with_last_sibling
            test_collecting_stored_sibling_does_not_own_or_move_task_lineage
        """,
        "heavy": "",
        "loopback_smoke": """
            test_concurrent_sibling_closes_claim_task_lineage_exactly_once
        """,
    }, {
        "test_core_stored_replay_drift_cannot_overwrite_any_descriptor": ("field", 5),
        "test_invalid_success_manifest_publishes_no_sibling": ("mode", 3),
        "test_owner_terminal_preflight_failure_does_not_mutate_recovery_or_siblings": ("status", 3),
        "test_dependency_lineage_is_task_scoped_and_releases_with_last_sibling": ("close_order", 3),
    }, (24, 0, 1)),
    ("test_core_worker_death_consumer.py", False, {
        "unit": """
            test_complete_suffix_applies_in_order_then_drives_existing_gc_path
            test_later_reducer_failure_commits_only_the_applied_prefix
            test_expected_exit_advances_cursor_without_sweeping_owner_references
            test_entire_suffix_is_validated_before_the_first_reducer_mutation
            test_rpc_or_invalid_reply_never_advances_or_infers_death
            test_no_gcs_is_a_compatible_noop_without_an_rpc
            test_committed_owner_death_discharges_only_that_owners_outbound_work
            test_death_journal_installs_same_proof_in_foreign_lineage_runtime
            test_owner_timeout_is_unavailable_and_preserves_release_obligation
            test_committed_death_converges_all_outbound_tables_and_shutdown_state
            test_death_cleanup_discharges_owned_incoming_pins_without_raw_reply_authority
            test_timeout_cannot_release_typed_incoming_pin_without_death_record
            test_uninstalled_or_expected_death_cannot_sweep_outbound_work
            test_release_failure_racing_committed_death_does_not_create_retry
            test_network_timeout_keeps_foreign_release_work_and_task_unfinished
            test_late_outbound_admission_is_fenced_after_owner_death
        """,
        "heavy": "",
    }, {
        "test_rpc_or_invalid_reply_never_advances_or_infers_death": ("outcome", 3),
        "test_committed_death_converges_all_outbound_tables_and_shutdown_state": ("reason", 2),
    }, (19, 0)),
    ("test_drop_object_replica.py", False, {
        "unit": """
            test_drop_protocol_requires_one_matching_replica_epoch
            test_drop_reply_compatibility_views_follow_typed_status
            test_node_drop_is_idempotent_and_fences_reconstructed_attempt
            test_node_drop_rejects_mismatched_replica_capability
            test_node_cannot_drop_a_pinned_transfer_source
            test_missing_replica_without_matching_tombstone_is_not_acknowledged
            test_higher_deleted_epoch_fences_stale_drop
            test_present_replica_with_higher_tombstone_is_stale_not_deleted
            test_node_reports_store_metadata_inconsistency
            test_forget_failure_is_inconsistent_then_exact_replay_finishes_cleanup
            test_finalizing_node_preserves_replica_and_returns_typed_status
            test_shutdown_drain_still_serves_embedded_owner_gc_drops
            test_wrong_handler_message_type_is_a_transport_contract_error
            test_object_manager_forgets_ready_pull_only_after_bytes_are_deleted
            test_core_does_not_remove_owner_location_after_rejected_drop
            test_core_drop_marks_put_lost_and_get_reports_unreconstructable
        """,
        "heavy": "",
    }, {
        "test_drop_reply_compatibility_views_follow_typed_status": (("status", "accepted", "dropped"), 7),
        "test_node_drop_rejects_mismatched_replica_capability": ("change", 4),
        "test_node_reports_store_metadata_inconsistency": ("inconsistency", 2),
    }, (26, 0)),
    ("test_spillback_runtime.py", False, {
        "unit": """
            test_push_task_rpc_has_no_user_task_timeout
            test_connection_failure_abandons_but_receive_timeout_does_not
            test_stored_dependency_preparation_keeps_bytes_out_of_driver_path
            test_death_before_local_replica_report_restores_ready_route_atomically
            test_target_node_rechecks_local_capacity_instead_of_stale_gcs_hint
            test_pending_capacity_reuses_one_node_record_until_eventual_grant
            test_cancelled_pending_capacity_lease_can_never_grant_later
            test_shutdown_freezes_replayed_pending_lease_without_a_late_grant
            test_core_preserves_identity_and_does_not_release_from_submitter
            test_core_does_not_release_after_ambiguous_push_timeout
            test_explicit_worker_rejection_requeues_exact_push
        """,
        "heavy": "",
        "loopback_smoke": """
            test_concurrent_duplicate_spillback_uses_one_cached_snapshot
        """,
    }, {}, (11, 0, 1)),
    ("test_foreign_stored_task_dependencies.py", False, {
        "unit": """
            test_report_protocol_binds_full_credential_and_byte_free_descriptor
            test_owner_reports_target_location_idempotently_and_fences_metadata
            test_foreign_stored_prepare_keeps_ref_and_never_fetches_bytes
            test_location_reports_preserve_partial_ack_and_block_push
            test_location_report_ack_reenters_wire_validator
            test_confirmed_owner_death_cancels_grant_without_report_replay
            test_transport_failure_still_replays_without_owner_death
            test_report_stale_after_epoch_race_never_records_target
            test_foreign_grant_never_writes_borrower_owner_table
            test_consumer_retry_preserves_foreign_producer_descriptor_epoch
            test_lost_current_epoch_report_restores_ready_stored_location
            test_foreign_replica_report_after_source_death_restores_route
            test_foreign_replica_report_before_source_death_keeps_survivor_route
            test_foreign_replica_route_write_failure_has_no_owner_mutation
            test_execute_report_ambiguity_never_pushes_or_releases_hold_and_blocks_shutdown
            test_execute_replays_exact_report_then_pushes_once
            test_successful_report_survives_definite_push_failure
            test_confirmed_death_of_already_acknowledged_owner_cancels_grant
            test_noncustody_stale_is_quarantined_until_authoritative_owner_death
        """,
        "heavy": "",
    }, {}, (19, 0)),
    ("test_core_node_death_recovery.py", True, {
        "unit": """
            test_home_route_selector_keeps_live_home_and_migrates_deterministically
            test_home_route_selector_rejects_stale_or_conflicting_route
            test_death_atomically_installs_snapshot_migrates_home_and_removes_locations
            test_conflicting_installed_snapshot_is_rejected_before_death_mutation
            test_death_replay_is_exact_and_stale_or_malformed_proof_mutates_nothing
            test_death_removes_only_dead_replica_then_final_replica_becomes_lost
            test_promoted_route_does_not_change_canonical_task_reply_replay
            test_dead_location_and_late_stored_result_cannot_resurrect
            test_push_replay_consumes_death_before_rpc_and_uses_new_attempt_and_lease
            test_cancellation_consumes_death_before_rpc
            test_new_lease_uses_one_migrated_home_snapshot
            test_known_grant_cancel_keeps_frozen_requester_after_home_migration
            test_running_remote_push_is_not_rebound_when_home_changes
            test_pg_is_terminal_but_foreign_attempt_uses_normal_system_retry
            test_survivor_ambiguous_lease_cancels_before_terminal_pg_loss
        """,
        "heavy": "",
    }, {
        "test_pg_is_terminal_but_foreign_attempt_uses_normal_system_retry": ("mode", 2),
    }, (16, 0)),
)


# The retired raw TaskReply expectation was not migrated as a passing orphan
# cleanup assertion. A real post-finish duplicate cannot release a live
# committed edge; unadopted cleanup requires separate Node/GCS authority.
# The old Worker-wide multi-return contained refusal is also obsolete: current
# full/selected publications support it. The target projection preserves its
# unselected PENDING metadata and is not a whole reconstruction acceptance.
# Noncustody STALE plus Cancel cannot release a canonical foreign Task hold:
# quarantine remains until independently supplied owner-death authority.
_REPLACED_CONTRACTS = {
    "test_contained_edge_runtime.py": {
        "test_stale_reply_edges_are_released_as_orphan_obligations":
            "test_stale_reply_cannot_release_committed_publication_edges",
        "test_pending_or_stored_zero_reference_metadata_is_not_inline_collected":
            "test_pending_closed_output_waits_for_finish_then_stored_publication_is_collected",
    },
    "test_worker_export_pin_rollback.py": {
        "test_multi_return_contained_ref_rejects_before_any_seal_and_unpins_all":
            "test_multi_return_contained_refs_use_one_unified_publication_and_slot_scoped_gc",
        "test_target_single_slot_cannot_bypass_multi_return_contained_ref_rule":
            "test_target_single_contained_slot_keeps_full_identity_without_publishing_other_slot",
    },
    "test_foreign_stored_task_dependencies.py": {
        "test_typed_stale_waits_for_exact_grant_cancellation_before_hold_release":
            "test_noncustody_stale_is_quarantined_until_authoritative_owner_death",
    },
}


def _marker(value):
    expression = value.func if isinstance(value, ast.Call) else value
    if (isinstance(expression, ast.Attribute) and isinstance(expression.value, ast.Attribute)
            and expression.value.attr == "mark" and isinstance(expression.value.value, ast.Name)
            and expression.value.value.id == "pytest"):
        return expression.attr
    return None


@pytest.mark.parametrize("filename,whole_unit,modes,parameterized,counts", _CASES)
def test_fixed_runtime_cases_preserve_reviewed_modes_and_original_identity(
    filename, whole_unit, modes, parameterized, counts,
):
    path = Path(__file__).resolve().parent / filename
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    inherited = [node for node in tree.body if isinstance(node, ast.Assign)
                 and any(isinstance(target, ast.Name) and target.id == "pytestmark"
                         for target in node.targets)]
    if whole_unit:
        assert len(inherited) == 1 and _marker(inherited[0].value) == "unit"
    else:
        assert not any(isinstance(node, ast.Name) and node.id == "pytestmark"
                       and isinstance(node.ctx, ast.Store) for node in ast.walk(tree))
    functions = {node.name: node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")}
    names = {mode: value.split() for mode, value in modes.items()}
    assert set(names) <= {"unit", "heavy", "loopback_smoke"}
    assert all(len(items) == len(set(items)) for items in names.values())
    expected = {}
    for mode, items in names.items():
        for name in items:
            assert name not in expected, "test belongs to multiple execution modes"
            expected[name] = mode
    assert set(functions) == set(expected), "reviewed case omitted or silently added"
    for retired, replacement in _REPLACED_CONTRACTS.get(filename, {}).items():
        assert retired not in functions and replacement in names["unit"]
    measured = {"unit": 0, "heavy": 0, "loopback_smoke": 0}
    for name, function in functions.items():
        mode = expected[name]
        marks = [_marker(value) for value in function.decorator_list
                 if _marker(value) in {"unit", "heavy", "loopback_smoke", "multiprocess_smoke"}]
        assert marks == ([] if whole_unit else [mode]), name
        parameters = [value for value in function.decorator_list if _marker(value) == "parametrize"]
        if name in parameterized:
            labels, count = parameterized[name]
            assert len(parameters) == 1 and len(parameters[0].args) == 2, name
            assert ast.literal_eval(parameters[0].args[0]) == labels, name
            values = parameters[0].args[1]
            assert isinstance(values, (ast.Tuple, ast.List)) and len(values.elts) == count, name
        else:
            assert not parameters, name
            count = 1
        measured[mode] += count
    assert len(counts) in (2, 3)
    expected_counts = (*counts, 0) if len(counts) == 2 else counts
    assert (measured["unit"], measured["heavy"], measured["loopback_smoke"]) == expected_counts
