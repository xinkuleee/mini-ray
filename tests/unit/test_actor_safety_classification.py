"""Preserve the reviewed Actor test classifications without importing tests.

Only these five fixed source files are read and parsed. This guards scope and
original case identities/counts, not future fixture safety or runtime success.
No pytest collection, child process, thread, socket or real wait is performed.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


pytestmark = pytest.mark.unit


def _marker(value):
    expression = value.func if isinstance(value, ast.Call) else value
    if (isinstance(expression, ast.Attribute) and isinstance(expression.value, ast.Attribute)
            and expression.value.attr == "mark" and isinstance(expression.value.value, ast.Name)
            and expression.value.value.id == "pytest"):
        return expression.attr
    return None


@pytest.mark.parametrize("filename,unit_names,heavy_names,parameterized,expected_counts", (
    ("test_actor_control.py", (
        "test_actor_creation_selects_only_available_feasible_node_and_is_idempotent",
        "test_same_actor_id_with_different_specification_is_rejected_without_rpc",
        "test_unschedulable_actor_returns_typed_failure_and_never_calls_node",
        "test_gcs_capability_status_is_updated_and_method_calls_are_not_handlers",
        "test_closed_actor_admission_rejects_unknown_without_registering_or_reserving",
        "test_closed_admission_exact_creating_replay_redrives_frozen_reservation",
        "test_closed_admission_exact_alive_replay_returns_current_route_without_reserve",
        "test_closed_admission_existing_actor_spec_drift_remains_typed_conflict",
    ), (
        "test_actor_is_pending_until_node_reply_then_endpoint_is_published_alive",
    ), {
        "test_unschedulable_actor_returns_typed_failure_and_never_calls_node": (("total", "available", "error"), 2),
    }, (9, 1)),
    ("test_actor_public_runtime_contract.py", (
        "test_remote_class_exposes_ray_style_actor_public_api",
    ), (
        "test_actor_handle_debug_snapshot_queries_current_route_without_caching_it",
        "test_initial_actor_create_replays_exact_request_after_retryable_reply",
        "test_reply_loss_then_early_restart_installs_into_provisional_route",
        "test_initial_actor_create_nonretryable_rejection_is_not_replayed",
        "test_nonretryable_unknown_actor_rolls_back_exact_provisional",
        "test_unknown_state_cannot_delete_concurrent_late_alive_install",
        "test_initial_actor_create_replays_invalid_reply_without_new_actor_identity",
        "test_initial_create_replay_adopts_full_snapshot_if_actor_already_restarted",
        "test_shutdown_takeover_after_clean_connect_failure_prevents_another_rpc",
        "test_observed_retryable_create_ignores_shutdown_takeover_until_terminal",
        "test_three_clean_connection_failures_can_return_without_orphan",
        "test_actor_creation_uses_gcs_once_then_methods_push_directly",
    ), {}, (1, 12)),
    ("test_actor_worker.py", (
        "test_duplicate_completed_call_returns_cached_reply_without_reexecution",
        "test_stale_generation_is_fenced_before_method_execution",
        "test_application_error_is_cached_and_actor_survives_for_next_call",
        "test_clean_shutdown_direct_call_stops_immediately",
    ), (
        "test_gap_waiter_does_not_block_missing_sequence_and_fifo_execution",
        "test_shutdown_drains_inflight_call_but_abandons_gap_waiter",
        "test_shutdown_timeout_is_not_reported_as_clean_or_stopped",
        "test_clean_shutdown_defers_stop_until_daemon_handler_returns",
        "test_replayed_clean_shutdown_schedules_one_daemon_exit_joiner",
    ), {}, (4, 5)),
    ("test_core_actor_restart.py", (
        "test_max_restarts_is_actor_only_and_options_copy_is_independent",
    ), (
        "test_install_restarting_fails_old_inflight_and_new_generation_starts_at_zero",
        "test_node_loss_migration_reuses_route_fence_and_sequence_reset",
        "test_same_alive_route_transport_failure_is_typed_actor_unavailable",
        "test_transport_failure_with_new_gcs_route_fences_old_generation",
        "test_install_state_exact_replay_stale_noop_and_equal_epoch_conflict",
        "test_dead_snapshot_rejects_new_calls_immediately",
        "test_create_actor_binds_restart_policy_to_driver_owner_endpoint",
    ), {}, (1, 7)),
    ("test_node_actor_lifecycle.py", (
        "test_reserve_actor_worker_commits_after_typed_startup_and_is_idempotent",
        "test_actor_start_failure_releases_lifetime_resources",
        "test_node_stops_actor_workers_and_releases_lifetime_tokens",
        "test_actor_lifetime_allocation_is_valid_only_during_drain",
        "test_actor_stop_requires_exact_clean_ack",
    ), (
        "test_forced_actor_stop_reclaims_token_but_node_finalize_is_unclean",
        "test_shutdown_waits_for_inflight_actor_creation_then_reclaims",
    ), {
        "test_actor_stop_requires_exact_clean_ack": ("reply_factory", 4),
    }, (8, 2)),
))
def test_fixed_actor_cases_keep_exact_modes_without_inherited_unit(
    filename, unit_names, heavy_names, parameterized, expected_counts,
):
    path = Path(__file__).resolve().parent / filename
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    assert not any(isinstance(node, ast.Name) and node.id == "pytestmark"
                   and isinstance(node.ctx, ast.Store) for node in ast.walk(tree)), (
        "mixed Actor files must not attach unit to real-runtime cases by inheritance"
    )
    functions = {node.name: node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")}
    assert set(unit_names).isdisjoint(heavy_names)
    assert len(set(unit_names)) == len(unit_names) and len(set(heavy_names)) == len(heavy_names)
    assert set(functions) == set(unit_names) | set(heavy_names), "do not remove or silently omit an original case"
    counts = {"unit": 0, "heavy": 0}
    for name, function in functions.items():
        expected_mode = "unit" if name in unit_names else "heavy"
        modes = [_marker(value) for value in function.decorator_list
                 if _marker(value) in {"unit", "heavy", "loopback_smoke", "multiprocess_smoke"}]
        assert modes == [expected_mode], name
        parameters = [value for value in function.decorator_list if _marker(value) == "parametrize"]
        if name in parameterized:
            labels, count = parameterized[name]
            assert len(parameters) == 1 and isinstance(parameters[0], ast.Call), name
            arguments = parameters[0].args
            assert len(arguments) == 2 and ast.literal_eval(arguments[0]) == labels, name
            assert isinstance(arguments[1], (ast.List, ast.Tuple)) and len(arguments[1].elts) == count, name
        else:
            assert parameters == [], name
            count = 1
        counts[expected_mode] += count
    assert (counts["unit"], counts["heavy"]) == expected_counts
