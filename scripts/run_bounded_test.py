"""Run one statically reviewed smoke test with a process-level hard bound.

This runner intentionally has no pytest argument passthrough.  Adding a smoke
test requires a code review and an exact entry in the matching allowlist.
Even an in-memory thread/socket smoke runs in a separate pytest process so a
failed lock or teardown cannot hang the parent beyond the same hard deadline.
Execution requires POSIX process groups. --list is read-only on every platform.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Sequence, Set, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ALLOWED_NODE_IDS = frozenset(
    {
        "tests/integration/test_output_retirement_ack_path.py::test_lost_actual_adoption_ack_replays_retirement_without_reexecution",
        "tests/integration/test_task_contained_reconstruction_path.py::test_foreign_task_outer_renews_imports_replaces_edges_and_collects",
        "tests/integration/test_task_path.py::test_one_node_one_worker_task_path",
        "tests/integration/test_runner_process_tree_path.py::test_timeout_cleanup_reaps_owned_parent_and_detached_grandchild",
        "tests/integration/test_publisher_node_loss_handoff_path.py::test_promoted_output_publisher_node_loss_cleans_live_child_then_reports_unknown",
        "tests/integration/test_publisher_node_loss_handoff_path.py::test_completed_output_publisher_node_loss_keeps_success_receipt_but_result_lost",
        "tests/integration/test_put_contained_ref_path.py::test_stored_put_foreign_ref_survives_source_close_and_task_argument_import",
        "tests/integration/test_put_contained_ref_path.py::test_stored_put_dependency_survives_consumer_reconstruction",
        "tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example01]",
        "tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example02]",
        "tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example03]",
        "tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example04]",
        "tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example05]",
        "tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example06]",
        "tests/integration/test_teaching_examples_path.py::test_original_teaching_example_main_is_bounded_and_cleans_cluster[example07]",
        "tests/integration/test_cross_cleanup_receipt_path.py::test_publication_rollback_receipt_replays_after_same_object_retry_seals",
        "tests/integration/test_multi_return_path.py::test_public_multi_return_mixed_outputs_retry_dependencies_and_gc",
        "tests/integration/test_multi_contained_output_path.py::test_mixed_contained_outputs_share_one_publication_and_reconstruct_one_slot",
        "tests/integration/test_multi_output_node_loss_path.py::test_received_mixed_result_keeps_inline_and_reconstructs_only_lost_stored",
        "tests/integration/test_output_surviving_replica_path.py::test_adopted_mixed_outputs_keep_surviving_stored_replica_after_publisher_loss",
        "tests/integration/test_late_output_replica_cleanup_path.py::test_late_sealed_secondary_is_rejected_then_cleaned_after_real_consumer_cancellation",
        "tests/integration/test_foreign_late_output_replica_cleanup_path.py::test_foreign_late_replica_is_collected_and_old_messages_preserve_reconstructed_epoch",
        "tests/integration/test_multi_owner_handoff_failure_path.py::test_dead_first_owner_cancels_grant_but_hands_off_second_foreign_replica",
        "tests/integration/test_local_replica_handoff_failure_path.py::test_local_route_failure_replays_custody_without_executing_consumer",
        "tests/integration/test_local_replica_handoff_failure_path.py::test_exited_granted_executor_uses_outcome_fence_and_preserves_input_custody",
        "tests/integration/test_ambiguous_grant_custody_path.py::test_lost_grant_replies_cancel_and_transfer_both_input_replicas",
        "tests/integration/test_pregrant_custody_path.py::test_second_source_loss_hands_off_first_replica_without_a_grant",
        "tests/integration/test_transfer_pin_requester_death_path.py::test_requester_node_death_closes_only_its_source_transfer_pin",
        "tests/integration/test_transfer_pin_ack_loss_path.py::test_real_pin_ack_loss_closes_unknown_source_session_before_rejection",
        "tests/integration/test_transfer_pin_ack_loss_path.py::test_three_real_release_ack_losses_retry_from_the_existing_node_outbox",
        "tests/integration/test_abandoned_dependency_custody_path.py::test_dead_submitter_hands_granted_input_back_to_live_owner_without_child_execution",
        "tests/integration/test_output_owner_death_path.py::test_adopted_output_owner_death_cleans_live_executor_and_source_holds",
        "tests/integration/test_precomplete_output_owner_death_path.py::test_owner_death_after_intent_fences_unmaterialized_output_and_cleans_live_executor",
        "tests/integration/test_precomplete_output_owner_death_path.py::test_owner_death_after_promotions_drops_sealed_output_and_cleans_live_child_holds",
        "tests/integration/test_contained_cycle_control_path.py::test_registered_unified_graph_rejects_cycle_then_real_owner_gc_releases_container",
        "tests/integration/test_worker_owner_node_loss_path.py::test_live_worker_owner_retries_armed_child_after_certified_remote_node_death",
        "tests/integration/test_output_child_owner_worker_loss_path.py::test_dead_executor_child_cleanup_precedes_retry_on_same_live_node",
        "tests/integration/test_multi_return_reconstruction_path.py::test_multi_return_all_outputs_lost_reconstructs_once_from_nonzero_sibling",
        "tests/integration/test_multi_return_partial_reconstruction_path.py::test_one_lost_return_reconstructs_without_changing_healthy_siblings",
        "tests/integration/test_large_object_path.py::test_one_node_large_result_uses_object_store",
        "tests/integration/test_two_node_spillback.py::test_custom_resource_spills_task_to_second_node_and_cleans_cluster",
        "tests/integration/test_cross_node_dependency_pull.py::test_store_backed_dependency_pulls_to_consumer_node_before_direct_push",
        "tests/integration/test_lease_locality_path.py::test_stored_dependency_selects_data_first_hop_and_resources_can_spill_back_home",
        "tests/integration/test_worker_lease_locality_path.py::test_worker_without_snapshot_caches_cold_locality_for_foreign_stored_dependency",
        "tests/integration/test_secondary_replica_node_loss_path.py::test_dead_primary_promotes_surviving_replica_without_lineage_replay",
        "tests/integration/test_actor_k0_path.py::test_actor_is_placed_on_second_node_and_calls_use_dedicated_worker",
        "tests/integration/test_actor_cross_process_trace.py::test_actor_creation_uses_control_plane_and_method_call_bypasses_gcs",
        "tests/integration/test_actor_restart_path.py::test_actor_crash_restarts_once_fences_inflight_call_and_resets_state",
        "tests/integration/test_actor_node_loss_migration_path.py::test_actor_migrates_after_remote_node_loss_and_resets_generation",
        "tests/integration/test_public_put_path.py::test_public_put_inline_and_stored_values_without_worker_execution",
        "tests/integration/test_task_retry_path.py::test_explicit_worker_system_error_retries_once",
        "tests/integration/test_worker_crash_recovery_path.py::test_after_complete_worker_crash_recovers_output_without_reexecution",
        "tests/integration/test_worker_death_ownership_path.py::test_dead_attempt_borrower_is_swept_while_logical_hold_spans_retry",
        "tests/integration/test_worker_owner_death_path.py::test_confirmed_worker_owner_death_fences_foreign_get_wait_and_release",
        "tests/integration/test_startup_rollback_path.py::test_second_node_ready_failure_rolls_back_every_started_process",
        "tests/integration/test_node_crash_recovery_path.py::test_remote_node_death_retries_task_on_survivor_and_reports_crash",
        "tests/integration/test_driver_local_node_recovery_path.py::test_driver_local_node_death_migrates_home_and_retries_on_survivor",
        "tests/integration/test_cross_process_trace.py::test_one_task_emits_cross_process_golden_trace_and_cleans_up",
        "tests/integration/test_cross_process_trace.py::test_application_error_trace_is_terminal_without_system_retry",
        "tests/integration/test_parallel_task_lanes.py::test_two_nodes_execute_resource_pinned_tasks_concurrently",
        "tests/integration/test_worker_nested_task_path.py::test_worker_submits_child_task_and_gets_plain_result",
        "tests/integration/test_nested_task_argument_path.py::test_nested_argument_survives_sender_close_before_worker_push",
        "tests/integration/test_nested_large_argument_path.py::test_nested_large_argument_pulls_without_gating_on_pending_handle_and_collects",
        "tests/integration/test_inline_node_loss_path.py::test_received_inline_result_survives_publisher_node_loss",
        "tests/integration/test_inline_node_loss_path.py::test_unreceived_inline_result_is_lost_until_explicit_get_reconstructs",
        "tests/integration/test_worker_owned_ref_path.py::test_worker_owned_inline_ref_escapes_to_driver_and_supports_repeated_get",
        "tests/integration/test_lineage_reconstruction_path.py::test_stored_task_output_reconstructs_with_same_object_id",
        "tests/integration/test_recursive_lineage_reconstruction_path.py::test_recursive_lineage_reconstructs_leaf_to_root",
        "tests/integration/test_local_nested_reconstruction_path.py::test_local_nested_handle_survives_single_return_reconstruction",
        "tests/integration/test_two_worker_pool_path.py::test_one_node_two_workers_execute_two_tasks_concurrently",
        "tests/integration/test_blocking_get_cpu_yield_path.py::test_nested_get_yields_cpu_to_child_on_second_worker",
        "tests/integration/test_contained_ref_lifecycle_path.py::test_two_borrowers_outlive_their_inline_container",
        "tests/integration/test_foreign_stored_ref_path.py::test_inline_outer_restores_foreign_ref_then_driver_fetches_stored_bytes_from_node",
        "tests/integration/test_foreign_reconstruction_path.py::test_driver_reconstructs_worker_owned_stored_object_through_owner",
        "tests/integration/test_foreign_wait_drop_path.py::test_foreign_wait_drop_replay_then_owner_reconstruction",
        "tests/integration/test_foreign_inline_dependency_path.py::test_foreign_inline_dependency_survives_input_handle_close",
        "tests/integration/test_foreign_stored_dependency_path.py::test_foreign_stored_dependency_pulls_node_to_node_before_push",
        "tests/integration/test_foreign_input_lineage_reconstruction_path.py::test_foreign_input_hold_replaced_before_consumer_reconstruction",
        "tests/integration/test_stored_physical_gc_path.py::test_foreign_stored_dependency_collects_source_and_target_replicas",
        "tests/integration/test_stored_outer_publication_path.py::test_stored_outer_publication_adopts_graph_and_collects",
        "tests/integration/test_stored_outer_node_loss_path.py::test_precomplete_stored_outer_node_loss_rolls_back_then_retries_survivor",
        "tests/integration/test_stored_outer_node_loss_path.py::test_post_effect_precomplete_stored_outer_node_loss_compensates_then_retries",
        "tests/integration/test_stored_outer_node_loss_path.py::test_armed_unknown_stored_outer_node_loss_cleans_then_retries",
        "tests/integration/test_borrowed_output_unknown_path.py::test_armed_unknown_borrowed_output_releases_old_holds_before_retrying_same_live_child",
        "tests/integration/test_mixed_borrowed_output_unknown_path.py::test_armed_unknown_mixed_borrowed_outputs_release_each_old_slot_before_one_retry",
        "tests/integration/test_unreported_complete_node_loss_path.py::test_locally_completed_unreported_output_crash_is_unknown_then_cleans_before_retry",
        "tests/integration/test_targeted_borrowed_output_unknown_path.py::test_targeted_borrowed_arm_loss_retries_only_lost_slot_and_preserves_healthy_sibling",
        "tests/integration/test_targeted_borrowed_output_unknown_path.py::test_targeted_mixed_borrowed_arm_loss_retries_selected_batch_and_preserves_healthy_sibling",
        "tests/integration/test_stored_outer_node_loss_path.py::test_postcomplete_stored_outer_node_loss_retires_then_reconstructs",
        "tests/integration/test_placement_group_path.py::test_strict_spread_tasks_use_committed_bundles_and_remove_restores_resources",
        "tests/integration/test_placement_group_path.py::test_shutdown_removes_committed_group_without_explicit_remove",
        "tests/integration/test_placement_group_prepare_failure_path.py::test_second_participant_prepare_rejection_aborts_first_and_restores_roots",
        "tests/integration/test_placement_group_node_loss_path.py::test_participant_node_loss_is_terminal_and_survivor_cleans_pg",
        "tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[terminal]",
        "tests/integration/test_pg_publication_peer_loss_path.py::test_publication_replay_finishes_after_other_pg_bundle_node_loss[adopted]",
    }
)
# Keep ALLOWED_NODE_IDS as the original multiprocess-only allowlist: existing
# callers and tests retain their meaning.  The mode is inferred from exact
# membership, never supplied as arbitrary pytest options by the caller.
ALLOWED_LOOPBACK_NODE_IDS = frozenset(
    {
        "tests/unit/test_owner_reconstruction.py::test_concurrent_exact_requests_commit_once_and_replay_one_reply",
        "tests/unit/test_worker_side_core_contract.py::test_worker_shutdown_drains_task_before_embedded_core_and_clean_ack",
        "tests/unit/test_worker_side_core_contract.py::test_worker_shutdown_timeout_keeps_core_open_for_inflight_parent",
        "tests/unit/test_worker_side_core_contract.py::test_worker_shutdown_fences_only_new_owner_retains",
        "tests/unit/test_node_blocking_get_authority.py::test_concurrent_block_and_completion_linearize_without_leaking",
        "tests/unit/test_node_shutdown_drain.py::test_inflight_localization_blocks_drain_and_cannot_late_grant",
        "tests/unit/test_node_shutdown_drain.py::test_exact_cached_replays_are_counted_and_balance_after_begin_drain",
        "tests/unit/test_worker_side_core_contract.py::test_runtime_binding_isolates_worker_thread_and_restores_driver",
        "tests/unit/test_function_registry.py::test_concurrent_identical_registration_creates_exactly_once",
        "tests/unit/test_spillback_runtime.py::test_concurrent_duplicate_spillback_uses_one_cached_snapshot",
        "tests/unit/test_public_multi_return_runtime.py::test_concurrent_sibling_closes_claim_task_lineage_exactly_once",
        "tests/unit/test_contained_edge_runtime.py::test_publish_installs_edge_before_wake_and_same_owner_release_does_not_deadlock",
        "tests/unit/test_worker_export_pin_rollback.py::test_worker_drain_stays_unclean_while_export_release_cannot_converge",
        "tests/unit/test_borrowed_object_refs.py::test_shutdown_retries_unresolved_borrowed_release_before_clean",
        "tests/unit/test_borrowed_object_refs.py::test_shutdown_releases_live_borrowed_handle",
        "tests/unit/test_core_startup_rollback.py::test_lane_start_failure_stops_all_started_local_threads_without_rpc",
        "tests/unit/test_core_startup_rollback.py::test_lane_start_raise_after_start_is_still_joined",
        "tests/unit/test_core_startup_rollback.py::test_unpublished_abort_is_idempotent_and_never_syncs_gcs",
        "tests/unit/test_local_reference_lifecycle.py::test_each_python_handle_has_a_distinct_local_token",
        "tests/unit/test_local_reference_lifecycle.py::test_close_releases_exactly_once_and_invalidates_only_that_handle",
        "tests/unit/test_local_reference_lifecycle.py::test_shutdown_drains_accepted_releases_and_late_close_is_a_noop",
        "tests/unit/test_local_reference_lifecycle.py::test_unpickled_logical_handle_is_detached_until_borrower_protocol_exists",
        "tests/integration/test_core_shutdown_concurrency.py::test_short_shutdown_keeps_runtime_alive_until_ambiguous_push_resolves",
        "tests/integration/test_core_shutdown_concurrency.py::test_push_is_marked_unresolved_before_send_can_race_shutdown",
        "tests/integration/test_core_shutdown_concurrency.py::test_lease_is_marked_unresolved_before_grant_reply_races_shutdown",
        "tests/integration/test_core_dispatch_concurrency.py::test_two_ready_tasks_execute_on_two_dispatch_lanes",
        "tests/integration/test_core_dispatch_concurrency.py::test_shutdown_drains_all_accepted_ready_tasks",
        "tests/integration/test_core_gc_retry_timer_lifecycle.py::test_fired_reference_timer_removes_itself_after_enqueue",
        "tests/integration/test_core_gc_retry_timer_lifecycle.py::test_timer_teardown_fences_cancelled_and_future_callbacks",
        "tests/integration/test_core_gc_retry_timer_lifecycle.py::test_timed_out_teardown_retains_running_callback_for_next_pass",
        "tests/integration/test_core_reconstruction_concurrency.py::test_concurrent_lost_requests_merge_and_old_attempt_is_fenced",
        "tests/integration/test_core_reconstruction_concurrency.py::test_concurrent_multi_return_sibling_requests_start_once_and_join",
        "tests/unit/test_stored_intent_gate.py::test_arrival_frame_round_trips_exact_incarnation_and_publication",
        "tests/unit/test_stored_intent_gate.py::test_targeted_arrival_frame_preserves_full_manifest_and_selected_indices",
        "tests/unit/test_stored_intent_gate.py::test_receiver_rejects_truncated_frame_before_peer_exit",
        "tests/unit/test_inline_recovery.py::test_owner_death_and_intent_admission_linearize_atomically",
        "tests/unit/test_inline_recovery.py::test_owner_keep_drop_race_has_one_winner_and_no_payload_in_authority",
        "tests/unit/test_publication_owner_death_control.py::test_live_background_converges_owner_wide_and_publication_sagas[fence]",
        "tests/unit/test_publication_owner_death_control.py::test_live_background_converges_owner_wide_and_publication_sagas[publication]",
        "tests/unit/test_stored_publication_node_server.py::test_stored_complete_releases_state_lock_around_external_terminal_ack",
        "tests/unit/test_stored_publication_node_server.py::test_stored_outcome_validates_under_lock_then_queries_adapter_lock_free",
        "tests/unit/test_stored_publication_node_server.py::test_publication_step_external_effect_does_not_hold_node_state_lock",
        "tests/unit/test_inline_publication_node_server.py::test_pending_terminal_rpc_does_not_lock_out_complete_or_outcome",
        "tests/unit/test_owner_death_fence_control.py::test_live_progress_thread_retries_nonterminal_owner_sweep",
        "tests/unit/test_node_dependency_pull.py::test_concurrent_localizers_pull_once_and_second_uses_local_replica",
    }
)
TEST_TIMEOUT_SECONDS = 30.0
TERMINATE_GRACE_SECONDS = 2.0
PROCESS_SNAPSHOT_TIMEOUT_SECONDS = 0.25
TIMEOUT_EXIT_CODE = 124
_PS_PROCESS_TREE_COMMAND = ("ps", "-axo", "pid=,ppid=,pgid=")


def _require_posix_execution() -> None:
    """Reject unsupported cleanup before creating an isolated pytest child."""

    if (os.name != "posix" or not callable(getattr(os, "getpgrp", None))
            or not callable(getattr(os, "killpg", None))
            or not hasattr(signal, "SIGKILL")):
        raise RuntimeError(
            "bounded test execution requires POSIX process groups (Linux/macOS). "
            "Native Windows process-tree cleanup is unsupported; --list remains available."
        )


def _child_environment(environment: Mapping[str, str]) -> dict[str, str]:
    """Keep ambient pytest options and plugins outside both reviewed runners."""

    env = dict(environment)
    env.pop("PYTEST_ADDOPTS", None)
    env.pop("PYTEST_PLUGINS", None)
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    return env


@dataclass(frozen=True)
class _ProcessRecord:
    """One numeric row from a read-only process-table snapshot."""

    pid: int
    parent_pid: int
    process_group_id: int


def _positive_id(value: int, *, kind: str) -> int:
    """Reject values which POSIX could interpret as broad signal targets."""

    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("{} must be a positive integer".format(kind))
    return value


def _parse_process_snapshot(output: str) -> Dict[int, _ProcessRecord]:
    """Parse ``ps`` output, ignoring any row that is not unambiguously safe."""

    records: Dict[int, _ProcessRecord] = {}
    for line in output.splitlines():
        columns = line.split()
        if len(columns) != 3:
            continue
        try:
            pid, parent_pid, process_group_id = (int(column) for column in columns)
        except ValueError:
            continue
        # PID and PGID are signal targets and must be strictly positive.  PPID
        # zero is a valid kernel-parent sentinel, but can never be a traversal
        # root in this runner.
        if pid <= 0 or parent_pid < 0 or process_group_id <= 0:
            continue
        records[pid] = _ProcessRecord(pid, parent_pid, process_group_id)
    return records


def _parse_proc_stat(value: str, expected_pid: int) -> Optional[_ProcessRecord]:
    """Read Linux stat identity, allowing spaces and parentheses in comm."""

    prefix, closing, suffix = value.rpartition(")")
    pid_text, opening, _comm = prefix.partition(" (")
    columns = suffix.split()
    if (not opening or not closing or len(columns) < 3
            or len(columns[0]) != 1 or columns[0] not in "RSDZTtXxKWPI"
            or not pid_text.isascii() or not pid_text.isdecimal()
            or any(not part.isascii() or not part.isdecimal() for part in columns[1:3])):
        return None
    pid, parent_pid, process_group_id = (int(part) for part in (pid_text, *columns[1:3]))
    if pid != expected_pid or pid <= 0 or process_group_id <= 0:
        return None
    return _ProcessRecord(pid, parent_pid, process_group_id)


def _read_proc_process_snapshot(root: Path = Path("/proc")) -> Dict[int, _ProcessRecord]:
    """Read only numeric PID/stat entries, without requiring a procps binary."""

    deadline = time.monotonic() + PROCESS_SNAPSHOT_TIMEOUT_SECONDS
    records: Dict[int, _ProcessRecord] = {}
    for entry in root.iterdir():
        if time.monotonic() >= deadline:
            # Never return a timed-out partial scan as a complete snapshot.
            raise subprocess.TimeoutExpired("/proc/[pid]/stat", PROCESS_SNAPSHOT_TIMEOUT_SECONDS)
        name = entry.name
        if not name.isascii() or not name.isdecimal() or name.startswith("0"):
            continue
        pid = int(name)
        try:
            value = (entry / "stat").read_text(encoding="utf-8", errors="surrogateescape")
        except OSError:
            # Exit races and permission-denied entries supply no trusted identity.
            continue
        record = _parse_proc_stat(value, pid)
        if record is not None:
            records[pid] = record
    if time.monotonic() >= deadline:
        raise subprocess.TimeoutExpired("/proc/[pid]/stat", PROCESS_SNAPSHOT_TIMEOUT_SECONDS)
    return records


def _read_process_snapshot() -> Dict[int, _ProcessRecord]:
    """Take numeric identities from Linux procfs or the macOS ps interface."""

    if sys.platform.startswith("linux"):
        return _read_proc_process_snapshot()

    completed = subprocess.run(
        _PS_PROCESS_TREE_COMMAND,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=PROCESS_SNAPSHOT_TIMEOUT_SECONDS,
    )
    return _parse_process_snapshot(completed.stdout)


def _descendant_closure(
    records: Mapping[int, _ProcessRecord], roots: Iterable[int]
) -> Dict[int, _ProcessRecord]:
    """Return only rows reachable by following exact parent PID edges."""

    children: Dict[int, list[int]] = {}
    for record in records.values():
        children.setdefault(record.parent_pid, []).append(record.pid)

    pending = [root for root in roots if root > 0]
    visited: Set[int] = set()
    descendants: Dict[int, _ProcessRecord] = {}
    while pending:
        parent_pid = pending.pop()
        if parent_pid in visited:
            continue
        visited.add(parent_pid)
        record = records.get(parent_pid)
        if record is not None:
            descendants[parent_pid] = record
        pending.extend(children.get(parent_pid, ()))
    return descendants


class _TrackedProcessTree:
    """Remember identities captured before TERM, including reparented rows.

    Snapshot records contain only PID/PPID/PGID, without a birth token.
    A remembered PID is therefore trusted on a later scan only
    while its PGID is unchanged.  New rows are admitted solely through a PPID
    edge from one of those live, trusted rows.  Dead remembered PIDs are never
    used as traversal roots, which avoids adopting an unrelated tree after PID
    reuse.
    """

    def __init__(self, leader_pid: int) -> None:
        self.leader_pid = _positive_id(leader_pid, kind="pytest leader PID")
        # start_new_session=True makes this equality an invariant controlled by
        # this runner, even if the first snapshot races with leader exit.
        self._known_pgids: Dict[int, int] = {leader_pid: leader_pid}
        self._retired_pids: Set[int] = set()

    @property
    def known_pgids(self) -> Mapping[int, int]:
        return self._known_pgids

    def refresh(
        self,
        records: Mapping[int, _ProcessRecord],
        *,
        leader_alive: bool,
    ) -> Dict[int, _ProcessRecord]:
        # Once a known PID disappears from a complete snapshot, never trust a
        # later process with that numeric PID.  With only portable PID/PPID/PGID
        # columns there is no birth token that could disprove PID reuse.
        eligible_records = {
            pid: record
            for pid, record in records.items()
            if pid not in self._retired_pids
        }
        trusted_roots = {
            pid
            for pid, expected_pgid in self._known_pgids.items()
            if pid in eligible_records
            and eligible_records[pid].process_group_id == expected_pgid
        }
        # Popen.poll() is stronger identity evidence for our direct child than
        # the process table.  Seeding its exact PID also catches a child row if
        # the snapshot omitted the leader during an exit race.
        if leader_alive:
            trusted_roots.add(self.leader_pid)

        active = _descendant_closure(eligible_records, trusted_roots)
        for pid in self._known_pgids:
            if pid == self.leader_pid and leader_alive:
                continue
            if pid not in active:
                self._retired_pids.add(pid)
        for pid, record in active.items():
            if pid == self.leader_pid:
                # Never weaken the start_new_session invariant based on a
                # surprising/reused process row.
                continue
            self._known_pgids[pid] = record.process_group_id

        leader_record = records.get(self.leader_pid)
        if leader_alive:
            if (
                leader_record is not None
                and leader_record.process_group_id == self.leader_pid
            ):
                active[self.leader_pid] = leader_record
            else:
                active[self.leader_pid] = _ProcessRecord(
                    self.leader_pid, os.getpid(), self.leader_pid
                )
        return active


def _signal_targets(
    active: Mapping[int, _ProcessRecord],
    known_pgids: Mapping[int, int],
    *,
    runner_pid: int,
    runner_pgid: int,
) -> Tuple[Dict[int, Set[int]], Set[int]]:
    """Split exact descendants into safe groups and remaining PID targets."""

    groups: Dict[int, Set[int]] = {}
    for pid, record in active.items():
        pgid = record.process_group_id
        # A group is safe only when its leader was itself captured as an exact
        # descendant in that self-led group.  Inherited/foreign groups are
        # handled by individual PID below.
        if (
            pgid != runner_pgid
            and known_pgids.get(pgid) == pgid
            and pgid > 0
        ):
            groups.setdefault(pgid, set()).add(pid)

    grouped_pids = {pid for members in groups.values() for pid in members}
    remaining_pids = {
        pid
        for pid in active
        if pid > 0 and pid != runner_pid and pid not in grouped_pids
    }
    return groups, remaining_pids


def _kill_process_group(process_group_id: int, sig: int, runner_pgid: int) -> None:
    process_group_id = _positive_id(process_group_id, kind="process group ID")
    if process_group_id == runner_pgid:
        return
    try:
        os.killpg(process_group_id, sig)
    except ProcessLookupError:
        pass


def _kill_process(pid: int, sig: int, runner_pid: int) -> None:
    pid = _positive_id(pid, kind="process ID")
    if pid == runner_pid:
        return
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        pass


def _signal_active_tree(
    active: Mapping[int, _ProcessRecord],
    known_pgids: Mapping[int, int],
    sig: int,
    *,
    runner_pid: int,
    runner_pgid: int,
    signaled_group_members: Optional[Dict[int, Set[int]]] = None,
    signaled_pids: Optional[Set[int]] = None,
) -> None:
    groups, remaining_pids = _signal_targets(
        active,
        known_pgids,
        runner_pid=runner_pid,
        runner_pgid=runner_pgid,
    )
    for pgid in sorted(groups):
        members = groups[pgid]
        prior_members = (signaled_group_members or {}).get(pgid, set())
        if signaled_group_members is None or not members.issubset(prior_members):
            _kill_process_group(pgid, sig, runner_pgid)
        if signaled_group_members is not None:
            signaled_group_members.setdefault(pgid, set()).update(members)

    for pid in sorted(remaining_pids):
        if signaled_pids is None or pid not in signaled_pids:
            _kill_process(pid, sig, runner_pid)
        if signaled_pids is not None:
            signaled_pids.add(pid)


def _terminate_process_tree(process: subprocess.Popen[bytes]) -> None:
    """Terminate the pytest session plus descendants which called setsid()."""

    leader_pid = _positive_id(process.pid, kind="pytest leader PID")
    runner_pid = _positive_id(os.getpid(), kind="runner PID")
    runner_pgid = _positive_id(os.getpgrp(), kind="runner process group ID")
    tracker = _TrackedProcessTree(leader_pid)
    signaled_group_members: Dict[int, Set[int]] = {}
    signaled_pids: Set[int] = set()

    try:
        records = _read_process_snapshot()
    except (OSError, subprocess.SubprocessError):
        records = {}
    active = tracker.refresh(records, leader_alive=process.poll() is None)
    _signal_active_tree(
        active,
        tracker.known_pgids,
        signal.SIGTERM,
        runner_pid=runner_pid,
        runner_pgid=runner_pgid,
        signaled_group_members=signaled_group_members,
        signaled_pids=signaled_pids,
    )

    deadline = time.monotonic() + TERMINATE_GRACE_SECONDS
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        process.poll()
        try:
            records = _read_process_snapshot()
        except (OSError, subprocess.SubprocessError):
            time.sleep(min(0.05, remaining))
            continue
        active = tracker.refresh(records, leader_alive=process.poll() is None)
        if not active:
            break
        # A child may fork or call setsid while TERM is in flight.  Re-signal a
        # group only when a newly discovered exact member joined it.
        _signal_active_tree(
            active,
            tracker.known_pgids,
            signal.SIGTERM,
            runner_pid=runner_pid,
            runner_pgid=runner_pgid,
            signaled_group_members=signaled_group_members,
            signaled_pids=signaled_pids,
        )
        time.sleep(min(0.05, remaining))

    # A fresh final snapshot avoids signaling stale PIDs after the grace period.
    # If the process snapshot is unavailable, only the Popen-owned leader remains strong
    # enough identity evidence for a safe final signal.
    try:
        records = _read_process_snapshot()
    except (OSError, subprocess.SubprocessError):
        records = {}
    active = tracker.refresh(records, leader_alive=process.poll() is None)
    _signal_active_tree(
        active,
        tracker.known_pgids,
        signal.SIGKILL,
        runner_pid=runner_pid,
        runner_pgid=runner_pgid,
    )

    # The leader is our direct child.  Reaping it prevents a zombie without
    # making cleanup wait indefinitely if the process table became unavailable.
    if process.poll() is None:
        try:
            process.wait(timeout=TERMINATE_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            pass


def _smoke_marker(node_id: str) -> str:
    """Resolve one exact selector to its reviewed execution mode."""

    multiprocess = node_id in ALLOWED_NODE_IDS
    loopback = node_id in ALLOWED_LOOPBACK_NODE_IDS
    if multiprocess and loopback:
        raise ValueError("a bounded test cannot belong to both smoke allowlists")
    if multiprocess:
        return "multiprocess_smoke"
    if loopback:
        return "loopback_smoke"
    raise ValueError("bounded test requires an exact allowlisted node ID")


def _pytest_command(node_id: str) -> list[str]:
    return [
        sys.executable, "-m", "pytest", "-m", _smoke_marker(node_id),
        node_id, "-q", "-p", "no:cacheprovider",
    ]


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run one allowlisted, bounded mini-Ray smoke test.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "node_id",
        nargs="?",
        choices=sorted(ALLOWED_NODE_IDS | ALLOWED_LOOPBACK_NODE_IDS),
        help="complete pytest node ID (must be statically allowlisted)",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="show exact allowlisted selectors without importing tests or starting pytest",
    )
    args = parser.parse_args(argv)
    if args.list:
        if args.node_id is not None:
            parser.error("--list cannot be combined with a test node ID")
        print("Allowlisted smoke selectors (no test modules imported or pytest started):")
        for marker, selectors in (("multiprocess_smoke", ALLOWED_NODE_IDS),
                                  ("loopback_smoke", ALLOWED_LOOPBACK_NODE_IDS)):
            for selector in sorted(selectors):
                print("{} {}".format(marker, selector))
        print("Execution requires POSIX process groups; listing is not a test result or safety review.")
        return 0
    if args.node_id is None:
        parser.error("one exact test node ID is required unless --list is used")
    try:
        _require_posix_execution()
    except RuntimeError as exc:
        parser.error(str(exc))

    command = _pytest_command(args.node_id)
    process = subprocess.Popen(
        command,
        cwd=str(PROJECT_ROOT),
        start_new_session=True,
        env=_child_environment(os.environ),
    )
    try:
        # A normal pytest result is returned unchanged, including failures and
        # collection errors.
        return process.wait(timeout=TEST_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        print(
            "bounded test exceeded {:.0f}s; terminating process tree rooted at {}".format(
                TEST_TIMEOUT_SECONDS, process.pid
            ),
            file=sys.stderr,
        )
        _terminate_process_tree(process)
        return TIMEOUT_EXIT_CODE
    except BaseException:
        # ``start_new_session=True`` deliberately isolates pytest and every
        # nested Node/Worker process from the runner's terminal signals.  Any
        # interruption of this owner process must therefore clean the exact
        # tracked tree before propagating; otherwise Ctrl-C can orphan the whole
        # private cluster.
        _terminate_process_tree(process)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
