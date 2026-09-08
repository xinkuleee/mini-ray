"""Lock four reviewed observability/monitoring classifications using AST only.

Read only these fixed source files; never import or collect the mixed modules.
The checks preserve all original function names, expanded counts and modes,
not future body/fixture safety or runtime success. Original thread, socket
and file-writing cases remain heavy pending independent bounded review.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


pytestmark = pytest.mark.unit


def _marker_name(decorator):
    expression = decorator.func if isinstance(decorator, ast.Call) else decorator
    if (
        isinstance(expression, ast.Attribute)
        and isinstance(expression.value, ast.Attribute)
        and expression.value.attr == "mark"
        and isinstance(expression.value.value, ast.Name)
        and expression.value.value.id == "pytest"
    ):
        return expression.attr
    return None


@pytest.mark.parametrize(
    "filename,unit_names,heavy_names,expected_counts",
    (
        (
            "test_trace_collector.py",
            (
                "test_trace_event_converts_to_existing_wire_record",
                "test_store_deduplicates_identical_event_ids",
                "test_store_rejects_event_id_content_collision",
            ),
            (
                "test_async_sink_batches_on_background_thread",
                "test_async_sink_failure_and_queue_pressure_only_drop_trace",
            ),
            (3, 2),
        ),
        (
            "test_transport_rpc_causality_unit.py",
            (
                "test_successful_rpc_has_one_cross_boundary_causal_chain",
                "test_handler_nested_rpc_advances_outer_handler_scope",
                "test_handler_nested_rpc_inherits_server_sink_without_explicit_client_sink",
                "test_handler_business_event_and_failure_stay_in_scope",
                "test_unknown_handler_is_a_dispatch_failure_with_error_reply",
                "test_each_physical_client_attempt_gets_a_fresh_rpc_id",
                "test_server_reply_send_failure_is_not_retried_after_unknown_delivery",
                "test_server_reply_encode_failure_uses_one_fresh_fallback_attempt",
                "test_sink_override_failure_never_changes_client_outcome",
                "test_parent_cause_crosses_client_without_a_client_sink",
                "test_trace_sidecar_does_not_consume_business_frame_budget",
                "test_no_sink_and_no_scope_preserve_exact_legacy_request_envelope",
                "test_connection_failure_preserves_existing_exception_contract",
            ),
            (
                "test_fake_transport_does_not_capture_a_background_rpc",
                "test_causal_scopes_do_not_leak_between_threads_or_later_work",
            ),
            (13, 2),
        ),
        (
            "test_node_monitor.py",
            (
                "test_rejects_ambiguous_managed_identity",
                "test_thread_start_failure_closes_pipe_without_join_and_cannot_retry",
                "test_stop_before_start_is_idempotent_and_fences_later_start",
                "test_pipe_cleanup_attempts_both_ends_when_one_close_fails",
            ),
            (
                "test_reports_each_exact_process_once_and_joins_nonblocking",
                "test_wakeup_stops_without_reporting_live_process",
            ),
            (4, 2),
        ),
        (
            "test_transport_trace.py",
            (
                "test_client_receive_timeout_is_wrapped",
                "test_frame_limit_is_checked_before_connect",
                "test_memory_trace_has_process_sequence_and_cause_id",
                "test_trace_failure_never_escapes_to_runtime",
            ),
            (
                "test_tcp_request_reply_uses_explicit_handler_map",
                "test_remote_handler_error_is_returned_as_text",
                "test_jsonl_trace_writes_one_structured_event_per_line",
            ),
            (4, 3),
        ),
    ),
)
def test_fixed_observability_cases_keep_exact_modes_and_original_counts(
    filename, unit_names, heavy_names, expected_counts,
):
    path = Path(__file__).resolve().parent / filename
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    assert not any(
        isinstance(node, ast.Name)
        and node.id == "pytestmark"
        and isinstance(node.ctx, ast.Store)
        for node in ast.walk(tree)
    ), "mixed files must not inherit a module-wide execution mode"
    original_functions = [
        node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
    ]
    functions = {node.name: node for node in original_functions}
    assert len(functions) == len(original_functions), "no duplicate test names"
    assert set(unit_names).isdisjoint(heavy_names)
    assert len(set(unit_names)) == len(unit_names)
    assert len(set(heavy_names)) == len(heavy_names)
    assert set(functions) == set(unit_names) | set(heavy_names), (
        "every original contract must remain present and explicitly classified"
    )

    counts = {"unit": 0, "heavy": 0}
    mode_names = {"unit", "heavy", "loopback_smoke", "multiprocess_smoke"}
    for name, function in functions.items():
        expected_mode = "unit" if name in unit_names else "heavy"
        markers = [_marker_name(value) for value in function.decorator_list]
        modes = [value for value in markers if value in mode_names]
        assert modes == [expected_mode], name
        # These 33 historical functions have no parametrization. Do not
        # silently change expanded execution counts without another review.
        assert "parametrize" not in markers, name
        counts[expected_mode] += 1
    assert (counts["unit"], counts["heavy"]) == expected_counts
    assert len(original_functions) == sum(expected_counts)
