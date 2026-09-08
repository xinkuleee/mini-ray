"""Pure AST locks for two reviewed recovery test classifications.

This checks the explicit historical classification only. It neither imports
the mixed modules nor proves future test bodies, fixtures or imports safe.
Concurrent originals remain non-unit: the owner-reconstruction replay is a
reviewed bounded L1; the owner-death cross-product race still needs review.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


pytestmark = pytest.mark.unit
_UNIT_DIRECTORY = Path(__file__).parent


def _marker_name(decorator):
    value = decorator.func if isinstance(decorator, ast.Call) else decorator
    if (isinstance(value, ast.Attribute) and isinstance(value.value, ast.Attribute)
            and value.value.attr == "mark" and isinstance(value.value.value, ast.Name)
            and value.value.value.id == "pytest"):
        return value.attr
    return None


@pytest.mark.parametrize("filename,pure_names,concurrent_name,concurrent_mode", (
    ("test_owner_death_fence_registry.py", (
        "test_owner_death_fans_out_to_every_live_node_without_publications",
        "test_late_registration_gets_every_historical_owner_fence_before_safe",
        "test_registration_and_death_orders_produce_the_same_exact_effect",
        "test_exact_replays_do_not_duplicate_and_conflicting_proofs_fail",
        "test_acknowledgement_is_exact_and_lost_ack_replay_is_idempotent",
        "test_pending_queries_are_owner_and_exact_node_scoped",
        "test_exact_node_death_discharges_only_that_incarnation_pending_work",
        "test_fence_ack_and_node_death_race_has_one_monotonic_terminal",
        "test_expected_worker_exit_is_not_an_owner_death_fence",
    ), "test_registration_owner_death_race_never_misses_cross_product_entry", "heavy"),
    ("test_owner_reconstruction.py", (
        "test_protocol_echoes_full_capability_and_enforces_reply_shape",
        "test_start_is_owner_authoritative_and_exact_ack_replay_survives_release",
        "test_second_active_capability_joins_the_same_reconstruction_attempt",
        "test_source_binding_is_verified_and_claim_drift_is_a_typed_conflict",
        "test_failed_forgery_does_not_poison_a_later_exact_valid_request",
        "test_released_and_inactive_credentials_never_reach_local_authority",
        "test_put_exhausted_not_lost_and_committed_death_are_distinct",
        "test_expected_attempt_is_a_cas_fence_unless_an_active_attempt_can_be_joined",
        "test_borrowed_compatibility_form_normalizes_to_typed_credential",
        "test_active_retained_credential_can_start_after_parent_borrower_release",
        "test_released_retained_credential_is_rejected",
    ), "test_concurrent_exact_requests_commit_once_and_replay_one_reply", "loopback_smoke"),
))
def test_recovery_classification_preserves_original_cases_without_inherited_unit(filename, pure_names, concurrent_name, concurrent_mode):
    path = _UNIT_DIRECTORY / filename
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    assert not any(isinstance(node, ast.Name) and node.id == "pytestmark"
                   and isinstance(node.ctx, ast.Store) for node in ast.walk(tree)), "heavy must not inherit unit"
    functions = {node.name: node for node in tree.body
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")}
    assert concurrent_name not in pure_names
    assert set(functions) == set(pure_names) | {concurrent_name}, "keep every original contract explicitly classified"
    mode_names = {"unit", "heavy", "loopback_smoke", "multiprocess_smoke"}
    expanded = 0
    for name, function in functions.items():
        modes = [mode for decorator in function.decorator_list if (mode := _marker_name(decorator)) in mode_names]
        assert modes == [concurrent_mode if name == concurrent_name else "unit"], name
        parameters = [decorator for decorator in function.decorator_list if _marker_name(decorator) == "parametrize"]
        if name == "test_put_exhausted_not_lost_and_committed_death_are_distinct":
            assert len(parameters) == 1
            assert ast.literal_eval(parameters[0].args[0]) == ("fixture_kwargs", "failure")
            assert len(parameters[0].args[1].elts) == 4
            expanded += 4
        else:
            assert not parameters, name
            expanded += 1
    assert expanded == (10 if filename == "test_owner_death_fence_registry.py" else 15)
