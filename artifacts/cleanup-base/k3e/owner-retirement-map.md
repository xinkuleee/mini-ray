# K3 single-output owner retirement migration

Edited only current B tests/unit/test_output_owner_retirement.py and this audit note. No shared helper, production source, baseline manifest, repository docs, pytest, or commit changes were made by this subtask.

## Provenance and R2.2 retained scope

The original file was byte-identical in current B and fixed B/E archives: C:/Users/t-hdong/Desktop/gao/audit/two-version-cleanup/{base,enhanced}/tests/unit/test_output_owner_retirement.py.

Original SHA256: 29188bb8ca8300ab642f68cbdc18b91aa8827d5f8773a081e8c0065ea4bbe0bd.
Frozen migrated source SHA256: 8dd833f6f692e841c9f59ac4f94d370a170980655bd4e69e8aa533235e894123.

Original: 15 test functions / 26 parametrized cases. R2.2 docs/project-cleanup-plan.json:test_actions.retained_contract_groups lists 13 functions; it omits test_one_retirement_vector_can_span_old_publications_and_attempts (305) and test_post_retirement_gc_collects_metadata_only_and_preserves_sibling_lineage (342). Both are explicitly mapped below rather than silently lost.

R2.2 gives only partial_behavior_reference_not_one_for_one replacements: test_enhanced_owner_client.py::test_w3_owner_ready_and_actual_adoption_survive_unknown_c7_ack_and_early_close, ::test_w4_real_reconstruction_retires_old_edges_before_new_attempt_and_gc_preserves_history, and test_node_lost_output_resolution.py::test_known_complete_without_bytes_becomes_lost_and_keeps_live_references. These are not complete equivalents of retirement admission, exact per-proof matrices, copy isolation, or terminal replay.

## Every original function mapped

Original line numbers below refer to fixed archives. Current selectors remain in test_output_owner_retirement.py unless another file is named.

| Original function | Current B mapping | Exact boundary / retired portion |
| --- | --- | --- |
| test_retirement_preserves_stable_id_incoming_holds_and_task_lineage (78) | Same name, line 104. One LOST output retains ObjectID, current attempt, local token, incoming contained hold, incoming lineage token, TaskSpec and outgoing task lineage; actual final child releases clear only old result edges/canonical membership. An unrelated live object snapshot remains unchanged; exact retirement replay and metadata-only plan/receipt retained. | Sibling slot lifetime becomes unrelated-object isolation; multi-return sibling semantics are retired. |
| test_retirement_gates_publish_advance_and_gc_but_not_reference_release (118) | Same name, line 139. Nine supported entry points (plain INLINE/STORED/error, single attempt advance, whole-task preflight/commit/validated commit, GC begin and unified publication) reject pending retirement without whole-owner state mutation; reference release remains legal. Operations run under the same table lock when exercising caller-validated mutation. | TargetExecutionKey and TargetOutputAttemptAdvancePlan APIs retire; no old convenience publish_task_outputs call is introduced. |
| test_retirement_rejects_ready_slot_or_missing_replica_inventory_atomically (149) | Same name, line 164. READY cannot retire; after marking LOST, missing publisher Node and missing inventory key reject with full owner state unchanged and no claim. | Cross-slot READY/LOST atomic vector removed; valid single output retains each admission negative. |
| test_retirement_rejects_overlapping_claim_and_rebound_identity (166) | Same name, line 181. Different retirement ID overlaps rejected; same ID with extra replica changes immutable plan and is rejected; state unchanged and no terminal receipt. | Fully common, current API. |
| test_completion_requires_every_cleanup_proof_and_never_partially_clears (179), released_edges/graph_receipts/dropped_replicas | Same name, line 193, released_edges/dropped_replicas only. One output contains two children and two replica obligations; omitting final item from either vector rejects before owner mutation, then exact complete proofs settle. | graph_receipts belongs to E global graph composition and is removed from B call signature. Two child holds/two replicas are valid same-output obligations, not reintroduced return slots. |
| test_replica_nonterminal_status_is_not_cleanup_proof (199) | Same name, line 210. PINNED, STALE_EPOCH, NODE_DRAINING and INCONSISTENT all remain four distinct rejected Drop status cases; whole owner state unchanged. | Fully common; typed Drop values are boundary facts, not physical storage tests. |
| test_cleanup_proofs_cannot_substitute_another_hold_slot_or_epoch (211) | Same name, line 222. child-owner, child-hold, child-rejected, replica-epoch, replica-node and bool remain. Added replica-checksum validates canonical Drop identity. Two real child release receipts remain complete in count when one is corrupted, so failure tests exact identity rather than an accidental missing-proof condition. | graph-sibling removed as E graph/multi-slot composition. No replacement claims cross-slot graph coverage. |
| test_dead_publishing_node_requires_its_exact_frozen_incarnation (241) | Same name, line 252. Wrong publishing PID and registration epoch reject with full owner state unchanged; exact frozen publisher Node death settles the replica obligation. | Secondary Node epoch checking remains Core responsibility because owner manifest contains only publishing Node incarnation. This test does not infer death. |
| test_additional_replica_obligations_require_every_exact_ack (259) | Same name, line 267. Both primary and added replica nodes are in the plan; one missing exact ACK cannot retire; complete two-replica proof succeeds. | Fully common and single-output. |
| test_retired_slot_can_reconstruct_with_original_index_and_old_replays_are_fenced (272) | Same name, line 280. Retired old unified/plain publication fenced; same-lock validate_advance_task_outputs then commit_validated_advance_task_outputs advances one output; fresh publication uses new lease and new actual child hold tokens. Logical ObjectID/index 0 remains stable, new owner and child state unchanged by old publication/retirement replay; changing old Drop disposition on replay is rejected as changed proofs. | Original targeted index 1 is retired. Stable single-output index 0 is preserved; this is whole-function attempt advance, not targeted reconstruction. |
| test_one_retirement_vector_can_span_old_publications_and_attempts (305), omitted from retained groups | Retired without a new same-purpose selector. Current OutputOwnerPublicationRetirementPlan requires exactly one output membership. Supported old/new-attempt isolation is covered by the previous migrated test plus test_output_owner_retired_fencing.py::test_next_attempt_accepts_fresh_cross_tier_publication_without_replaying_old_retirement. | Mixed multi-return publication/attempt retirement vector is outside current B/E single-output contract. Do not recreate TargetOutputManifest or central graph merely to preserve this old vector. |
| test_empty_edge_slot_retires_without_graph_or_child_operations (334) | Same name, line 306. One no-child STORED output has no release operations/child entries; exact Drop boundary ACK permits retirement. | GCS graph no longer exists in B and no fake empty graph receipt is supplied. |
| test_post_retirement_gc_collects_metadata_only_and_preserves_sibling_lineage (342), omitted from retained groups | Renamed test_post_retirement_gc_collects_metadata_only_and_releases_final_task_lineage, line 314. Retirement retains task lineage. Closing the single output yields a metadata collection with no locations/old child edges/canonical metadata and exactly final task-lineage release; GC removes that task's lineage and preserves an unrelated object's lineage and snapshot. Exact retirement query remains ALREADY_APPLIED. | Old expectation of no lineage release while a healthy sibling survives is multi-return-specific and retires. Single-output final GC must release its task lineage, not manufacture a sibling. |
| test_normal_collection_and_reconstruction_retirement_cannot_overlap (364) | Same name, line 339. Actual current output collection claim makes retirement reject ObjectCollectionInProgressError with full owner state unchanged. | Fully common. |
| test_public_plans_proofs_and_terminal_getters_cannot_mutate_owner_history (375) | Same name, line 349. Mutated outward plan owner ID cannot poison admitted claim; caller Drop proof Node ID, returned receipt manifest digest and queried plan ObjectID mutations cannot change terminal owner history. Exact saved plan/proofs still replay, and all retirement/publication histories pass the existing metadata scan. | Graph-receipt copy checks removed with graph mechanism; deep isolation of current actual plans/proofs/receipts retained. |

## Minimal additional canonical integrity coverage

New test_canonical_result_is_revalidated_before_retirement_claim_and_completion (line 372) has size_bytes/checksum parameters. Each injects one canonical descriptor field mismatch into a LOST owner entry before admission and after an actual immutable retirement claim. Both phases must reject without mutation; restoring the exact original field permits the original complete proof set. This complements the new wrong Drop-checksum negative and tests current owner canonical revalidation rather than treating old retirement tombstones as permission to clear inconsistent metadata.

## Installed child death evidence remains in current CF tests

The separate current test_owner_retirement.py is an actual single-output owner fixture and remains unchanged. Its ten cases are necessary shared proof coverage, but are not claimed to replace this file's 26 cases:

- test_mixed_release_and_exact_installed_child_death_retire_without_fabricated_ack (117): actual release for one final hold plus exact already-installed death for the other, no fake Release.
- test_one_actual_owner_death_can_cover_each_of_its_distinct_final_holds (137): same exact death may cover multiple final holds, with an entry for each obligation.
- test_unproven_or_wrong_child_death_never_partially_retires (146), missing/wrong-installed/other-owner/expected-exit/changed-incarnation: each rejects before partial owner retirement.
- test_death_coverage_is_not_a_reason_to_skip_an_unsettled_final_hold (167): death proof is not permission to omit another hold.
- test_death_receipt_replay_cannot_change_a_proof_after_retirement (176): exact installed proof remains bound in terminal replay.
- test_mutating_nested_death_input_cannot_change_retained_receipt (189): nested supplied death mutation cannot corrupt saved receipt.

Production complete_output_publication_retirement validates each proof vector and then compares every WorkerDeathRecord against the installed immutable owner fence before clearing old result state. No new source or fake death pathway is introduced here.

## Current implementation and cost

Migrated source: 15 functions / 26 cases. It projects 14 original business contracts, retires the unsupported mixed vector, and adds one two-case canonical descriptor test. The old graph proof parameter is removed; graph-sibling parameter is replaced by canonical Drop-checksum identity coverage. Case counts happen to remain 26; this is not a statement of one-to-one semantic equivalence for removed multi-slot/graph assertions.

Each fixture uses the read-only current test_output_owner_publication._Fixture, builds at most one single-output manifest with two child transfers, and prepares/promotes them through one real child owner table. _proofs invokes actual final-hold release before returning a typed ACK, so release evidence is not constant True. Replica Drop/Node death receipts are declared model inputs with no claim of actual store deletion or process exit.

Per case limits: one output, up to two child entries, up to two Drop obligations, one owner table and one child table; optional one unrelated control object; at most two output attempts in the reconstruction test. No GCS/Node/Core/Worker/TCP constructors, transport, processes, threads, waits, sleeps or user execution. No imported live fixture or test entry point is invoked. All faults are finite direct owner reducer calls. No other files were edited.

## E and remaining limits

- Global graph-retirement receipt proof and graph/sibling negative assertions remain E-only composition work if still part of the enhanced single-output lifecycle; current enhanced owner-client references are partial, not equivalence evidence.
- Multi-return mixed-publication retirement vectors, targeted index 1 reconstruction and sibling-lineage retention retire in both current single-output editions. Single-output child lifetime, stable ID/attempt fences and final-lineage release remain fully explicit.
- Current CF installed-death proof tests remain required alongside this file; neither subset substitutes for the other.
- This owner-level suite does not prove actual replica deletion, remote acknowledgements, execution/reconstruction scheduling or concurrency. Those boundaries need independently reviewed runtime tests.

## Validation and freeze

Syntax-only ast.parse passed and counted 15 functions / 26 cases. git diff --check passed (normal working-tree LF/CRLF warning only). Static inspection verified current single-output APIs, supported validated-advance sequencing and real child release bindings. No pytest/import execution or commits were performed; parent owns coordinated acceptance and source freeze.
