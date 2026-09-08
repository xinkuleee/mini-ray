# Retired Node publication test sources

The `.py.txt` files preserve the complete pre-migration test sources verbatim
(1,053 lines and 425 lines respectively). They are historical evidence, not
pytest inputs. The active same-named test modules use the unified selected-output
protocol; no compatibility flags or legacy journals are instantiated.

This mapping records **coverage implemented in test code**, not a claim that
the migrated tests have been executed. Root verification remains required.

## Former INLINE Node tests

The active file is `tests/unit/test_inline_publication_node_server.py`.
Names in the left column omit the `test_` prefix.

| Historical cases | Current contract / location |
|---|---|
| `open_prepare_bind_order_and_authority_exclusion` | Same test name; one output manifest drives provisional prepare → graph → materialization → promotion → ARM. Tier is per slot, not competing journal ownership. |
| `open_ack_loss_replays_exact_intent_before_any_effect` | Same test name; replay the unified intent before child/data effects. |
| `prepared_ack_loss_keeps_effects_and_exact_replay_only_reports` | Same name; former PREPARED readiness barrier is now the exact ARM ACK. No repeated child/graph/materialization effects. |
| `success_complete_returns_journal_envelope_without_gcs_commit`; `complete_replay_and_outcome_return_without_any_gcs_io`; `outcome_recovers_same_inline_envelope_without_descriptors` | `complete_replay_and_outcome_return_without_any_gcs_io`: real mixed envelope, local ledger release, graph stays PREPARED, no terminal/resource RPC. STORED siblings legitimately produce their matching descriptors; INLINE remains inline, never fabricated as STORED. |
| `complete_still_requires_the_exact_prepared_ack` | Same name; exact unified ARM ACK required before first local Complete. |
| `open_rejects_stored_bound_lease_without_journal_effect` | `prepare_rejects_rebound_selected_manifest_before_any_effect`; legacy-tier exclusivity no longer exists. Immutable one-manifest-per-execution binding remains. |
| `prepare_failure_is_trusted_only_after_explicit_rollback` | Same name; failed Complete releases physical lease but withholds successful cleanup acknowledgement until ordered compensation and GCS rollback report converge. |
| `explicit_abort_ack_loss_replays_retirement_before_rejection`; `aborted_ack_loss_keeps_shutdown_dirty_until_exact_replay` | `aborted_ack_loss_keeps_shutdown_dirty_until_exact_replay`; only exact report is retried after local effects finish. |
| `worker_loss_resumes_an_ambiguous_explicit_abort` | Same name; retain original rollback ID after real local Worker-loss reduction and replay unfinished effects. |
| `shutdown_cleanliness_blocks_unresolved_inline_publication` | `shutdown_cleanliness_blocks_unresolved_publication_and_unretired_payload`; terminal-report success alone does not release reply custody. |
| `terminal_ack_loss_keeps_only_background_report_pending` | Same name; local CPU release/Complete do not depend on terminal-report delivery. |
| `outcome_keeps_success_after_terminal_ack_loss_and_worker_loss` | Same name; Worker process loss never rewrites existing successful Complete. |
| `outcome_never_exposes_worker_lost_after_complete_boundary`; `worker_loss_after_complete_boundary_replays_terminal_without_commit` | `worker_loss_after_complete_boundary_preserves_success[outcome/supervisor]`; real unified journal witness plus local lease reconciliation. |
| `background_terminal_failure_releases_interrupted_local_completion` | Same name; local resources converge before background terminal callback. |
| `terminal_callback_can_read_exact_local_complete_without_another_rpc` | Same name; lock-free synchronous callback reentrancy with exact Complete/outcome envelope. |
| `owner_fence_during_terminal_rpc_defers_ack_to_owner_cleanup`; `owner_cleanup_during_terminal_rpc_cannot_resurrect_journal` | `owner_fence_during_terminal_reply_preserves_fact_but_never_restores_custody[False/True]`; a true Complete fact may be acknowledged while owner visibility is independently fenced. Exact owner finalize clears custody and prevents late report from restoring it. The old terminal-ACK flag expectation is intentionally not copied. |
| `complete_intent_alone_cannot_win_after_worker_loss` | Same name; no successful witness means first Complete remains fenced, then bounded supervisor rounds complete rollback. |
| `complete_replay_recovers_local_fault_after_resource_release` | Same name; failure after ledger mutation does not double-release resources or discard the journal Complete. |
| `supervisor_flushes_completion_metadata_without_starting_threads` | Same name; one synchronous supervisor iteration with finite fake stop event and typed resource ACK. |
| `pending_terminal_rpc_does_not_lock_out_complete_or_outcome` | Same exact name and `loopback_smoke` marker retained. Two daemon request threads, one-second gate/final join deadline, no real Node process or socket. Not part of the pure run. |

### Child-owner death proof migration

`worker_loss_rollback_waits_for_gcs_death_proof` previously exercised a special
INLINE shortcut: once the executor/child owner was authoritatively dead, Node
could replace unreachable child-release RPCs with that exact death proof.
The unified Node now uses `_release_output_child_pin`: a failed exact Release
can return a separately queried, deeply validated `GetWorkerStateReply`. The
adapter revalidates its registered incarnation, watermark and non-EXPECTED
death at effect consumption, without fabricating a success callback or Boolean
death flag. `test_output_dead_child_cleanup.py` covers the wrapper, wrong/missing
proof matrix, callback corruption and remaining live-child obligations. The
same-live-Node Worker-loss integration slice
`test_output_child_owner_worker_loss_path.py::test_dead_executor_child_cleanup_precedes_retry_on_same_live_node`
was separately reviewed and passed through the bounded runner. Wider faults
remain open; owner-death or publishing-Node-death tests alone do not prove them.

## Former Node owner-death finalize tests

The active file is `tests/unit/test_node_publication_owner_death_finalize.py`,
using `FinalizeOutputOwnerDeath(full_manifest, exact_owner_death)` and the shared
`test_output_owner_death_node` fixture.

| Historical cases | Current contract / location |
|---|---|
| `node_finalize_installs_independent_terminal_and_exact_replay[inline/stored]` | Same base name, `[running/complete]`: mixed-slot unified journal; unfinished execution becomes ABANDONED, true successful Complete stays successful; ledger/dependency cleanup and replay remain exact. No tier-specific lifecycle is reconstructed. |
| `node_finalize_rejects_request_id_owner_history_and_tier_conflicts` | `node_finalize_rejects_rebound_death_manifest_and_node_identity`; new identity is full selected manifest + physical Node incarnation + exact owner-death record. Old caller-chosen finalize ID and tier-tag conflicts are obsolete, not an extra protocol to retain. |
| `finalize_wire_revalidates_corrupted_typed_key` | `finalize_wire_revalidates_corrupted_execution_key`; constructor/deserialize validate nested unified execution identity. |
| `finalize_releases_dependency_pins_and_fences_late_inline_steps` | `finalize_releases_dependency_pins_and_fences_late_output_steps`; actual lease dependency pin is released, bytes remain, late Prepare/Complete/outcome are fenced. |
| `finalize_fences_late_stored_complete_and_claim` | `finalize_fences_late_adoption_without_rewriting_success_history`; unified owner adoption replaces legacy claim, cannot restore retired data after owner death. |

Additional current tests in `test_output_owner_death_node.py` cover partial
writes, Worker cleanup ACK loss, nonblocking busy tickets, malformed Worker ACKs,
and process-observer unavailability. This mapping does not replace those tests.
