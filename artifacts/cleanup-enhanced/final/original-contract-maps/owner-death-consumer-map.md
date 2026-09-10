# K3 publication-owner-death consumer migration

Only current B tests/unit/test_publication_owner_death_control.py and this note were edited. No production source, baseline manifest, repository docs, pytest execution or commits were performed in this subtask.

## Fixed provenance and R2.2 scope

Current B and fixed B/E test_publication_owner_death_control.py were byte-identical before migration. SHA256: 7b3662da9b9ea80c1c04698b659aa4eafd4558f628f81364f45e2e3ffa5a7635. Original source remains in both C:/Users/t-hdong/Desktop/gao/audit/two-version-cleanup/{base,enhanced}/tests/unit/test_publication_owner_death_control.py. The additional legacy history file named in that source was not modified.

The fixed file has four test functions / five cases: three unit cases and test_live_background_converges_owner_wide_and_publication_sagas[fence,publication], both loopback_smoke. Its R2.2 retained_contract_groups entry lists the original functions at lines 198, 227, 279 and 314, with coverage_status partial_behavior_reference_not_one_for_one and an explicit instruction to preserve semantics while adapting fixtures.

The R2.2 replacements are only adjacent E evidence:

- tests/unit/test_enhanced_publication_control.py::test_dead_publisher_node_does_not_release_surviving_child_without_actual_ack
- tests/unit/test_enhanced_owner_retirement.py::test_unproven_or_wrong_child_death_never_partially_retires
- tests/unit/test_enhanced_publication_control.py::test_prior_abort_fence_is_not_rebound_by_later_owner_death

None establishes the old full ordered child-ACK/finalize fault sequence, two-owner global publication drain, or real two-domain background convergence.

## Exact four-function map

Paths below are relative to tests/unit. Original lines refer to the fixed archives, not the rewritten file.

| Original function | Preserved common B invariant and current evidence | E / remaining coverage boundary |
| --- | --- | --- |
| test_direct_worker_death_freezes_without_remote_effect_and_replays (198) | Shared death-report atomic metadata admission, exact replay, and no synchronous remote effects move to NEW test_worker_death_report_replays_without_rpc_and_scoped_fence_progress_preserves_other_owner. Actual GCS report_worker_death, WorkerRegistry and OwnerDeathFenceRegistry run; first report/replay yields one immutable pending sweep per owner and no effect callbacks/thread. Node owner cleanup requires the actual installed owner fence in NEW test_child_ack_loss_then_invalid_worker_finalize_preserves_exact_owner_cleanup. Existing test_node_publication_owner_death_finalize.py::test_finalize_fences_late_adoption_without_rewriting_success_history and test_owner_led_publication.py::test_owner_death_releases_both_holds_then_sources_without_reversing_complete retain forward fencing/known Complete. | GCS frozen_owner_workset, owner_cleaned publication registry, global graph snapshot and blanket old ReportTerminal rejection are E-only former mechanisms. Current E must preserve accurate terminal history without reopening forward progress; do not restore OutputPublicationRecoveryAuthority or the old rejection oracle to B. |
| test_exact_child_ack_loss_and_invalid_finalize_ack_keep_cleanup_replayable (227) | NEW test_child_ack_loss_then_invalid_worker_finalize_preserves_exact_owner_cleanup composes current actual Core handoff, completed Node journal, two real child owner tables, installed owner-wide Node fence, first real final-hold release followed by lost ACK, then malformed cleaned=1 Worker ACK. Failed child ACK leaves no adapter cleanup receipt; valid child replay completes all exact final/provisional releases; malformed finalization preserves payload/history, finalization retry does not repeat child effects, terminal replay is effect-free. Existing test_output_owner_death_node.py::test_worker_cleanup_ack_loss_retains_node_payload_and_replays_exact_request and ::test_worker_finalize_reply_is_revalidated_before_node_retirement cover the individual boundary faults; new case preserves their composition with real child release. | Former GCS-to-Node finalization ACK boundary/graph retirement differs from current B Node-to-Worker custody ACK. E still needs its current controller's combined child-release-loss + invalid publisher-finalize ACK scenario; split B evidence does not close that E boundary. Old two slots/eight releases become one output/two children/four logical hold releases. |
| test_explicit_progress_filters_owner_and_global_drain_converges (279) | Shared owner isolation and finite cleanup drain move to NEW test_worker_death_report_replays_without_rpc_and_scoped_fence_progress_preserves_other_owner. Two owners share one registered Node; explicitly driving first owner's actual sweep leaves second's outbox unchanged, exact settled death replay cannot requeue first, current DrainOwnerDeathFences settles second once and terminal replay sends no callbacks. Adjacent current test_common_cleanup_progress.py::test_owner_fence_driver_rotates_past_blocked_target_and_survives_shrink tests fairness under pinned/timeout/incorrect ACK. | Old ProgressPublicationOwnerDeath(owner), DrainPublicationOwnerDeaths and publication active counts are E-only central compositions. New B test claims owner-fence outbox isolation only; it does not claim that B has a global output publication drain or that two-owner publication graph cleanup was reproduced. |
| test_live_background_converges_owner_wide_and_publication_sagas (314), params fence/publication | Shared monotonic retry/outbox/custody safety is preserved synchronously by both new tests, test_common_cleanup_progress.py::test_owner_fence_driver_rotates_past_blocked_target_and_survives_shrink, and test_node_publication_owner_death_finalize.py::test_busy_worker_finalize_does_not_start_an_ordinary_supervisor_rollback. No thread is started and no real scheduling/liveness claim is made. | Both old live cases run a GCS fence + publication dual-domain driver that B no longer owns. Enhanced needs a separately reviewed L1 current-controller replacement with bounded failure injection, gate/wait/join cleanup. The fence parameter also contains shared real owner-fence thread-liveness evidence; this pure migration is NOT an equivalent replacement for that evidence. Record it as an unclaimed separate live-driver gap if preserving that stronger B oracle is required, rather than labeling a pure schedule test as concurrency coverage. |

## Current implementation and resource cost

The rewritten file has a module-level unit marker and exactly two non-parametrized functions:

- test_worker_death_report_replays_without_rpc_and_scoped_fence_progress_preserves_other_owner
- test_child_ack_loss_then_invalid_worker_finalize_preserves_exact_owner_cleanup

First fixture uses object.__new__(GCSLite), real NodeRegistry/WorkerRegistry/OwnerDeathFenceRegistry, one registered Node, two registered workers, MemoryEventSink, real direct membership/progress methods, and exactly two synchronous typed Node sweep ACK callbacks. It does not construct GCS/TCPServer or invoke actor/placement/global publication authorities. ACKs are explicit boundary inputs, not claimed physical deletion evidence. No dead processes are invented or detected by the fixture.

Second fixture imports the current real _fixture(refs=True, stored=False) / _close from test_core_output_publication.py. It creates one threadless Core, one Node reducer, one already completed single INLINE output, two child transfers and the helper's bounded 1 KiB in-memory store (no output replica bytes). The true owner-wide Node fence is installed before cleanup. Five child-release callbacks represent first release with lost ACK plus four exact ordered release ACKs; two typed Worker finalize callbacks represent malformed ACK then valid retry. Four explicit Node finalization calls follow the initial unfenced rejection. No background drain, real worker, socket, process or timing behavior is introduced.

Current Core owner metadata is deliberately retained unchanged by the Node cleanup test: that test proves publisher custody disposal after a death input, not execution of the dead owner or a complete process death fanout. The Complete witness and lease COMPLETED state survive; journal payload retires only after valid finalization; no rollback tombstone is fabricated.

The imported current test_common_cleanup_progress._no_runtime autouse fixture prevents GCS/Core/Node/Worker constructors, process/thread starts and joins, socket/transport operations, timer creation, sleeps and blocking waits. Real local locks/events and in-memory tables remain allowed. _close releases actual owner tokens and uses the existing pure Core teardown; counters/owner state are not erased to fake cleanup.

## Explicit E migration queue

1. Current enhanced Begin/owner death/freeze/fence/history interaction, preserving known Complete and rejecting forward permission without blanket loss of accurate terminal facts.
2. Current enhanced controller ordering: owner-wide sweep complete before publication child cleanup, graph retirement, and publisher finalization.
3. Combined real child Release ACK loss then invalid final publisher ACK; proof replay must skip only successfully validated release obligations and retain terminal bytes obligation until exact ACK.
4. Two-owner explicit publication progress and global publication drain isolation using two single-output publications; graph cleanup and owner_cleaned counters need actual E evidence.
5. Separately reviewed live enhanced driver tests for fence-domain failure and publication-domain failure with bounded failure injection, convergence gates and finally stop/join. No restored base central module is justified by these E goals.

## Validation

Syntax-only ast.parse passed and found exactly two test functions. git diff --check passed (Git reports normal LF-to-CRLF working-tree conversion warning). No pytest, module-import execution, live infrastructure, production edits or commit was performed. Parent owns coordinated acceptance execution and manifest updates.
