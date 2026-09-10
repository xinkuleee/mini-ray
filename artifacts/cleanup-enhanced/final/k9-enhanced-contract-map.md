# K9 enhanced contract closure map

**All seven original R2.2 groups are dispositioned, and their retained reachable current contracts have bounded validation evidence. No current contract remains unresolved in this map.**

This does not claim that all reviewed registry selectors ran or that every historical scene survived verbatim. The JSON binds current functions to exact snapshots/logs and preserves failed attempts plus explicit non-equivalence rationale.

## Verified evidence

- Graph/proof reducer: **37 passed**, enhanced-contracts-01.
- Lifecycle/control schedules: **13 passed**, enhanced-contracts-02; final controller ACK negatives included in **15 passed**, enhanced-contracts-05.
- Core loss **11 passed**, PG loss **5 passed**, owner receipt loss **6 passed**, enhanced-contracts-03.
- E trace matcher **58 passed**, enhanced-contracts-04; prior57/1 failed run remains recorded.
- Adopted owner-death live path **1 passed**, enhanced-contracts-03; precomplete owner-death live path plus pure/37-smoke gate passed enhanced-final-01.

## E-CENTRAL-LIFECYCLE

Status: `closed_current_lifecycle_and_first_admission_contracts`

- Current first PID/epoch zero-history negatives now passed in control13. Old adapter/route class names retired; current actual facade retained.

Do not infer completion from index pending label or four pure file selection alone.

## E-GLOBAL-CYCLES

Status: `closed_current_supported_cycle_contracts`

- All three identified exact gaps now passed in graph37; current real cycle smokes separately passed final-01. No old multi-target guarantee restored.

Root/testing_audit prepared these new bounded cases; actual cycle integration three smokes passed final-01, no historical-only acceptance claim.

## E-DEATH-SHUTDOWN

Status: `closed_current_death_shutdown_controller_ACK_and_background_liveness_contracts`

- Current control shutdown/death/ALREADY_DEAD two-publication schedules passed control13 and then complete control15. Current background liveness uses actual precomplete and adopted owner-death process runs, separately from finite fault/reentry safety.
- Exact GCS-controller malformed Node Finalize ACK boundary passed both wrong-request and invalid-closed-hold cases in contracts05. Worker-to-Node adjacent negatives are retained but not mislabeled as this boundary.

Source controller may retire graph before owner-wide physical sweep but does not mark finished until sweep completes; record architectural boundary, not silent strict-order equivalence. Original dual-saga background mechanism is retired. Its current liveness projection is supported by the real precomplete owner-death smoke (GCS + 2 Nodes/Workers, SIGKILL owner, read-only fence observation, actual child release and Worker Finalize, bytes absent, clean nonforced shutdown), combined with finite fault/ACK-reentry safety tests. The smoke does not inject the old two per-saga background fault parameters; this difference is explicit, not equivalence-by-name. Additionally contracts-03 executed adopted-output-owner-death exact live selector successfully; this checks real GCS RETIRED and child/byte cleanup after an already adopted result.

## E-REENTRANT-CLEANUP

Status: `closed_current_control_reentry_and_ticket__old_dead_owner_Core_callback_shape_retired`

- Current real control reentry and lost-ACK finally-ticket case passed control13; common two-lane actual adoption ACK/Node-loss case passed K8.
- Old owner-death interruption of a still-running logical-owner Core is not a real surviving-process schedule: actual owner death stops that Core; old separate GCS Node-loss saga no longer exists. No new source bug inferred from continuing a killed process in a mock.

Current GCS controller reentry/ticket fault passed enhanced-contracts-02. Common live-owner adoption ACK / Node-loss takeover passed actual enhanced K8 two-lane case. Exact C7-specific error-after-finish is not claimed as the same scene: current shared obsolete-lane entry/exit fence is exercised with actual enhanced source at the Node ACK boundary, while C7 ACK-loss safety is exercised by actual enhanced owner-client replay. This is a documented semantic decomposition, not restoration of obsolete dual GCS worksets.

## E-CUSTODY-RECOVERY-PG

Status: `closed_current_custody_and_PG_lost_ACK_contracts`

- Both original fixture closures are now actual E journal/authority paths and passed: Core11, PG5. Legal preterminal/query-window/final RetireGraph ACK schedules covered.
- Actual local C3 plus central ABSENT is causally impossible in current single surviving in-memory authority: Node requires C0/C1/ARM before C3. This old shape is explicitly retired without fabricated response.

Keeps old 9 Core cases and 4 PG cases and their names/decorators; no new matrix.

## E-EXACT-PROOFS

Status: `closed_current_exact_hold_set_contracts`

- Added duplicate/tamper/permutation current-boundary cases passed graph37. Initial proof-set permutations do not imply changed terminal tuple replay acceptance.

No old ordered graph-vector oracle restored; real installed child death/epoch/lineage are inherited common boundaries.

## E-TRACE

Status: `closed_current_E_matcher_negatives_and_actual_observation_contracts`

- Trace matcher58 passed contracts04 after helper-only C7 cause fix; E golden/runtime unchanged. Actual observation callbacks passed K8, actual traces/smokes are separate runtime evidence.
- Synthetic matcher input is not relabeled producer/runtime execution.

E schema has actual INTENT/PREPARED/ARMED/TERMINAL/COMMITTED/ADOPTED stages, not normalized B stages.

## Explicit representation limits

- Current Node C3 requires central C0/C1/ARM. The old actual-C3-plus-central-ABSENT shape is impossible without corrupting the authority; legal preterminal history with actual Complete is tested.
- Old separate GCS Node-loss and owner-death sagas are retired. Current Node-loss runs in the logical owner process, which stops on actual death. The surviving current GCS controller has its own validated death-cleanup and reentry contract.
- Fault safety and liveness use separate evidence: finite real-reducer ACK-loss/reentry checks plus actual process owner-death cleanup runs. They are not claimed to recreate old thread schedules exactly.
- Initial proof sets permit permutations; accepted terminal replay still binds the complete historical receipt.
- Synthetic matcher negatives are not runtime execution; actual observation and process runs are separate evidence.

## Final controller evidence

`enhanced-contracts-05/000-case.log`: 15 passed in1.55s; runner3.175s, exit0. Includes wrong-request and invalid-closed-hold corruption after actual Node/Worker/child cleanup effects. Formal test SHA: `6979da11e01408bf29fea92aa5c84f620c4efec2426f286fd7b6a3eebc55f52c`.

No formal/source files were changed by this mapping; only audit artifacts were written.
