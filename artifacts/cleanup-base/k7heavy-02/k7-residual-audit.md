# K7 residual audit

**Result: K7 is not closed.** Source import/export retirement checks are clean, but six retained test files still access retired runtime attributes; registration/disposition gaps remain. This audit did not run pytest, collection or test imports, and did not modify base. The current worktree was changing during review; exact per-file evidence is in the JSON. Enhanced/E remains deferred until K8.

## Blocking items

| ID | Concrete residual | Required closure |
|---|---|---|
| K7-BIND-01 | `tests/unit/test_core_late_replica_cleanup.py:241–242` still uses GCS publications.output_recovery/kept_slots; `:390,392,401,494,497` uses removed targeted execution; `:464` uses publications.commit_node_death. | Preserve old-epoch cleanup/new-attempt/Node death assertions through current owner/Node authorities. Root delegated this file to a separate audit candidate. |
| K7-BIND-02 | `test_actor_result_publication.py:117`, `test_ambiguous_grant_custody.py:224,226`, `test_core_lease_locality.py:259,264`, `test_handoff_push_admission.py:200`, `test_location_handoff_worker_loss.py:116,118` read or pass removed target_execution. | Review/apply the 5-file audit patch and execute the registered originals. Only retired dimensions were removed; all 30 functions/decorators remain. |
| K7-REG-01 | Retained machine151 files `test_reconstruction_runtime.py` and `test_recursive_reconstruction_planning.py` have no current selector. | Register exact reviewed files/current closure, or record an approved retained/deferred disposition. Historical k3j 9/11 passed only certifies that historical candidate. |
| K7-REG-02 | 17 retained functions lack selectors: core_actor_restart 6, core_placement_group_scheduling 3, stored_physical_gc 6, worker_crash_supervisor 2. All currently marked heavy. | Per-case cost review and exact marker/disposition. Do not register whole files as pure or delete by name. Exact functions are in JSON. |
| K7-EVIDENCE-01 | Observation found 310/311 reviewed migration hashes stale during parallel P2 changes; manifest changed during the audit. | Freeze final candidate and refresh reviewed closure evidence before K7 execution. This is a registration-state gap, not a runtime failure count. |

## Completed static and historical checks

- Parsed all 383 current src/tests Python files. Absolute/relative local imports (including namespace packages), explicit __all__ exports and direct self handler bindings have no missing entries. OwnerService's 18 explicit Core method bindings resolve. This does not prove arbitrary attribute dispatch, which is how the six test omissions escaped import-only checks.
- All 19 SRC and 21 API items were mapped; 35 explicit retirement obligations have no remaining definitions/references at their planned source paths. The seven mandatory REP construction/route/GC/admission/shutdown/owner-service items show the intended current structure. Assessment-only P2/P5/P6 decisions are not reclassified as mandatory failures.
- Machine151: 122 files remain and 29 are absent. The latter are 26 planned whole retirements plus 3 explicitly mapped graph/targeted migrations. No removed file has an active current selector. Every one of the 151 actions has a per-file presence, registry and saved-artifact link in the JSON.
- 48 history actions (43 deletion, 5 merge) have removed working copies and indexed archives; the broader base history index has 57 entries. These are archive/knowledge facts, not runtime acceptance.
- The six legacy-attribute files are outside machine151 and have no current registry selection or exact/file record in the 49 saved results.json files scanned. Merely appearing in snapshot.json is not a pass. K2b's 18 cases were actor_reference_rejection/node_resource_terminal/resource_ledger_retirement/single_output_journal_retirement/single_output_submission_rollback. K3c's latecleanup5 was test_late_cleanup_shutdown.py, not test_core_late_replica_cleanup.py.

## Proposed bounded migration

`audit/k7-legacy-target-bindings/candidate.patch` is a 4,114-byte, 5-file patch. SHA256: `35e7c2f3154b323748ca077b6aedea7bc82f33ce6316c5f8721b5df055b3c2a6`. Read-only `git apply --check` passed. It removes only old target_execution assertions/tuple members/extra constructor arguments while retaining complete lease/task/attempt/owner/executor/object/scheduling identities and actual Node arbitration. Base has not been patched by this audit.

All five files are suitable for separate whole-file unit registrations after current closure review: Actor fixture has one threadless Core, <=2 calls and 3-byte values; custody fixtures have two threadless Cores/two 1-KiB stores, <=6 source transfer calls, fixed four lease rounds/twelve lost replies and <=3 explicit replays; locality has <=4 lease sends and <=64-byte dependencies. Runtime tripwires prohibit real process/thread/socket/wait work. The patch adds no operations. Exact 30-function selectors, original decorators and cost bounds are in `audit/k7-legacy-target-bindings/registration-recommendations.json`; no execution or expanded case count is claimed.

Supporting artifacts: `audit/k7-source-closure.json`, `audit/k7-action-evidence.json`, and `audit/k7-residual-audit.json`. Root must record actual runner results on the frozen successor; this report does not certify all retained tests or K7 completion.

## Root follow-up after audit cutoff

Root reported registered execution of the legacy-binding repairs: Actor 13 (after one scalar fixture correction), ambiguous grant 12, locality 12, handoff 3, location worker-loss 4, Core late replica 9, reconstruction runtime 9, and recursive planning 11 passed. These are root-reported results after this audit, not execution by this reviewer. Registry was subsequently refreshed to 320 entries, so the prior 310/311 stale observation remains historical cutoff evidence and is not asserted as the current blocking state.

K7-REG-02 is now assigned for actual bounded migration of all 17 original selectors rather than open-ended deferral. The GC6 candidate is ready at `audit/K7-heavy-migration/stored-physical-gc.patch`: six current supported contracts use a real Node Complete/owner-adoption fixture, actual one/two-Node Drop handlers and synchronous retry observations; no legacy fake successful publication, reference thread, Timer or unbounded Queue.join remains. Two original owner-only unit functions are AST-identical, all eight function names are retained. Root validation is pending. Actor6/PG3 and supervisor2 are separate finite candidates in progress.
