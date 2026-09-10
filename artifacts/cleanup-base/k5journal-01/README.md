# P2 candidates rebased on frozen P1 trial02

Both independent candidates are built from the exact 419-file snapshot of `base-p1trial-02`. Every input raw SHA256 matched before and after generation. Origin archive SHA256 is `332d78b7dcedc67da3c1db3b384f5ee915b69b4516c411e4ac25a196f2024f73`; origin snapshot SHA256 is `081db0bbdd3796420e8b888714a4dc62dc688ce184326c6237ac8b298f5384d6`.

`journal-scalar/` and `progress/` are separate 419-file executable candidate roots. Their manifests, all unchanged tests, and all unaffected P1 source bytes are identical to the frozen trial02. No base or P1 file was edited. The build script applies exact expected hunks/method bodies only and explicitly preserves the P1 `manifest.value`, `payload: bytes`, `envelope.result`, and derived `publication_id.object_id` interfaces; it never overlays old whole source files.

Each candidate has an independent patch and complete raw-hash inventory. `summary.json` records changed paths, before/after hashes and static metrics. `p1-trial02-raw-sha256.json` preserves the baseline inventory. No pytest, manifest mutation or commit occurred here. Root must freeze each independent candidate using the existing runner path and refresh reviewed hashes normally before running it.

## 1. Journal result and retirement

`journal-scalar.patch` changes only `src/miniray/output_publication_journal.py`. `_Record.results` and `_Record.retired_slots` become optional `result` and `retirement`, with actual Complete, materialization/replay, rollback, owner-death cleanup, retirement, data read and snapshot methods converted. The envelope receives `record.result` directly using P1 format.

Complete remains its own exact journal fact. Intent/ACK tables, rollback history, adoption proof and owner-death fact stay independent. `materialized_slots` still derives from the materialization ACK, while retained result presence derives from `result`; a retired payload can still have an acknowledged materialization. Exact retirement replay and conflicts, detached snapshot/result returns and owner-death cleanup without fabricated adoption/rollback are preserved.

Static costs: 673→666 physical lines; 33055→32438 normalized bytes; 89→86 branch AST nodes; 50 functions and 31 replace sites unchanged. Two internal per-record dictionaries disappear. No new interface or fixture change is required. This independent trial is promising, pending real ordinary/contained and journal-negative validation.

## 2. Eight progress maps to one record

`progress.patch` changes the adapter and adds a narrow journal `completion_witness` read method. It changes only one test file to read the new exact child-reply map, preserving the original zero-then-four-ACK assertions and adding explicit identity checks.

`NodePublicationProgress` holds separate lease-converged, terminal-reported, rollback-receipt, exact owner-death, cleanup-finished, and exact per-effect reply facts. No Complete object is stored there; `completion_observed` records only that adapter convergence saw the journal fact. `_converge_lease`, `report_terminal`, rollback, owner-death cleanup and pending readers all use the record. The transient ticket set remains separate.

P1-aware hunk resolution is explicit: `finish_owner_death` keeps its `manifest.value.transfers` loop and exact effect identity. Preparation keeps the P1 scalar payload and value; materialization/compensation retain P1 object-ID derivation. All old map attributes disappear.

No business protocol or callback changes occur. Canonical Complete is journal-only; lease and terminal reporting remain independent. Owner-death cleanup suppresses pending reads without pretending an ACK happened. The after-RPC terminal check still prevents finished owner cleanup from being revived. Readers capture identities under adapter lock and release it before acquiring journal; Node cleanliness retains its existing nonblocking journal→adapter lock sequence.

Static costs on P1: adapter 577→594 lines, journal 673→679 lines, total +23 source lines; +3 functions; replace call sites 63→53. Two retained adapter Complete copies per completed publication disappear. However, pending queries change from O(P) pending-entry iteration to O(H) retained-history scanning plus P journal reads. A clean long-running Node with P=0 still scans H progress records. The narrow getter avoids cloning/sorting full journal snapshots but adds an interface; each ordinary progress record also allocates an empty exact-reply dictionary.

Therefore this trial remains an experiment for evidence-backed retention, not a recommendation to merge merely because eight maps became one. Root should run existing ordinary and contained cases on this candidate before recording the final implement/retain decision.

## Effect slot-index boundary

This rebase intentionally keeps `OutputPublicationEffect.slot_index` and current public slot-zero projections. It is not universally a zero: OWNER_REGISTER requires None; materialization/drop/child stages require exactly zero. Removing it changes the positional/pickle operation key inside ACKs and rollback tombstones. It does not need a new handler, but it is a coordinated wire representation change beyond the internal two-dictionary trial.

At least four source files would require migration: journal effect/dataclass/order/signatures, adapter prepare/compensate/child cleanup, Node write-claim and canonical replica-effect validation, and Core rollback-report validation. At least seven direct tests require rewritten constructors/negative assertions: journal, output_handoff, output_replica_node, owner_finalize_replica_receipts, worker_stored_publication, cross_cleanup_receipt and output_child_owner_worker_loss. Other helper-signature consumers also need review. Owner membership slot indices and Worker-pool slot indices are separate concepts and must not be mechanically removed.

P2 follow-up boundary: if pursuing effect-field removal, replace ordinal corruption tests with stage-specific illegal child indices while retaining wrong publication/attempt/digest/owner/checksum and nested-ID checks. Preserve child_index for multiple references. Snapshot tuple projections, retirement singleton return and materialized_result(id,0) can be removed only in that coordinated follow-up. They are retained here as explicit existing interfaces, not dual internal authorities.

## Validation handoff

`validation-selectors.json` lists existing selectors, not a second registry. Minimum real pair for each independent candidate is ordinary `test_task_path::test_one_node_one_worker_task_path` and contained `test_stored_outer_publication_path::test_stored_outer_publication_adopts_owner_handoff_and_collects`. Use the existing 30-second bounded process-tree runner.

Journal trial requires its existing single-output journal/retirement negative tests. Progress trial additionally requires the real owner-report race, local lease completion fault, and zero/four child ACK before Worker-finalize tests. No test format was bulk rewritten and no P1 test closure was reverted. Runtime acceptance is pending; static AST/compile and both `git apply --check` checks passed.
