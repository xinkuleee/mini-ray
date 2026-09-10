# P2 isolated implementation trial

Status: both experiments are real method changes in isolated source copies, not diagrams. No runtime test has run here and no final retention decision is claimed. Base/P1 checkouts were not edited. Base HEAD at capture was 42ed62d7231d17d3b7f703670a357c509183e853 with ongoing K4 work; captured normalized hashes are in preimage-sha256.json. The unfinished P1 candidate was not read.

## Two independently applicable patches

- p2-progress-trial.patch: adapter progress consolidation, narrow journal Complete getter, and the sole current direct private-map test adaptation. Full trial files are under candidate/.
- p2-journal-scalar-trial.patch: results/retired_slots dictionaries become one optional result and one optional retirement. Full trial files are under journal-scalar/. It is independent of progress consolidation and adds no interface.

The before/ tree is a preserved source preimage. Whole candidate trees are evidence, not safe overlays onto later P1. Root must apply only the intended patch to a matching fresh isolated freeze, or rebase its method changes after P1. Journal slot-zero DTOs, effect.slot_index and public snapshot tuple views remain unchanged in these trials; they must be adapted to P1 representation when that package completes.

## Progress trial: actual changed methods

NodePublicationProgress (adapter line 56) stores completion_observed, independent lease_converged and terminal_reported flags, exact rollback_report, exact owner_death, owner_cleaned, and a per-publication cleanup_replies map keyed by the entire original OutputPublicationEffect. The existing transient ticket set stays separate. No Complete witness is stored in the progress record.

Actual _converge_lease (265), report_terminal (315), rollback (336), finish_owner_death (401), pending query and fence methods read/write the new record. Complete remains in OutputPublicationJournal. A local completion_witness getter (journal line 472) returns a detached witness without constructing/sorting a full manifest/effect snapshot. There are no wire, Node/Worker protocol, callback signature, or RPC changes. Node and Worker continue using the same adapter methods.

| Fact / obligation | Before | Trial |
| --- | --- | --- |
| Canonical successful Complete | journal + duplicate copies in terminal and lease maps | journal only; completion_observed records outbox admission, never permission to Complete |
| Local lease callback completed | lease_pending → lease_converged transfer | lease_converged remains False on exception, True only after same callback returns |
| Owner received exact Complete | terminal_pending → terminal_reported transfer | terminal_reported becomes True only after the existing exact-ACK callback returns |
| Rollback received by owner | exact rollback tombstone in map | same exact tombstone in rollback_report, same conflict/replay check |
| Owner death fence | immutable death in owner_cleanup_deaths | immutable owner_death; forward ticket rejected once present |
| Child cleanup | one global effect→real reply map | nested full-effect→real reply map; no bool ACK or predicted reply |
| Owner cleanup finished | second immutable death in owner_cleaned map | owner_cleaned boolean, set only after child work, Node/Worker callback and journal retirement |
| Payload retirement / Store GC / phase | journal/Node/owner remain independent | unchanged; owner_cleaned only suppresses pending delivery, not a fake lease/report ACK |

The after-callback report check is retained. If owner cleanup finishes during its RPC, report_terminal returns False and cannot restore pending work/custody. A death fence that has only begun is separate from completed cleanup. Exact child death validation, effect intents, ACK identity, and retry behavior remain unchanged.

Lock ordering is preserved: Complete holds journal then Node then briefly adapter; local lease callback follows existing composition. Standalone pending queries capture candidate IDs while holding adapter, release it, then call the journal getter. No external callback is under adapter/journal lock. Node cleanliness already obtains journal and adapter nonblocking, in that order, before calling these readers; reentrant getter calls do not introduce a reverse blocking acquisition there.

## Measured progress costs

| Metric | Before | Progress trial |
| --- | ---: | ---: |
| Adapter physical lines | 575 | 592 |
| Journal physical lines | 674 | 680 |
| Total source physical lines | 1249 | 1272 |
| Adapter normalized bytes | 31747 | 32249 |
| Journal normalized bytes | 33433 | 33783 |
| Source functions | 74 | 77 |
| Source classes | 19 | 20 |
| Source branch AST nodes | 168 | 170 |
| Source replace call sites | 63 | 53 |
| Top-level progress dictionaries | 8 | 1 |
| Per-record effect dictionaries | 0 (global effect map) | 1 |
| Retained Complete copies in adapter per completed publication | 2 | 0 |

The independent facts have not disappeared; they are now seven fields, including one nested exact-reply dictionary, rather than eight separately indexed maps. A completed ordinary publication creates one slotted record and an empty cleanup dictionary; the original created no per-publication empty cleanup dictionary. Persistent witness/key duplication decreases, but heap-byte savings were not measured and are not asserted.

_converge_lease shrinks from 12 to 10 lines, four touched maps/six map references/seven replace call sites to one progress lookup and one replace. report_terminal shrinks 25→20 lines; finish_owner_death 55→51. These local gains are offset by the new record, two adapter helpers and one journal interface.

The important regression is reading pending work: old pending query visits P pending entries and copies P witnesses; the single-map trial visits H retained historical progress entries, captures P IDs, takes P journal locks and copies P witnesses. For a clean long-running Node, P=0 while H grows, so two formerly empty-dict checks now scan history on every drive/cleanliness call. Using journal.snapshot would be worse because it sorts/clones all effects; the narrow getter avoids that but does not remove the O(H) scan. Restoring pending indexes would reintroduce duplicate membership maintenance.

Preliminary assessment: do not adopt the progress trial as-is solely for eight→one maps. The measured write-path and retained-copy gains do not yet establish a net simplification against added interfaces and history scanning. Root will still run its original ordinary and contained selectors on an independent candidate before recording an evidence-backed keep/implement decision.

## Separate journal scalar experiment

The scalar journal trial changes actual complete, materialize ACK/replay, rollback ACK, retirement, owner-death retirement, snapshot and materialized_result methods. _Record.results becomes result: Optional[ResultDescriptor]; _Record.retired_slots becomes retirement: Optional[OutputPublicationSlotTombstone]. Validated manifest already requires exactly one slot, and effect validators still reject nonzero/invalid indices. No dual internal representations are retained.

Journal Complete, rollback, adoption_proof, owner_death, intended effects, full ACK table and real payload copy boundaries remain separate. The adoption receipt cannot replace a conflicting previous retirement; repeated Complete after payload retirement still raises the existing typed exception carrying the same singleton tombstone. Snapshot.retained_result_slots/retired_slots stay derived singleton boundary views only, pending P1 migration.

Measured journal result: 674→667 physical lines, 33433→32708 normalized bytes, 93→88 branch AST nodes, same 50 methods/16 classes/31 replace sites. Two per-publication dictionaries become two optional fields, indexed get/pop/clear/sort and all-slot retirement loop disappear. No new method, protocol or test fixture is required. This is a smaller promising independent change, not excused by the progress trial result. Runtime acceptance and P1 rebase remain pending.

## Exact validation handoff

validation-selectors.json lists existing scoped pure cases and serial real-process scenarios. Minimum real pair: ordinary test_task_path::test_one_node_one_worker_task_path and contained test_stored_outer_publication_path::test_stored_outer_publication_adopts_owner_handoff_and_collects. Retain the existing runner 30-second process-tree bound; do not collect the whole old suite.

The only direct old private-map test use found is test_publication_owner_death_control. Candidate adaptation preserves zero recorded ACKs after the first lost reply, then four exact accepted child replies before invalid Worker finalize. No other fixture signature or source test import is changed. Freeze its complete existing helper closure with the runner; source copies alone are not an acceptance environment.

Static verification performed here: changed files parse and compile to code objects without executing imports; old map names are absent from the progress candidate; old result dictionary references are absent from the scalar journal; diff whitespace checks; SHA comparison confirms changed source/test preimages still matched B at analysis time. No timing, memory benchmark, pytest, manifest edits, commit or base/P1 mutation occurred.
