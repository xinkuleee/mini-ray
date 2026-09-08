# Retired GCS publication test sources

The `.py.txt` files preserve the complete pre-cut sources byte for byte. The
three former GCS test modules were moved out of `tests/unit/`: they import the
retired `StoredPublicationControlAdapter` and are no longer pytest inputs.
This is an archive, not a compatibility runtime or proof of equivalent coverage.

SHA-256 was checked before and after each move:

| Archived source | SHA-256 |
|---|---|
| `test_stored_publication_protocol_adapter.py.txt` | `6e4ca714ff6fcd4e0e35cb761d2b9a397890643df2f945b7b736d416ecd4967a` |
| `test_inline_recovery_control.py.txt` | `bf58f973c91dc1f5c7194bc450aba05b655bd096dfe9f850114ee68b1661fc81` |
| `test_stored_node_loss_runtime.py.txt` | `c002f4ac022c587aef149dc81ce1aa15fa50f5747ba06b726501964e003d71bd` |

## Preserved invariants and current test sources

Archived line numbers identify the frozen original, not active selectors.
Current coverage means implemented assertions, not a claim that this migration
subtask ran them. Root verification remains required.

| Historical contracts | Unified counterpart | Mapping boundary |
|---|---|---|
| Protocol adapter lines 145–198: graph identity, prepare/commit/release replay, cycle/conflict rejection | `test_contained_graph_protocol.py`, `test_multi_container_graph_protocol.py`, `test_contained_cycle_policy.py` | Same logical graph and exact container release; live RPCs now require admitted `OutputPublicationID`. Naked legacy manifests are not supported forward requests. |
| Protocol adapter lines 201–376: monotonic reports, exact request/ACK echo, wire revalidation | `test_output_protocol.py::test_recovery_stages_and_queries_echo_exact_metadata_facts`, `::test_recovery_rejects_stage_digest_manifest_and_proof_rebinding`, `::test_prepare_and_recovery_deep_revalidate_tampered_values_on_roundtrip` | INTENT/ARM/Complete and owner proofs replace old manifest/sealed/promoted enums. GCS must not regain result descriptors or payloads. |
| Protocol adapter lines 377–825: byte-free ACK, payload integrity, terminal envelope/lease/executor identity | `test_output_protocol.py` prepare, terminal-boundary and metadata-completion tests; `test_output_publication.py`; `test_output_publication_node.py` | Mixed-tier and targeted manifests replace the old exactly-one non-targeted STORED requirement. Old Open/Seal/Claim stages are not required API. |
| Protocol adapter lines 826–1018: exact owner/death takeover replies and metadata progress | `test_output_recovery.py` owner-vector/death/work tests; `test_output_protocol.py`; `test_output_node_loss_control.py` | Exact work/decision/resolution remain required. Old OWNER_EFFECT/NON_OWNER_PROGRESS enums are retired. Registry tests alone do not prove every service query mutation boundary. |
| INLINE control lines 120–191; STORED runtime lines 326–470: bounded cleanup, graph fences, bad child ACK | `test_output_node_loss_control.py::test_frozen_output_work_cleans_exact_child_vector_and_preserves_kept_graph`, `::test_cleanup_revalidates_child_ack_before_retiring_an_obligation`; `test_contained_graph_protocol.py::test_unified_graph_forward_operations_respect_exact_death_fence` | One output driver replaces two tier-specific sagas. Per-call bounds and pending effects matter, not old phase counts. |
| INLINE control lines 192–452, 678–730: owner custody, abort/adopt/retire replay, normal terminal then death, adoption before terminal outbox | `test_output_recovery.py` terminal/adoption/rollback/slot-collection/freeze tests; `test_output_publication_node.py::test_owner_adoption_can_precede_terminal_outbox_without_reopening_forward_work` | UNKNOWN KEEP requires an exact Complete witness and adapter-verified custody, never recovered GCS bytes. Full GCS graph combinations remain separately listed below. |
| Protocol adapter lines 1033–1064: constructor and handler registration | `test_output_publication_control.py::test_normal_gcs_constructs_only_unified_publication_authorities_and_routes`; `test_contained_graph_protocol.py::test_gcs_generic_routes_and_typed_dispatch_share_one_publication_authority` | New tests reject old registries/handlers instead of preserving their names. |
| Protocol adapter lines 1065–1106; INLINE control lines 561–615: shutdown retains work, exact cleanup reaches clean | `test_output_control_shutdown.py::test_shutdown_fences_new_publications_but_exact_reports_and_cleanup_reach_clean` | Actual `GCSLite.shutdown`, not only `has_active_operations`; real child owner release/tombstones, no network or threads. Written, not run by migration subtask. |
| STORED runtime lines 176–212, 875–895: APPLIED/ALREADY_DEAD and closed-admission freeze | `test_output_control_shutdown.py::test_closed_shutdown_still_commits_and_replays_exact_node_death_under_publication_lock` | Actual membership/freeze callbacks under the same composition lock after unclean shutdown; root reports this case passed. Not a true-thread race or a promise of service after process exit. |
| STORED runtime line 938: interrupted membership/recovery composition must replay every publication | `test_output_control_shutdown.py::test_membership_commit_before_freeze_failure_replays_complete_two_publication_workset` | Two distinct publications on one Node. First handler raises after membership commits but before registry mutation; ALREADY_DEAD replay freezes both. The new all-or-nothing workset replaces old partial saga admission; passed in the root's reviewed selection. |

### Shared child-pin wire

`test_stored_publication_protocol_adapter.py.txt:331` checks deep round-trip
validation of a borrowed source credential in `PrepareStoredContainedPin`.
That shared pin wire still serves the new Node adapter despite its name.
`test_stored_contained_owner_table.py` tests owner semantics, not the complete
wire corruption contract. `test_publication_sources.py` now checks the shared
wire's corrupt-source round trip, old pickle GLOBAL names, same-class aliases
and unchanged fingerprint framing. Do not restore the legacy GCS adapter
merely to keep these assertions active.

## Three audit gaps and migration status

1. **Shutdown/membership composition: migrated to three bounded pure cases.**
   Root reports that the first two `test_output_control_shutdown.py` cases
   passed: actual unclean → exact replay/cleanup → clean and APPLIED/ALREADY_DEAD
   freeze after admission closes. The newly added third case covers the old
   line 938 invariant without recreating partial saga mutation: membership
   commits, a one-shot exception prevents atomic freeze, the handler does not
   acknowledge success, then an exact ALREADY_DEAD report freezes both admitted
   publications. Shutdown stays unclean until both frozen items resolve. This
   third case also passed in the root's three-case run. Full old `begin()`
   layout equivalence is not claimed.

2. **Child-owner death proof: Node path migrated; wider GCS matrix remains.**
   Old INLINE control line 453 and STORED runtime lines 688/764 distinguish exact
   non-EXPECTED death from missing/wrong/alive/EXPECTED membership.
   `test_output_dead_child_cleanup.py` now checks the Node wrapper and adapter
   consumption boundary, including malformed incarnation/watermark, foreign
   and executor owners, and mixed dead/live child cleanup. The broader actual
   GCS WorkerRegistry/progress interleaving matrix remains separate. A Boolean
   dead flag or unreachable RPC is insufficient proof.

3. **Graph/death recovery combinations: added pure coverage; real race pending.**
   Old STORED runtime lines 252/289 cover opposite COMMIT/death orderings; old
   INLINE control line 621 covers already-COMMITTED all-DROP cleanup via RELEASE
   instead of ABORT. The separate new
   `test_output_node_loss_control.py::test_typed_node_loss_routes_preserve_committed_graph_and_owner_witness_choice`
   covers committed all-DROP and ARM-UNKNOWN plus valid owner KEEP witness,
   with actual graph commit/release, typed Get/Decide/Progress, per-call effect
   bounds and post-resolution RELEASE replay/forward fence. It has been read
   for this mapping and passed in the root's focused run. True concurrent graph/death
   atomicity remains a distinct L1 gap. The archived two-thread
   tests lacked full startup-to-finally cleanup despite a unit marker: preserve
   any real race test as separately bounded L1, not a relabeled L0 or a serial
   substitute falsely advertised as equivalent concurrency coverage.

## Separately coordinated owner-death archive

`test_publication_owner_death_control.py.txt` is archived by the owner-death
migration subtask; its same-named active module is rewritten for the unified
domain, rather than removed. Its reported matching before/after SHA-256 is
`ef8359b82dcfcc0b599dbe85116ef26ac81e2b6ab73dc0e45a1e2f64ed1556eb`.
That subtask retains real background progression as isolated L1 selectors,
not pure coverage. Current exact cases are:

- Pure: `test_direct_worker_death_freezes_without_remote_effect_and_replays`;
  `test_exact_child_ack_loss_and_invalid_finalize_ack_keep_cleanup_replayable`;
  `test_explicit_progress_filters_owner_and_global_drain_converges`.
- L1: `test_live_background_converges_owner_wide_and_publication_sagas[fence]`
  and `[publication]`; same selectors, now unified publication state.

Legacy INLINE/STORED node-first arbitration, the former terminal-registry
two-phase ACK shape, and broader cross-Node/EXPECTED-death combinations are not
declared equivalent merely because these new tests exist.

## Scope

The old `test_stored_node_loss_runtime.py` cases all construct the retired
adapter or GCS; none is a directly retainable standalone runtime fixture.
The protocol-adapter module also contains pure coordinator/wire-shape cases,
but they construct retired effect/envelope types and require migration rather
than mere import relocation. Useful invariants belong in the unified
registry/graph/handler or shared child-pin layer. Archived sources, old enum
names, and historical test totals do not demonstrate K0/K1 completion.
