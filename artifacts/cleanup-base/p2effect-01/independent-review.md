# P2 effect scalar: independent source review

Result: no correctness regression found in the bounded four-source delta against `audit/p2-after-p1/journal-scalar`. This review is source/AST evidence only; no candidate source was changed, no test was imported or run, and no pytest acceptance is claimed. Tests were still being migrated by the author.

## Correctness checks

- `candidate/src/miniray/output_publication_journal.py:91` removes only `OutputPublicationEffect.slot_index`. Its constructor still requires the concrete publication/stage types, deep-copies publication identity, validates the digest, applies `_uint` to every child-stage transfer index, and rejects non-None transfer indexes on OWNER_REGISTER/MATERIALIZE/SLOT_DROP (`:99–107`). `_uint` still rejects bool, float, None, negative, and values outside uint64. `_effect` then rejects a valid integer beyond `len(manifest.value.transfers)` (`:529–536`), before intent insertion in `_begin` (`:478`). ACKs rebuild nested effects and recheck the exact stage, manifest and previously recorded intent (`:487`). The removed zero-only outer index is derived by the publication identity, not a missing child bound.
- `core.py:4668` still reconstructs `ReportOutputHandoffRollback` before reading its plan. The request reconstructs the tombstone, its plan and every effect, so the remaining transfer upper-bound check (`:4686`) operates after exact uint validation. Manifest/publication digest, owner, registered-manifest equality, absence of Complete/adoption, no effects for an unregistered handoff, and frozen receipt replay remain checked. Removing the constant outer-index guard adds no new owner authority.
- `node.py:205` keeps write-claim matching tied to MATERIALIZE, no child transfer index, canonical ObjectID, exact attempt, owner and checksum. `_validate_output_replica_effect` (`:1633`) deep-reconstructs the effect, requires MATERIALIZE or SLOT_DROP, checks Node incarnation and canonical stage/digest/publication identity, then requires materialization intent and ACTIVE sealing or the exact next rollback/drop-ACK replay. An injected transfer index on a physical effect fails before store mutation.
- Node physical seal/drop bodies are AST-identical to the comparison baseline: descriptor/tier/size/checksum checks; owner-death and deletion-epoch fences; exact write claims; unknown partial-replica protection; pin checks; metadata/manager cleanup; and lock order remain unchanged. Complete/lease commit bodies are also AST-identical. The delivery checkpoint only removes the now-unnecessary `0` argument to `materialized_result`.
- Adapter prepare/promote obtains a journal effect before indexing a child (`output_publication_node.py:179`, `:210`). The journal has already enforced exact integer and range checks. Public rollback obtains its effect from `next_rollback_effect`; owner-death cleanup constructs child effects from a bounded range (`:409`). These paths still reconstruct expected child metadata after callbacks and require exact request/hold replies (`:465`), or independently validated child-owner death. Removing `_value(manifest, slot_index)` removes only its constant-zero validation. It did not previously validate transfer_index.
- Local descriptor construction still derives ObjectID from publication identity. Journal descriptor validation (`output_publication_journal.py:577`) retains all identity/tier/size/checksum/owner/Node checks; only its impossible alternate outer index is gone. Adapter drop reply validation (`output_publication_node.py:552`) retains object, producer attempt, owner, Node, checksum and accepted-status checks.
- Rollback order is unchanged (`output_publication_journal.py:359`): possible physical materialization drops first, then possible final child holds in reverse transfer order, then provisional holds in reverse transfer order. Intent, not ACK presence, determines compensation. `ack_rollback` remains AST-identical, enforcing the exact next effect and ordered full tombstone receipts. Successful Complete still forbids rollback. Complete/witness creation, retirement proof validation, adapter lease convergence and independent terminal reporting are AST-identical.

## Net structure

The delta removes one redundant effect field, `_SLOT_STAGES`, the zero-only adapter `_value` helper, constant-zero call plumbing, and pair-shaped transfer iteration. Read-normalized source lines fall by 33 across the four files (Core -2, Node -4, journal -19, adapter -8). No new phase map, lock, RPC, or authority is introduced. The retirement tombstone/snapshot singleton fields remain deliberately untouched. A small structural remnant remains: Node owner-death cleanup still iterates `(manifest.value,)`, and some comments still say slot zero; these are not correctness blockers.

## Exact reviewed candidate hashes

| File | SHA256 |
|---|---|
| core.py | 8a2139607414a4b29e8e015c175d47389069d873538e1df3644b57eb94b44e51 |
| node.py | 146ce67603a25b1aa7f9bf320ae4b9dd0eb941807cae339fd48cdd8a5da853c7 |
| output_publication_journal.py | ffd14c324c3bd76ef524178ee61763f8c2da8978eb13ae332c9a417678367f84 |
| output_publication_node.py | d4d4347ccaac010195bfea9fcdd8a0606fe854ad3e833857e32a5833a5e39ce0 |

The comparison used parsed source to ignore existing CRLF serialization noise. The review does not establish test-callsite migration completeness or runtime behavior. No new matrix was created.
