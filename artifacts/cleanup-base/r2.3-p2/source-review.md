# R23-01 B test candidate source review

The candidate changes only `tests/unit/test_owner_finalize_replica_receipts.py`. No shared helper, runtime, manifest, P3 cleanup, or formal repository file was changed by this agent. The parent applies the candidate and owns registry updates and all bounded execution. This review used source text only: no pytest, collection, or project import.

## Frozen bytes

| Input/output | SHA256 | Bytes / format |
|---|---|---|
| Original B test at `ab4cfb317fd786a286359d8f4e971ff195739375` | `052874c16ff36f455e1827f1fffd60810fa3001f47eab60ca344b5ab77fb2b50` | 20,693; UTF-8 without BOM, CRLF |
| Candidate `tests/unit/test_owner_finalize_replica_receipts.py` | `0125d9ec9101b4217d09e62ac02521fd465a85a21ffa164df9ffbde7f2ea6724` | 28,682; UTF-8 without BOM, CRLF |
| `test-repair.patch` | `637a734e2855c293d2058c28d7a2f2e6eba71b2910631f3589bd08ac02be3991` | Unified Git diff, LF patch framing |

`source-original.txt` reconstructs the original Git blob in its observed CRLF working-copy representation and matches the original SHA256 above. It was saved after the parent applied the candidate so the final patch still compares against the exact original input.

## Source facts supporting the assertions

- `output_publication_node.py:192–208`: MATERIALIZE intent precedes storage callback; the result descriptor ACK follows its successful return. `output_publication_journal.py:497–505` assigns the result only with the ACK. All existing STORED fault cuts therefore have no result/ACK, while their physical obligations remain.
- `node.py:1681–1764`: the actual seal path records an exact write claim before create/write/seal. The candidate observes `claim.expected_metadata`, full allocation size, and actual untouched/partial buffers instead of assuming a retained result proves bytes. The original failure hooks still execute real create/write/seal first where applicable.
- `node.py:1476–1579`: owner-death finalization releases the incomplete lease as ABANDONED, skips storage for INLINE, retains physical/manager obligations until their exact receipt, and only then calls the alive Worker. The fake Worker callback now observes these conditions at the boundary and still checks exact endpoint/request, lock-free invocation, and at most two calls.
- `node.py:3526–3565`: a dropped watermark precedes manager acknowledgement and the final receipt. Both before-effect and after-effect manager failures retain their original assertions; neither is equated with complete cleanup.
- `output_publication_journal.py:426–449`: owner-death retirement clears the descriptor only after the adapter's actual cleanup callback succeeds. New common pending and retired observations forbid both a rollback plan and a rollback tombstone.
- The independent INLINE test calls `_node(refs=False, stored=False)` and the actual Prepare handler. It checks the accepted request identity, real handoff registration, MATERIALIZE ACK, exact detached INLINE descriptor, and no Complete. The descriptor stays exact before both fake Worker ACK attempts; first ACK loss retains it and the retry clears it only after confirmation. A delegating ledger.release observer records the one actual release invocation and unchanged final ledger snapshot.
- The INLINE guard prohibits create/write/seal/delete/abort, adapter materialization/drop callbacks, and manager deletion. It permits Generic Drop's legitimate read-only `contains` call (`node.py:4405–4431`), which must reject without a physical receipt.

## Preserved boundaries

Eight functions / fifteen parameter instances are a static expected count, not a claim of passing tests. The fake typed Worker reply remains an explicit external unit boundary, not a real Worker process result. The parent separately runs the existing adjacent Node test and actual owner-death process selector.

Independent static review verified all referenced fields/signatures and original parameter branches. Its two concrete suggestions were applied before freezing: inspect exact retained INLINE payload immediately before both Worker ACKs, and reject a pending rollback plan as well as a completed rollback tombstone. No remaining static finding was reported.

## Parent execution feedback: after-01

The parent's actual bounded run (`audit/two-version-cleanup/execution/r23-base-after-01/000-case.log`) reported fourteen passing cases and one failing `claim` case. Failure occurred while the retained original test tried to deepcopy its deliberately malformed effect, before the Node finalization call. `_WireValue.__reduce__` reconstructs `OutputPublicationEffect`, whose `__post_init__` correctly rejects MATERIALIZE with a transfer index. This is a test observation setup failure, not evidence of a runtime cleanup failure.

The independently reviewed minimal follow-up clones the valid complete claim map before injecting corruption, then sets the same invalid field in both independent expected and actual effect objects. It preserves the full post-call map equality, physical snapshot, no-Worker-call and no-receipt checks, while allowing the Node to see the same malformed claim. The parent owns that formal follow-up and its next exact input/hash/run. The candidate and patch in this directory remain the original frozen after-01 input, not an updated or passing artifact.
