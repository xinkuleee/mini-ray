# Independent disposition assessment for the single StageAck candidate

**Recommendation: adopt this candidate, subject to the already-planned final enhanced gate, affected migration cases and frozen install/import checks.** The finite design experiment now supports that choice. This is not a claim that final acceptance is already complete, that E became shorter, or that measured byte/copy reductions are network/elapsed-time speedups. No second candidate or source rewrite is recommended.

This assessment independently weighs the actual implementation and measurement records against the cleanup teaching objective. The reviewer also implemented the candidate source earlier, so it is not an external clean-room code review; the earlier seven-file copy-layer assessment did use a separate read-only subagent. No source was modified, and no tests or runtime imports were performed for this disposition assessment.

Inputs: frozen source patch `1da561e41b36764f29bb6de23794f4525f199665bfa35c0c56f4ed29664c8574`; `structure-cost.json/md`; actual `measurements.json` SHA256 at review `a0b986e7148ceff0ecc04a8d0a7f2ce67c6f47e39b84e452fe24135a65a62a4d`; retained `measurement-differences.json`; root reported passing four enhanced files (37/5/15/4 cases) and eight new ACK-boundary cases. Final full-gate results remain root acceptance evidence.

## Why the teaching responsibility is clearer

The candidate makes three already-required concepts visible at the interface: a mutation acknowledges one exact stage and its actual accepted fact; a query retrieves complete historical knowledge; current permission to move forward derives from closing evidence and still requires the local participant post-RPC check. The old interface returned the whole history for every successful mutation, allowing the consumer to read broadly even when it needed only preparation, Complete or one receipt. The new return type directly exposes what that operation proves.

That separation is educationally useful here because E specifically teaches a surviving publication fact source and global contained-edge admission. The essential distinction is that accepted ARM, Node Complete, GCS TERMINAL, graph COMMITTED, owner READY, ADOPTED and RETIRED are not one success flag. `accepted_fact` plus a derived `forward_open` expresses that distinction without moving GCS history into a Node/Core cache. Late TERMINAL/ADOPTED and first-fence preservation remain explicit rather than being hidden by returning a generic success boolean.

This clarity is grounded in the actual consumers: journal ARM compares the accepted TaskPreparedReceipt with real local child/materialization evidence; the terminal-loss gate reads the actual accepted Complete at the reply interception point; client Task/put commit compares the relevant accepted fact and then checks forward; Node/client compare authority owner context with their already-known publication. The graph/history authority stays inside the same PublicationAuthority, and Core scheduling/owner/recovery algorithms are untouched. A student tracing the operation still follows the same participants and commit points.

The new object is therefore a wire projection, not another state machine or pluggable publication backend. GetPublication remains the explicit way to ask what survived an unknown reply or Node loss. Preserving full query/failure snapshots also avoids inventing a second recovery cache to compensate for the smaller successful response.

## The real additional burden

The source cost is meaningful: **+204 physical / +181 code-bearing lines**, **one new class/eight stored wire fields**, and **+62 `if` statements**, mostly in a 93-line strict constructor and 36-line projection factory. Client `call` expands from 13 to 36 lines and must understand two response families, known owner binding and explicit accepted facts. This is a second, narrower cross-field validation model beside the retained full-snapshot validator, and future wire changes must keep both models consistent. The eight-way fact union is not as immediately obvious as a single full snapshot.

The candidate does not make all duplication disappear. It still echoes the whole request and separately rebuilds the accepted preparation/Complete/adoption/closed-hold fact. A normal fence can appear in request, fact and closing context. Closed historical replies retain a real fence proof and closing receipts, and client owner binding adds a known-publication copy. The controller pre-mutation full query, canonical snapshot rebuild and failure/query copies remain. It would be inaccurate to describe the implementation as simply deleting redundant copies.

These costs are acceptable for this particular experiment because they are localized at a concrete protocol boundary and replace work repeated on every ordinary successful stage and receiving boundary. They introduce no new retained authority, new RPC/query, new synchronization point, new failure model or new user-facing capability. Node journal/adapter changes are small; Core, publication_gate and output_protocol need no redesign. The full authority reducer remains readable as one record and the same stage transitions.

This is a tradeoff in favor of making the contract at the boundary explicit and reducing repeated payload reconstruction, not a reduction in total learning material. The public cleanup description should acknowledge the added validation code and avoid claiming that E has become a smaller or universally simpler runtime.

## Actual cost evidence supports the localized tradeoff

The measurements exercise actual controlled Task/Node/client/owner lifecycle callbacks and use the project serializer plus business RPC framing, not isolated DTO lengths. They exclude trace sidecars and do not measure real network latency or receive-side deserialization. Measurement serialization and test-only observations are excluded from local copy-call profiling. These limits are declared in the measurement index.

| Complete measured slice | GCS exchange frame bytes B → C | Reduction | Local copy/validation calls B → C | Reduction |
|---|---:|---:|---:|---:|
| Zero-child Task | 37,131 → 25,798 | 30.5% | 21,495 → 15,602 | 27.4% |
| Owned/borrowed contained Task | 149,225 → 100,718 | 32.5% | 66,733 → 50,631 | 24.1% |
| Contained put | 113,114 → 91,700 | 18.9% | 37,000 → 31,234 | 15.6% |
| Whole replacement | 181,290 → 115,534 | 36.3% | 79,056 → 56,708 | 28.3% |

The decision does not rely on a minimum percentage. The relevant observations are that every selected lifecycle benefits after counting actual residual fact/closing/owner-copy work, including the contained put path where saving is smallest; request sizes and query/callback counts do not increase; and there is no compensating extra round trip or new recovery store. Repetition produced the same cost numbers. This demonstrates that the added validation machinery did not merely move equivalent full-history cost into another layer.

The plan explicitly permits source growth when an actual vertical slice demonstrates net benefit without changing contracts. Retaining the original implementation only because this candidate has more lines would substitute a line-count target for that rule. Conversely, if the added client checks or fact reassembly had erased the lifecycle reduction, or required extra queries/new obligations, the same source growth would have supported retaining the original design. The actual result does not show those drawbacks.

## Exact-history calibration was handled honestly

The initial 24 runs all completed their lifecycle assertions, but **`task_mixed-r2` was not byte-for-byte equal in requests/checkpoints**. Its retained raw comparison still says false. `measurement-differences.json` shows the two child release calls and corresponding first RetireGraph proof tuple in opposite order. Core still iterates `tuple(obligation.pending_edges)` at `core.py:5055`; Core source is unchanged, and the initial runs did not freeze Python hash seed. Different valid first proof order must not be sorted away, because exact retirement replay preserves whichever first order was accepted.

The additional six runs with `PYTHONHASHSEED=0` repeated the same mixed slice three times in both variants. All three seed-controlled pairings have identical original-order requests, checkpoints and callback counts, with the same measured costs as before. They appear alongside the original false result, not in place of it. This supplies the same-environment comparison that was missing; it does not retroactively relabel the old mismatch as exact. No source adjustment or relaxed proof equality was needed.

## Disposition boundary

The experiment supplies an evidence-backed reason to proceed with this one implementation rather than retain the old full-success reply. The recommendation applies only to this frozen candidate, its exact strict validation, current consumers and the scoped lifecycle evidence. It does not authorize a broader decoder framework, compression of the request echo, adjacent RPC merging, internal-copy removal, or another candidate search.

Before formal adoption is marked complete, root still needs the plan specified same-input enhanced pure/smoke gate, affected non-gate cases and locked install/import checks for the changed package. The evidence index should retain the original mismatch plus hash-seed calibration, source growth and measurement limits. If those remaining checks reveal an actual contract or wiring failure, fix that failure within this candidate or make an evidence-backed retain decision; do not treat this design-value recommendation as a passing label.
