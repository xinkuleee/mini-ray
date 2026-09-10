# P2 final read-only disposition

Checked current `mini-ray-base` after root applied journal/effect/snapshot changes and current P3/P4 work. This audit made no base changes or test runs. Existing candidate execution evidence was read directly. No constant-zero journal/effect parameter or old snapshot tuple API remains.

| Original P2 item | Current B implementation / disposition | Evidence |
| --- | --- | --- |
| Journal one result / retirement | Implemented: `_Record.result` and `_Record.retirement` are Optional fields; no payload/retirement slot dictionaries. Complete, materialized read, rollback and exact adoption/owner-death cleanup use them. | journal lines242/246,416,444,600; accepted scalar trial and subsequent effect/snapshot tests |
| Constant-zero effect dimension | Implemented: effect has stage plus optional transfer index; no slot_index. Prepare/promote child methods take only true child index; materialize/read no slot argument. | journal91 onward; adapter171/174 and compensation; `base-p2effect-01`: pure343/1deselected, journal23,replica22,ownerDeath10,ordinary1,contained1 all exit0 |
| Snapshot singleton tuples | Implemented: `materialized` derives from MATERIALIZE ACK; `result_retained` derives from present result; optional exact retirement is detached. No new mutable authority. | journal216–228 and snapshot; `base-p2snapshot-01`: pure343/1deselected,journal23,retirement3,trace6,ordinary1,contained1 all exit0 |
| Tombstone ordinal and tuple return | Implemented: OutputPublicationTombstone has no slot index; retire_completed returns scalar; PayloadRetired.tombstone is scalar. Exact publication/digest/ObjectID/proof binding and alias isolation remain. | journal185,416, exception; same snapshot retirement/trace and real path evidence |
| Eight adapter maps → one record | Evaluated and rejected for current B: real candidate methods implemented and tested, but introduced O(all retained history) pending scans, record and getter interface/copy/lock cost. Keep current stage-specific outboxes and exact per-effect child ACK map. | `base-p2progress-01`: NodeAdapter35,ownerDeath2,ordinary1,contained1 all passed. Actual trial `audit/p2-after-p1/progress`; cost report and root retain decision |
| Exact intent/ACK and independent lifecycle facts | Retained intentionally: missing ACK still leaves possible effects; lease released differs from report accepted; payload retirement differs from physical Store GC; owner-death cleanup differs from rollback and never rewrites Complete. | existing journal/adapter/Node methods and above fault/retirement paths |

## Remaining code observations

Two local singleton iteration forms remain in Node, neither is an independent state or a missing P2 protocol migration:

1. `node.py:1431–1437` builds the legitimately tuple-shaped GetWorkerLeaseOutcomeReply.descriptors via a comprehension over one envelope.result. It can use a direct conditional while preserving that protocol boundary.
2. `node.py:1505–1553` loops once over manifest.value in owner-death physical cleanup. Replacing it requires converting three continue paths to local guards so all paths still perform the unchanged Worker-finalization check. Early return True would be incorrect.

`optional-node-plumbing.patch` is an actual minimal current-byte trial for these two cleanups, not applied or tested. It adds no function/API, preserves the Worker finalization tail text, exact intent/claim/pin/checksum/store cleanup checks and no-effect behavior. It removes two singleton loops and two physical lines, but deeper guards are a readability tradeoff. AST compile and git apply --check passed. This is optional local cleanup, not a blocker for the four completed representation outcomes. If root adopts it, use the existing owner-finalization/Node-outcome tests and real adopted-owner death path before claiming validation.

Legitimate dimensions remain out of scope: multiple child transfers/final+provisional cleanup, multiple physical replica locations, multiple publications/outbox items, Worker-pool indices, phase-indexed exact effects, and the P1-preserved `TaskReply.results=(envelope.result,)` boundary (`worker.py:1313`). Node descriptors remains tuple-shaped. Owner membership and attempt/replica tombstones are distinct authorities.

The name SLOT_DROP and local variable names such as slot now refer to the existing exact effect stage / a single value, not a selectable output ordinal. Renaming them or exception wording solely for prose is optional, not a reason to expand P2. Tombstone.object_id remains explicitly bound to publication_id.object_id; removing that validated identity field was not a requested P2 ordinal requirement.

## Mandatory remainder

No missing B P2 representation implementation was found. Root still owns final merged-checkout verification and recording the final plan/progress status; existing candidate results must not be represented as a test of an unverified later combined HEAD. E adaptation is explicitly K8 work and not silently satisfied by the B audit. The optional two Node plumbing changes do not require a new feature, scenario or matrix.
