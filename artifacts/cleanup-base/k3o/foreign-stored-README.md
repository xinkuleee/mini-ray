# K3 foreign stored dependency migration freeze

Candidate: `mini-ray-base/tests/unit/test_foreign_stored_task_dependencies.py`.
SHA-256: `d98b0a4f43c9c5f794f110045d5ff8ec962de092e0c9d7d3bc4e8e7b77507110`; 92,325 bytes.
Observed source HEAD: `56ab9cc4843e0fc4c6856a54301d3d3d09e93286`.

The candidate retains all 19 original function names as 19 unparametrized
`unit` cases. `mapping.json` records every original selector/line, current
selector/line, retained contract, helper hashes, and finite cost class. No
retained selector was removed and no obsolete interface was restored.

This agent ran AST parsing and `git diff --check` only. No test module import,
pytest, manual test execution, source edit, shared helper edit, manifest edit,
or Git mutation occurred. Runtime results must be supplied by the root's
reviewed, exact-selector Linux runner; this file is not a passing-results claim.

## Changes and actual authorities

- Removed `ReferenceExportSession`, `_put_serialized`, old output helper import,
  and ordinary central publication callbacks. Canonical stored input creation
  now uses `CoreWorker.put(value)` with `inline_threshold=0` and real Node Seal.
- Bootstrap borrowing installs a complete typed `ContainedReferenceHold` in
  the real owner reducer, invokes actual owner Acquire through the consumer
  restore path, and releases that exact temporary hold through the owner
  Release handler. Normal submission then installs the actual foreign Retain
  and lineage record. Closing the borrowed input releases only its borrower
  token, leaving the task hold until final output collection.
- Success uses single-output discovery, real Node Prepare and Complete, exact
  owner handoff Register/Complete callbacks, Core adoption, and the real Node
  adoption ACK/journal retirement. No GCS ordinary-publication backend exists.
- The formerly synthetic dead-owner cancellation now uses the existing
  canonical two-owner fixture. A real Node lease localizes both inputs; both
  report ACKs can be lost after owner mutation. A separately registered Worker
  death is consumed from GCS before replay. Real Cancel precedes the healthy
  owner's unresolved report; the dead owner is not contacted. Actual output
  GC releases the healthy hold, and exactly two GCS owner-death fences delete
  the dead owner's physical replicas without mutating its retained Core image.
- The consumer retry case now uses an actual borrowed Python handle and normal
  submission. Retry changes only the consumer attempt; retained hold, guard,
  RefArg, and producer descriptor epoch stay fixed. Actual GC releases lineage.
- Reducer source-death ordering cases now pass complete typed survivor
  snapshots rather than the obsolete integer/boolean fixture form.
- Removed the unused live Core fixture and enabled runtime tripwires for every
  test. Existing pure scope is retained: no sockets, background threads,
  processes, timers, sleeps, or actual blocking waits.

## Exact finite infrastructure costs

| Case family | Nodes / stores | Inputs / leases | Pull callbacks | Owner protocol calls | Cleanup |
|---|---|---|---:|---:|---|
| Canonical ACK-loss cancellation | 2 / 1 KiB each | 1 / 1 | 3 | 7 borrowed RPCs | 1 Cancel, 1 custody ACK, 2 Drops |
| Canonical ACK-loss success | 2 / 1 KiB each | 1 / 1 | 3 | 7 borrowed RPCs | 1 Push, 1 custody ACK, 1 INLINE publication, 2 Drops |
| Definite Push failure | 2 / 1 KiB each | 1 / 1 | 3 | 6 borrowed RPCs | 1 failed Push, 1 abandon, 1 custody ACK, 2 Drops |
| Two-owner death (2 cases) | 2 / 1 KiB each | 2 / 1 | 6 | 12 borrowed RPCs | 1 Cancel, 1 custody ACK, 2 death fences, 2 live-owner Drops |
| One-owner STALE/quarantine/death | 2 / 1 KiB each | 1 / 1 | 3 | 5 borrowed RPCs | 2 exact Cancels, 1 custody ACK, 2 death fences |
| Consumer retry | 0 | metadata producer / 0 | 0 | 5 borrowed RPCs | 1 retained Release through output GC |

Stored input payloads are at most 128 bytes each. At most three Cores exist in
a case. Each canonical fixture creates one real grant and Core observes it
with one exact lease replay; no second allocation occurs. The death cases
create one GCS authority with an inert TCP-constructor boundary and install
one actual Worker death. Queue reads are capped at eight items per drain;
the existing reference mailbox is capped at 128 events per drain. The
shutdown contract passes `timeout=0.01` but exits on retained unresolved
protocol state without starting lanes or waiting.

## Scope limits

- The temporary typed incoming bootstrap hold is explicit fixture state, not
  evidence of a complete outer export discovery/prepare/promote lifecycle.
- Remaining owner/report cases and the retry producer are metadata-only
  reducer fixtures. They do not claim physical stored-byte creation or pull.
- Node ledgers are real, but Node GCS resource-report transport is inactive.
  These cases do not establish GCS resource-report convergence.
- The STALE response is a declared typed negative wire input; it does not
  invent reconstructed producer history. Cancel alone never grants custody
  or authorizes physical deletion.
- Source-death ordering does not replace late-old-Drop/reconstruction-epoch
  evidence. The unresolved local shutdown early return does not establish
  public multiprocess shutdown.
- Failure teardown closes actual Python handles and fences pure mailboxes. It
  does not manufacture terminal success, clear live obligations, or run normal
  GC against the deliberately retained dead-owner image.
