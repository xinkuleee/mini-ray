# P3 repeated validation trial

A real one-line Core candidate removes only standalone `validate_output_publication(plan)`. Public owner commit, every deep-copy/tamper boundary and both receipt queries remain unchanged.

## Lock conclusion

No new revision capability or owner/recovery cross-lock API is required for this candidate. Core holds its existing composition lock. Recovery preflight changes only copies. Final owner commit still rebuilds and validates immediately before owner mutation; recovery commits only after owner success. Recovery has no independent lock.

## Concrete bounded cases

One new test runs four cases: ordinary/contained output crossed with recovery-copy failure or same-size payload corruption at final owner commit. It checks no early owner/recovery mutation, no RPC/wake, unchanged child/store/journal facts and exact successful replay plus GC. The existing success-preflight failure selector moves its injection point to commit entry without removing its negative oracle.

## Cost and disposition

Source: one removed line; no new API/state/lock. Removes one redundant owner plan/digest/state-validation pass per first CAS. Tests and runtime are not executed. Larger prepared-token/revision design remains deferred: stale tokens need attempt/collection/retirement/fence revision coverage, and owner RLock alone is not a reentrancy fence.

Use the five already registered test files in assessment.json after trial hash refresh. No base or registry changes have been made.
