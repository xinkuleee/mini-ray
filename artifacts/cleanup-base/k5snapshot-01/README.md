# P2 scalar snapshot and retirement candidate

This completes the remaining journal projection slice on the passed effect candidate. `candidate/` is a complete independent 419-file input. `snapshot-scalar.patch` changes only two source files and the 39 existing test/helper consumers; base, prior candidates and P1 evidence remain untouched. It does not use progress8→1.

## Actual source changes

The journal snapshot now returns frozen `materialized: bool`, `result_retained: bool`, and `retirement: OutputPublicationTombstone | None` projections. No mutable fields or independent state authority are added to `_Record`:

- `materialized` is computed from the existing MATERIALIZE acknowledgement, so it remains true after acknowledged materialization loses its payload.
- `result_retained` is computed from whether the existing optional result is present. It is independent of whether an ACK happened: partial unknown writes still belong to the intent/physical cleanup path.
- `retirement` is the detached exact adoption retirement receipt, not a boolean inferred from result absence or phase. Owner-death cleanup can have no result and no adoption retirement receipt.

`OutputPublicationSlotTombstone` becomes `OutputPublicationTombstone` without the constant-zero slot field; publication/digest/ObjectID/proof binding remains exact. No old class alias is retained. `retire_completed()` returns one detached tombstone. `OutputPublicationPayloadRetired.tombstone` carries the same one detached receipt instead of a tuple. All source/test callers migrate together.

Node cleanliness reads the scalar result flag and requires a retirement receipt for a successful completed lease. Its separate owner-death-finished branch remains ahead of that check, so a cleaned dead owner is not forced to fabricate an adoption. Lease/report ACK independence, journal Complete, physical Store GC and rollback are unchanged.

## Consumer and negative preservation

The 39 test/helper edits mostly replace tuple truthiness with corresponding bool or optional-receipt tests. Three tests structurally consume retirement returns/exception receipts; these now assert scalar publication/ObjectID/digest/proof identity, exact replay and detached nested fields. The trace observer probe and its expected tuple elements change together from (0,)/() to True/False.

Existing journal retirement tests explicitly verify `materialized is True`, `result_retained is False`, and the exact retirement receipt after adoption. Pre-Complete rejection, changed owner/digest/commit rejection, no physical Drop/rollback invention, returned/input/exception proof non-aliasing, owner-death cleanup and all original runtime markers/scenarios remain. No source or test uses the retired projection field names/class/exception tuple.

## Measured costs

Two touched source files total 7899→7892 physical lines (-7), normalized bytes 378484→377970 (-514), 1022→1017 branch AST nodes, and the same 235 methods. The journal itself is 647→641 lines. Its three singleton/empty tuple projections become two bool values and one optional receipt; no additional historical scan, lookup index, getter or duplicate authority is introduced. Retirement method/exception no longer allocate a singleton result container. Heap or timing gains have not been benchmarked.

The migration footprint is 2 source +39 test/helper files, with low code churn per ordinary reader. Existing tuple representation is not retained as compatibility state. Runtime validation remains required; the candidate is an actual full caller implementation rather than a proposed data model.

## Verification and execution

All 393 Python files parse/compile without imports or execution. The complete raw source inventory was verified against the passed effect candidate before and after build. `summary.json` contains per-file raw hashes and costs; `candidate-raw-sha256.json` is the pre-registration manifest. `git apply --check` passed against the exact effect input.

Root requested execution freeze as `base-p2snapshot-01`. `prepare_execution.py` redirects the existing static registrar to this candidate without new selections, preserving 311 reviewed migrations,26 pure gate files,32 smokes and their markers/costs; only input hashes refresh. `execution.json` and `execution-raw-sha256.json` record the frozen archive. No pytest, base edit or commit occurs here.

`validation-selectors.json` lists the existing journal/retirement/Node owner-death/trace/spillback cases and real ordinary+contained paths for root execution under the unchanged 30-second process-tree runner.
