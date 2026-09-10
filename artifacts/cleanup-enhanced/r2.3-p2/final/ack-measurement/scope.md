# Fixed ACK lifecycle measurement harness

Status: frozen source candidate; not imported, collected, or executed by its author. The parent must statically review/AST-check, register the exact selectors, and use the existing 30-second process-tree runner. Copy the same two files unchanged into each fixed trial's `tests/unit/`.

| File | SHA256 | Bytes |
|---|---|---:|
| `tests/unit/test_ack_lifecycle_measurement.py` | `2068eba394db342dfe4ff284e4758011b61f3ddf110b420487c507613983c83d` | 30,662 |
| `tests/unit/_ack_cost_capture.py` | `3b4dc6a14d296f7c1941a4e0f9f7a880bd33e1ad655aead7bb8639d982b4d886` | 7,385 |

Both are UTF-8 without BOM, CRLF. The collector is the existing inert `ack-preparation/cost_capture.py` copied unchanged apart from normalized working-copy newline representation. The harness itself does not branch on full Reply versus StageAck. It obtains semantic history from real GetPublication queries and leaves current consumer validation to the real source.

## Exact run inventory

Register twelve migration selectors with `marker=unit`, work package R23-02. Each selector has its own existing 30-second runner budget, including only one fresh trajectory/repetition. Do not register/run the whole file as the measurement gate.

```text
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[task0-r1]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[task0-r2]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[task0-r3]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[task_mixed-r1]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[task_mixed-r2]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[task_mixed-r3]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[put_contained-r1]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[put_contained-r2]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[put_contained-r3]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[whole-r1]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[whole-r2]
tests/unit/test_ack_lifecycle_measurement.py::test_actual_lifecycle_cost[whole-r3]
```

Set `MINIRAY_ACK_CAPTURE_DIR` separately for baseline and candidate. It is inherited by the existing runner. Otherwise files go to that trial's `artifacts/ack-measurement/`. Each exact test writes `<slice>-r<N>.json` and a short `ACK_MEASUREMENT` line via `capsys.disabled()` so the bounded runner log identifies the artifact. No extra CLI or asynchronous worker is used.

## Actual path and scope

| Slice | Real composition | Assertions / finite work |
|---|---|---|
| task0 | Enhanced owner-client `_Runtime`, real Core submission/owner, output discovery, Node adapter/journal, authority | No child; actual Prepare/Complete/terminal/adoption/Node payload retirement; outer GC and GCS RETIRED; one task attempt. |
| task_mixed | Same two real Cores; import existing put-runtime `_Mailbox`; addressed real acquire/release/prepare/promote handlers | One publisher-owned child and one truly borrowed owner-owned child. Borrowed handle comes from a real seed put plus `_loads_owned_value`, and its actual token/source binding is checked. Two real transfer types/holds, one real accepted CommitGraph reply discarded, pending-owner/retained-payload assertions, exact resume, final hold removal/tombstones and borrower release. |
| put_contained | Existing put-reference `_Runtime`, actual NodeRegistry and Node 16 KiB store | Retains the borrowed+stored path's original alias and independent nested-child lifetime assertions, source close, stored outer get/GC, restored child/leaf survival and final collection, no Task lineage/ARM/Complete/adoption. |
| whole | Enhanced owner-client `_Runtime` and actual owner reconstruction entry | First stored attempt lost through real drop, old graph retired before next attempt, replacement actual publish/adopt, late original Commit/Adopt/Retire through authority before/after replacement GC, successor unchanged; two attempts. |

The mixed Task requires two existing Core addresses, not a new third owner or fixture authority. Its adapter dispatch simply selects the actual owner by address; borrowed handles use real acquire/release methods. The original test helper's one-child `<1024` payload condition is not modified: the measurement-specific prepare sequence has an explicit `<8192` serialized sample cap. It retains the original 16 KiB store and all production limits. The same prepare sequence and 8192 cap run in both variants.

The cost probe permits at most 256 callbacks and 2048 events per trajectory. Existing owner-runtime 160 and put-runtime 160 GCS/128 physical callback limits still execute. Discovery sees at most two task children; whole creates at most two task attempts. All drains use existing finite mailbox loops. Tests forbid real Core/Node construction, process/thread start, timers, sockets and sleeps; Event.wait is allowed only for already-set completion.

This is a controlled local composition. The Node Complete callback is the retained fixture's execution witness boundary. It is not an actual OS Worker, scheduler, resource ledger, GCS membership controller, or process shutdown measurement. Existing affected contracts and actual process smokes remain independent required evidence. No fake successful protocol reply is introduced: all GCS, child, put/Node and owner callbacks invoke real methods; the local Node adoption bridge retains the existing exact owner proof checks and real journal retirement.

## Cost attribution

- Each actual callback records the real request and returned reply, serialized through current `transport._serialize` and its `_WireRequest`/`_WireReply` business envelope, plus framing bytes. Full recursively detached field facts are saved under `value` (binary payloads identified by length and SHA256).
- The callbacks remain in-process: serialization is measured for length, not used to deliver messages. Transport receive/unpickle/decode cost, socket/kernel work, RTT, bandwidth, trace sidecar cost and elapsed-time performance are not measured. Local consumer copy counts must not be extrapolated to full network-path cost.
- When the existing runtime loses CommitGraph's accepted reply, an observation wrapper records the actual authority-produced response before the loss. The callback event tags it `discarded`; retry is its own actual exchange. It does not query or replay merely to produce a sample.
- Explicit Core/Node operations and callbacks have profile boundaries. Actual copy/validation/constructor counts are current-thread counts, not time or allocated-byte estimates. Serializer and capture work is suspended.
- Fixture/harness direct authority queries are marked `test_observation`, `not_sent` request / `generated` local reply, and excluded from business-copy profiling. Client GetPublication calls through runtime RPC remain `business`. No controller participates in these slices, so no controller-internal query claim is made.
- The existing Node adoption bridge's extra owner/handoff/central invariant observations are suspended; its real journal retirement and actual reply construction remain profiled. Authority scans used by fixture teardown are suspended.
- Checkpoints retain exact history, stage receipts, accepted facts, owner state when it still exists, collection state, journal, real store bytes, task finish/GC/borrower obligations, and actual local completion/adoption callback counts. After collection the owner entry is correctly absent; no fake snapshot is created.

IDs and UUID tokens are deterministic finite test inputs, reset for each repetition, using exact ID constructors. Submission uses an importable module-level function so the two trial filesystem paths do not introduce a local-lambda serialized code filename difference. These inputs are not replacements for effect acknowledgements.

## Comparison output

Each JSON preserves ordered events, separate message direction/delivery/query role, content path/identity counts, raw semantic facts, and copy-count rows. It records `COMPLETED_ASSERTIONS` only after real fixture final cleanup succeeds. Failure keeps `FAILED_OR_INCOMPLETE`, retains cleanup error separately and does not swallow the originating assertion. Root records input identity/environment/manifest and command log alongside each output; aggregate only after both variants complete corresponding exact paths.

Compare three deterministic repetitions for internal consistency, then compare baseline/candidate business request/reply bytes, generated discarded bytes, copy/validation rows, query counts and exact semantic checkpoints. Reply representation differs intentionally; do not require message `value` equality across different reply types or collapse meaningful histories into a single success boolean.

Independent read-only review found and corrected two harness issues before freeze: querying a deleted owner snapshot after collection and profiling the fixture's extra adoption invariant observations. Other path routing, genuine borrowed credential acquisition, exact lost-ACK capture and reconstruction sequence were checked against current source. No execution result is claimed by this scope note.
