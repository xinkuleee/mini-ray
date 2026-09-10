# StageAck candidate structural cost

The single candidate adds **204 physical source lines and 181 code-bearing physical lines** across seven files. It adds one wire class and eight wire fields, with no new retained authority/progress table and no new network RPC/query call site. Its potential saving is reduced successful-reply construction and consumer revalidation of full history, rather than less source code. Structural cost alone decides neither adoption nor retention; pair it with the measured lifecycle data and correctness evidence.

Inputs are exactly the raw hashes in `candidate-src-identity.json`: baseline archive `26a54d5549fd61ddbd0e4dc29816f38c18b8f17baf013335312262fcb42f2669`, source patch `1da561e41b36764f29bb6de23794f4525f199665bfa35c0c56f4ed29664c8574`. `structure-cost.py` is an audit-only standard-library AST/tokenize/hash script; it reads both source trees, asserts their expected hashes, and writes `structure-cost.json`. Compared source and tests were never imported or executed.

## Same-input counts

Physical lines use `len(raw_bytes.splitlines())`. Code-bearing lines are unique physical lines occupied by Python tokens after comments and module/class/function docstrings are excluded. This is a defined source-size measure, not a speed metric. Blank/documentation changes are therefore visible separately.

| File | Physical B → C | Physical delta | Code B → C | Code delta | Added / deleted diff lines |
|---|---:|---:|---:|---:|---:|
| `control.py` | 3,781 → 3,780 | −1 | 3,149 → 3,148 | −1 | 5 / 6 |
| `enhanced_publication.py` | 996 → 1,168 | +172 | 795 → 944 | +149 | 188 / 16 |
| `enhanced_publication_client.py` | 109 → 136 | +27 | 81 → 108 | +27 | 53 / 26 |
| `enhanced_publication_control.py` | 324 → 324 | 0 | 274 → 274 | 0 | 4 / 4 |
| `node.py` | 7,340 → 7,344 | +4 | 6,345 → 6,349 | +4 | 8 / 4 |
| `output_publication_journal.py` | 783 → 783 | 0 | 615 → 615 | 0 | 7 / 7 |
| `output_publication_node.py` | 639 → 641 | +2 | 492 → 494 | +2 | 6 / 4 |
| **Total seven files** | **13,972 → 14,176** | **+204** | **11,751 → 11,932** | **+181** | **271 / 67** |

The same AST scope adds 158 statement nodes, 62 `if` statements, one conditional expression, two `for` loops, 22 boolean-operator continuations, 34 `raise` statements and 93 call expressions. It adds no `except` handlers, `while` loops or comprehensions. These are source syntax counts, not executed branches, test coverage or cyclomatic-complexity scores; do not add boolean continuations to `if` counts and present the sum as a measured runtime cost.

## Types, fields and authority

The added `PublicationStageAck` class occupies candidate `enhanced_publication.py:613–736`. Its eight actual fields are `request`, `reference`, `owner_worker_id`, `receipt`, `accepted_fact`, `fence`, `fence_receipt`, `retired_receipt`. Four readonly properties—`accepted`, `error`, `error_kind`, `forward_open`—are methods, not extra saved flags. The other added functions are `_request_stage`, its constructor and `stage_ack_from_snapshot`: seven new function definitions altogether.

The old six-field `PublicationReply` remains because full GetPublication/ABSENT and typed rejection replies still need it; the candidate removes its successful-mutation use. Thus total wire shape definitions increase by one rather than replacing the old type wholesale. Both types are concrete response variants, not selectable backends or duplicate authority records.

`PublicationAuthority._records/_sequence/_lock`, the full `PublicationSnapshot`, journal `publication_receipts`, controller `_deaths` outbox, and client `_publications/_current` retain their prior responsibilities. No new stored mutable phase/forward field appears. The constructor `sequences` dictionary is temporary cross-field validation and is never retained. The closing fields convey accepted facts on one reply; `forward_open` derives from them, not from a new mutable table.

The old full snapshot type is deliberately still present. Its full preparation/causal/closed-hold validator remains authoritative before state commit. A smaller response cannot itself re-prove the entire omitted history; it instead binds the required accepted fact to the exact request/stage, with existing Node/client expected-owner/preparation/Complete checks.

## Where the added reading/branch cost resides

The main added reading cost is the 93-line strict StageAck constructor (`enhanced_publication.py:628–720`): 37 `if` statements, two loops and 20 explicit rejection points. They select the eight concrete request/fact meanings, validate known owner context and exact closing history, and prevent a receipt from silently becoming forward permission. The 36-line projection factory (`:739–774`) adds 13 `if` statements and six rejection points, including the already-supported later-death/first-fence exception. The factory is one local projection over the existing canonical record.

The client `call` path expands from 13 to 36 physical lines (`enhanced_publication_client.py:47–82`): `if` statements 4 → 13, explicit raises 4 → 9, calls 8 → 20. It now locates and rebuilds known publication metadata, discriminates full query/rejection versus success ACK, validates reference/owner and preserves the original stage check. Task/put commit adds explicit accepted-fact comparisons. These are actual added consumer costs, not eliminated by explaining that the ACK is smaller.

The Node journal remains the same length and branch count; it replaces snapshot/reference/forward/prepared reads with exact ACK owner/reference/forward/fact reads. Node terminal gate adds four source lines for reference/owner checks. The adapter adds one successful-type rejection branch. Trace drops the full publication read, derives execution from reference, and uses accepted owner context. Controller still performs its original full query before membership composition and still owns death admission/cleanup.

The ordinary reading route therefore remains Node/client → authority → exact reply decoder/consumer, with a new local StageAck projection and constructor in that route. It does not remove a protocol stage or need to read a second state authority. A reviewer must nevertheless understand both response variants and the explicit fact union.

## Copy/validation layers: removed, retained and added

| Layer | Baseline | Candidate | Cost interpretation |
|---|---|---|---|
| External request and authority entry | Request constructors and authority `_copy` | Same | No claimed input-copy elimination. |
| New authority stage | Full `PublicationSnapshot` rebuild/validation | Same | Internal canonical history is still copied/validated. |
| Successful returned value | `PublicationReply` reconstructs complete request, full snapshot and receipt | StageAck reconstructs complete request, exact fact/reference/owner, receipt and closing context | The whole-history returned copy is removed; a new strict cross-field constructor replaces it. |
| Node adapter + journal receipt | Each `replace(reply)` revalidates/copies full history | Same receiving boundaries revalidate/copy StageAck fields | Smaller content passes through retained defensive boundaries; boundary count is not zero. |
| Owner client | Request/reply rebuild | Same plus known-publication rebuild/check | Some publication copy cost is intentionally added to bind owner for reference-only requests. |
| Full query or typed failure | Full detached reply/snapshot as available | Same | No saving assumed; original controller pre-query remains (`enhanced_publication_control.py:46`). |
| First normal authority commit | All fallible reply construction before record/sequence mutation | Same, via projection factory (`enhanced_publication.py:1071`) | No new commit point or fallible-after-commit window on this transition. |

Full request echo remains. ARM duplicates actual preparation in echo and accepted fact; put commit does the same; terminal/adoption/retirement also retain fact/echo duplication. `_set(request)` and `_set(accepted_fact)` reconstruct these separately, so they are not merely two references to one already detached fact. This is permitted because the initial experiment removes full snapshot redundancy without changing exact historical binding. Begin already includes publication/route in its request; Begin/Prepare accepted fact is only reference. Task commit carries the small accepted Complete witness because its request contains only reference.

Closed historical replies can carry a full canonical first fence and FENCED/RETIRED receipts. These extra fields are usually absent on open successful stages, but a delayed death or retirement reply can be larger than an open ACK. A normal FENCED reply may rebuild the proof in request, accepted fact and closing fence; the later-death exception deliberately has a different requested death and canonical first fence. Duplicate paths and actual serialized identity-sharing must be assessed from real envelopes rather than adding standalone nested pickle sizes.

There is no new network query/RPC call site. The controller original full query, owner client GC/death queries and test-only observation queries must be counted in their respective measured roles. Pure-call measurements should not be represented as timing/receive-deserialization measurements of an actual network.

## Relation to actual measurement

Root reported that the four controlled lifecycle slices currently preserve exact requests/callbacks/semantic checkpoints and query counts, with first-repeat GCS exchange bytes and local copy-call counts both reduced. The mutable `measurements.json` is the authoritative measurement index; this static report does not freeze preliminary r1 numbers or replace the final three-repeat result.

The structure tradeoff is explicit: the candidate spends +204 source lines and more typed validation/error branches to return and revalidate less historical material. If repeated actual trajectories retain their contracts and show reductions after this added publication/fact/closing copy cost, source growth alone does not negate that improvement. Conversely, DTO size or constructor counts alone do not establish teaching clarity, process correctness, network speed or absence of missing consumers. Final disposition must weigh the measured trajectory evidence, bounded contract/gate results and the concrete additional reading burden together.

A separate read-only subagent reviewed all seven files and independently confirmed these retained-copy, exact fact duplication, closing payload, consumer branching and before-commit differences. It ran no tests, benchmark or project imports.
