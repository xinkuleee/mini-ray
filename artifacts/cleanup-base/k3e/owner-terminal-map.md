# K3 owner terminal metadata migration

Authorized edits: current B tests/unit/test_output_owner_terminal_metadata.py and this note only. No production source, helper, baseline manifest, repository docs, pytest or commits changed/run by this subtask.

## Provenance and retained scope

Before migration the current B file was byte-identical to fixed B and E archives under C:/Users/t-hdong/Desktop/gao/audit/two-version-cleanup/{base,enhanced}/tests/unit/test_output_owner_terminal_metadata.py.

Original SHA256: bd925f8b87c1de92a92169ddc1dfae05ddcf4ae1e3efda5d0e182d354cbcad6d.
Final migrated source SHA256: 28694238a0e579521cbfe81b55775f0c40ac9ed23f4006ba5a4778e7b3a0fb70.

R2.2 docs/project-cleanup-plan.json:test_actions.retained_contract_groups lists both original functions at lines 224 and 248 and explicitly labels the following replacements partial_behavior_reference_not_one_for_one:

- tests/unit/test_enhanced_owner_client.py::test_w3_owner_ready_and_actual_adoption_survive_unknown_c7_ack_and_early_close
- tests/unit/test_enhanced_owner_client.py::test_w4_real_reconstruction_retires_old_edges_before_new_attempt_and_gc_preserves_history
- tests/unit/test_node_lost_output_resolution.py::test_known_complete_without_bytes_becomes_lost_and_keeps_live_references

These references preserve adjacent custody/reconstruction history behavior, not the full-owner reachability oracle or all five terminal payload/identity negatives. Both stronger common owner guarantees are retained directly in the migrated B tests.

## Exact per-function disposition

| Fixed legacy function | Current B selector | Preserved assertions and retired mechanism |
| --- | --- | --- |
| test_full_owner_gc_forgets_all_payloads_but_replays_exact_caller_plans, original line 224 | Same name, migrated line 170 | Real owner publication and child prepare/promote/release/GC; argument, keyword and serialized function bytes are installed into TaskSpec before owner registration; actual INLINE result is owner-held before GC. Every vars(owner) root except _lock is deeply scanned after complete parent/child collection and again after original/rebuilt caller-plan query/complete replay. No raw payload, TaskSpec, FunctionDefinition, InlineArg, ResultDescriptor, envelope or the original two payload-bearing plan classes may survive. Exact original/structurally rebuilt caller plans return ALREADY_APPLIED, repeat begin returns None, old publication remains FENCED, complete owner and child states remain unchanged. The two-slot/sibling and GCS graph release scaffolding is retired; one current output plus a real child final-hold release precedes complete_output_publication_collection(plan). |
| test_terminal_owner_rejects_changed_payload_or_collection_identity, original line 248; parameters argument, keyword, function, result, collection_id | Same name, migrated line 196; all five parameters retained | Changed argument/keyword/function payloads produce otherwise valid current TaskSpecs; changed result rebuilds a self-consistent manifest/witness/descriptor/collection membership, so rejection is terminal identity, not an unrelated envelope mismatch; changed collection ID fails repeated begin. Both terminal complete and receipt query reject each altered plan, genuine old plan still returns exact collection, and complete parent/child state and metadata-only scans remain unchanged. Old graph-release manifest corruption is unnecessary because B no longer accepts a graph receipt; exact result digest remains part of terminal membership. |

## Strong reachability oracle preserved exactly

_owner_state reads all of vars(owner) except its synchronization _lock, not selected receipt dictionaries. _assert_metadata_only follows dict keys and values, tuple/list/set/frozenset members, Enum values, every declared dataclass field and every extra instance attribute not declared as a field. It tracks visited identities to avoid cycles; unknown history object kinds fail.

Only exact JobID, TaskID, WorkerID, NodeID and LeaseID leaves may own bytes, and only when vars(value) contains exactly the value field and its bytes are exactly 16 bytes. Payload bytes elsewhere, including arbitrary 16-byte data or extra opaque-ID cache fields, are rejected.

The forbidden classes are the exact original contract: bytes, bytearray, memoryview, protocol.TaskSpec, protocol.FunctionDefinition, protocol.InlineArg, protocol.ResultDescriptor, OutputPublicationEnvelope, OutputOwnerPublicationPlan, OutputOwnerPublicationCollectionPlan. Generic ObjectMetadataCollectionPlan and other metadata-only types are recursively inspected rather than rejected solely for having a plan name. No broader blacklist was retained.

The caller is intentionally allowed to keep its original spec/publication/collection plans; the scan roots are only the owner authorities. Rebuilding exact caller plans therefore tests durable hash/identity replay without accidentally demanding that caller objects be payload-free.

## Current fixture and resource cost

The local _Fixture subclasses the existing read-only test_output_owner_publication._Fixture(edges=True, all_stored=False); no helper was edited. It modifies TaskSpec payloads before table() registration, constructs one exact single-output plan using the existing current manifest/header/descriptor identities, and uses a second real ObjectOwnerTable for its one child.

Per case: one parent owner, one child owner, one tiny INLINE output, one child, one prepare, one promote, one actual final release, one parent collection and one child collection. No Core/Node/GCS/Worker constructor, process, thread, transport, store, sleep, wait, user code or payload deserialization runs. The child provisional hold is already tombstoned by actual promotion; the final hold keeps it alive until parent GC releases it. Child local handle is released before collection, proving the contained hold provides the lifetime.

Exactly two unit functions / six cases remain. The first performs two structurally equivalent caller-plan query/complete replays; each of the five negative cases performs one altered complete and one altered receipt query, plus its matching original receipt query. All work is direct finite in-memory reducer calls.

## E and scope limits

The no-payload terminal owner history and exact replay/negative tests are shared B/E business invariants. E can apply the same current single-output owner-table contract; its global graph receipt/retirement composition remains an E integration concern, not proof supplied by this B file. The old two-output sibling lifetime and cross-container graph release are retired capabilities, not reintroduced tests.

The test uses INLINE output deliberately to exercise real owner-held payload bytes. It does not claim STORED physical deletion, remote child transport, Node execution, whole runtime GC or concurrency evidence. Existing owner-client references in R2.2 remain partial coverage; they do not replace these stronger whole-state scans.

## Validation and freeze

Syntax-only ast.parse succeeded after final edits and found the two functions. git diff --check passed with the normal LF/CRLF working-tree warning. No pytest or test-module import execution was performed. Parent received final source hash before coordinated acceptance freezing; no further source edits are planned.
