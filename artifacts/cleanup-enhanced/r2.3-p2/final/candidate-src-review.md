# Single StageAck candidate source handoff

Status: `SRC_FROZEN_AWAITING_ROOT_RUNTIME_VALIDATION`. Source implementation performed only in `ack-trial/candidate/src`. The baseline and formal repositories were not edited. Tests/scripts and the runtime comparison driver are owned by root and were not edited or run by this implementer.

Baseline: post-R23-01 E archive SHA256 `26a54d5549fd61ddbd0e4dc29816f38c18b8f17baf013335312262fcb42f2669`. Exact changed source hashes and patch hash are recorded in `candidate-src-identity.json`. Patch: `candidate-src.patch`, SHA256 `1da561e41b36764f29bb6de23794f4525f199665bfa35c0c56f4ed29664c8574`.

## Changed source

| Path | Result |
|---|---|
| `enhanced_publication.py` | One success-only `PublicationStageAck` wire value; exact concrete request/stage/fact/owner/closing validation; detached nested fields and constructor-based pickle support. `forward_open` derives from closing facts. Full `PublicationReply` remains for query/ABSENT/failure only. Private stage mapping and `stage_ack_from_snapshot` project the single existing authority record. First normal transition still constructs all fallible values before `_records` and sequence mutation; replay projects the retained snapshot. |
| `enhanced_publication_control.py` | Existing pre-query retained. Existing validated later-death branch returns the actual first-fence ACK via explicit factory exception. Post-mutation owner-death lookup uses accepted owner context. No new RPC/query or cleanup state. |
| `control.py` | Trace reads owner from accepted ACK and object/Task/attempt/lease from exact reference. No extra history/query; observer exception isolation preserved. |
| `enhanced_publication_client.py` | Query/rejection retain full reply; mutation requires exact StageAck. Reference-only requests compare ACK owner against locally known full publication. Task/put commit checks actual Complete/preparation and current forward. Existing mutation methods return the same small PublicationReceipt, except begin returns the new actual ACK. |
| `output_publication_journal.py` | Actual request/reference/stage/manifest owner checked; ARM accepted fact compared with local actual preparation; forward/ACTIVE checks preserved. Existing receipt dictionary remains the only stored GCS progress. |
| `output_publication_node.py` | Actual success mutation ACK distinguished from failure. Graph checkpoint retains its real response; journal receives new ACK. |
| `node.py` | Terminal-loss gate checks actual accepted Complete, reference, stage and manifest owner before deliberately discarding its reply. No retrospective query substitutes for this checkpoint. |

No `core.py`, `publication_gate.py`, `output_protocol.py`, authority record layout, RPC stage/count or synchronization change was needed. Node adoption continues receiving existing `PublicationReceipt`; no StageAck API leaks into B.

## Explicit facts and exceptions

Begin and PrepareGraph use accepted PublicationRef (Begin still echoes full publication/route). ARM uses full actual TaskPreparedReceipt. Terminal and Task commit use Complete. Put commit uses actual PutPreparedReceipt. Adoption uses owner proof. Fence uses the first accepted fence. Retirement uses the first exact ClosedContainedHolds.

All StageAck fields are reconstructed at constructor/replace/unpickle. `_WIRE_TYPES` automatically includes the new `_Wire` subclass; `_Wire.__reduce__` re-enters its constructor after deserialization. Class checks reject unsupported/subclass values, exact request/reference/stage drift, wrong fact, known owner mismatches, inconsistent closing facts/stages/sequence order and sequence reuse by distinct stages. Constructor does not reconstruct a full snapshot or claim to re-prove the authority graph history; that unchanged full validation remains before projection.

Later registered owner death may acknowledge an already accepted, different owner fence. Only the current controller branch passes `accepted_existing_fence=True` after existing membership validation. Request echo remains the later death while `accepted_fact/fence/fence_receipt` remain original history. Ordinary authority fence mismatch still rejects. Existing controller order—cleanup admission before returned reply construction—is unchanged from the full-reply baseline; normal first authority transition still builds reply before state commit.

Closed historical Task commit must predate its fence; terminal/adoption may legitimately arrive after fence or retirement. Successful Begin/Prepare/ARM require open state because baseline authority rejects their fenced replays. Current remote open remains no substitute for caller local epoch/abort checks.

## Static checks and evidence boundary

All seven changed files parsed using standard-library `ast.parse` with the existing Windows audit Python. No project module, test or collector was imported. Diff whitespace check reported only checkout CRLF notices, no whitespace errors. A separate subagent reviewed concrete wire/authority/controller integration; distinct-stage closing receipt sequence reuse was tightened before freeze. No runtime test or performance result is claimed here.

The root consumer inventory added `test_put_home_failover.py:253`: its existing C5 cut now checks actual accepted PutPreparedReceipt; remembered client publication supplies metadata and the already-existing query remains historical observation. This is a direct test bridge adaptation, not a new production query. The preparation inventory is a starting closure; root is adapting other exact current test consumers based on actual evidence.

Disposition remains open until root completes bounded contract and lifecycle validation plus same-input cost comparison. This candidate must be retained only if net benefit and unchanged contracts are demonstrated; failures must be fixed or documented under the plan actual-counterexample rule, never converted into a passing label.
