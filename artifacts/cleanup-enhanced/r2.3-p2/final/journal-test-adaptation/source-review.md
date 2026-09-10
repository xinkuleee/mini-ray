# E journal test adaptation

This single-file candidate migrates the retained E journal contracts to the current enhanced GCS/child-reply gates. The parent reproduced all 23 failures against the unchanged full-Reply E input and the StageAck candidate: they reached missing GCS PREPARED admission in the old fixture. This is an E test migration gap, not evidence that StageAck introduced that failure. B is outside this adaptation.

## Frozen candidate

| Artifact | SHA256 |
|---|---|
| `source-original.txt` | `443b429fea0687cbdc106ac711428bca79a84b8bb4b50ea8e9f01525e8f6db5f` |
| `tests/unit/test_output_publication_journal.py` | `6b60c1540cf0da3e6ebd9e4d3653c04623e2a7775ad3d13efa9d3f605ce78cec` |
| `journal-adaptation.patch` | `076518895a58f691f8accd04e404ac9d71dbb5fc237ba82e204de76320ac1dce` |

Candidate: 30,632 bytes, UTF-8 without BOM, CRLF. No runtime/shared helper/manifest file was edited by this agent, and no project imports, collection or tests ran here. Parent owns application, reviewed registry/closure and exact bounded execution against full Reply and StageAck.

## Preserved test inventory

All original eighteen function names, ordering and decorators remain. The first function retains four combinations (plain/children × inline/stored), the late-ACK function retains MATERIALIZE/PROMOTE, and the adoption-retirement function retains INLINE/STORED. The other fifteen functions remain single cases. Total remains 23, pending actual runner confirmation. No negative test was deleted, xfailed or relaxed to avoid execution.

## Actual stage and reply path

- Fixture `register()` records its explicit model owner-register ACK, then actual authority BeginPublication and PrepareGraph results through journal.record_publication_reply. The exact-replay test that manually owns this ACK installs C0/C1 immediately afterward rather than silently skipping their prerequisites.
- Child prepare/promote replies come from actual `ObjectOwnerTable` transitions for two fixed child identities. The borrowed child's complete source binding is registered through the real owner reducers before Prepare. A received-reply cache preserves the exact original reply for journal replay and later invalid/terminal ACK tests. No always-success callback or fabricated StageAck is present.
- Fixture promote obtains real ARM using the journal's actual preparation receipt. The ordered-gates test still exercises every original gate and explicitly requires ARM after the final child promotion ACK before local Complete becomes possible.
- Fixture Complete remains the real journal method and then records authority C4/C5. Positive production-shaped adoption cases explicitly record C7 for the exact model adoption proof. The journal's direct invalid-retirement tests remain direct: journal.retire_completed validates local Complete/owner/proof and is not falsely described as an independent C7 authority gate.
- Rollback still starts with the real journal plan, preserving the no-effect immediate local retirement case. The bounded compensation helper then installs an actual authority FENCED receipt from an explicit model owner-abort fact, executes exact real child releases, records their typed replies, and obtains authority RETIRED from journal.closed_rollback_holds. SLOT_DROP is an explicit journal model ACK, not a physical-store effect.

The one gcs helper passes each actual authority reply unchanged into the matching journal. It checks exact request/receipt/reference; it does not synthesize a full snapshot or select a backend. Both current full Reply and StageAck can run this identical file through their own real journal decoder.

## Negative and evidence boundaries

All originally invalid child ACK calls now carry a real typed child reply so Python's missing-argument TypeError cannot mask the intended journal error. Invalid manifest/stage/ID checks still target the original journal method. Some invalid-delivery scenarios acquire the actual child reply before the journal rejects it: their zero-mutation assertion refers to the journal, not absence of an external child effect. This boundary is stated inline. Cached replies avoid fresh child effects for post-Complete rejections.

The file is a pure reducer contract. Owner-register/materialization/local Complete/adoption/abort inputs are explicit model facts. Actual child-owner state transitions and actual GCS receipts are real local reducers; no physical bytes, real owner CAS, OS Worker, network delivery or lease/resource release is claimed. Existing Node/Core/process tests prove those separate responsibilities.

Independent static review checked the written candidate against E journal/authority/owner source, including rollback validation order, no-effect rollback, ARM, retained metadata shape and all negative signatures. It reported no blocking issue. That review does not certify the 23 tests have passed.
