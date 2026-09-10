# P1 current-base integration package

**Frozen application candidate, not applied by this packaging task.** Target HEAD is 2e50c35fe90c41626c5d38627a87da52334d55cd on teaching-base. Root owns validation acceptance and application.

- Application patch: current-base-to-p1.patch, 523,477 bytes.
- SHA256: bf9dc5a8c6539c35fe6510a35afcc268aa620407992e4dbd854fc40f423acd7f.
- Exactly 121 changed paths: 14 source files and 107 tests/helpers.
- Incorporates the frozen minimal P1 change plus the two scalar prepare-call payload fixes found during trial 01.
- No manifest, runner, lock, README or unrelated current-source overlay is in the patch. Root must refresh the current branch's reviewed hashes after application.

The package was built by copying only these current-base paths to before/ and post/, applying the original minimal patch and the exact prepare fix to post/, then generating one normalized-LF diff. before/post snapshots retain their actual bytes; only diff generation normalizes CRLF. This avoids a 1.75 MB line-ending-only diff. The final 523 KB patch passed git apply --check against the recorded current base.

file-hashes.json records raw before/post hashes, normalized-LF before/post hashes, trial02 expected raw hashes and normalized AST fingerprints for all 121 paths. Every post file matches trial02 executable AST after removing docstrings. All 28 later runner-docstring changes were checked separately and preserved, including changes outside the patch. summary.json names the preserved paths. This exception is documentation-only, not a normalization of runtime semantic differences.

The test payload fix patch SHA256 is fa8599ed8a3b40085e1c7b870dcf6d9b5f899af2af452c053d9fc914b8a64328; its before/after evidence remains in audit/p1-evaluation-fixes/prepare-scalar-payload. Original trial01 failure logs and trial02 validation are unchanged.

No pytest, test body, runtime constructor or control-payload benchmark was run by this packaging task. Runtime results belong to the exact frozen trial02 snapshot and root's execution logs. The current-base integration still requires root's post-application identity and manifest handling.

## Structural cost against K4 baseline

The table covers the 14 changed source files, not all source or the whole repository. Code lines count physical lines with Python tokens after excluding comments, whitespace/layout and docstring spans; this is a reproducible static count, not logical statements or cyclomatic complexity. Full field lists and counts are in structural-cost.json.

| Source file | Physical K4 → P1 | Code K4 → P1 |
|---|---:|---:|
| core.py | 12,763 → 12,756 | 10,794 → 10,787 |
| node.py | 7,257 → 7,256 | 6,274 → 6,272 |
| output_discovery.py | 231 → 197 | 162 → 135 |
| output_handoff.py | 359 → 359 | 277 → 277 |
| output_protocol.py | 302 → 295 | 202 → 196 |
| output_publication.py | 492 → 420 | 374 → 309 |
| output_publication_journal.py | 674 → 673 | 521 → 519 |
| output_publication_node.py | 575 → 577 | 434 → 433 |
| ownership.py | 4,178 → 4,162 | 3,334 → 3,324 |
| protocol.py | 6,653 → 6,654 | 5,510 → 5,511 |
| publication_gate.py | 215 → 214 | 176 → 175 |
| reconstruction_runtime.py | 981 → 984 | 765 → 769 |
| task_outputs.py | 111 → 60 | 75 → 49 |
| worker.py | 2,195 → 2,196 | 1,840 → 1,841 |
| **Total changed source** | **36,986 → 36,803 (-183)** | **30,738 → 30,597 (-141)** |

The modest line reduction does not mean the representation change failed: its benefit is removing a retired dimension at protocol boundaries, while preserving correctness checks. P1 deliberately does not move Core responsibilities, collapse Node progress maps or redesign owner retirement tables.

## Concrete representation changes

| Boundary | K4 stored fields | P1 stored fields | Actual change |
|---|---|---|---|
| Task identity | TaskOutputManifest(task_id, output_ids) plus TaskExecutionKey(manifest, attempt_id) | TaskExecution(attempt_id) | 4 declared fields across 2 value types → 1 across 1; TaskID/ObjectID derived |
| Result metadata | OutputSlotManifest(object_id, tier, size_bytes, checksum, transfers) | OutputValue(tier, size_bytes, checksum, transfers) | Duplicate outer ObjectID removed; child transfers retained |
| Publication manifest | header, slots tuple, manifest_digest | header, value, manifest_digest | Single output wrapper removed; digest and header identity retained |
| Discovery result | manifest, slot_payloads tuple | manifest, payload bytes | Single payload wrapper removed |
| Prepare wire request | manifest, slot_payloads tuple | manifest, payload bytes | Exact bytes validation remains |
| Successful envelope | manifest, complete, results tuple | manifest, complete, result | One actual descriptor; Complete cannot recreate bytes |
| Owner membership | manifest, slot_index | manifest, slot_index | Both stored fields remain in P1; readers use manifest.value directly and object_id derives from publication identity. Constant-zero membership cleanup remains P3 scope. |

Children, replicas and borrowers remain real collections. The ordinary TaskReply.results and lease/recovery return-ID collections remain explicit singleton boundary adapters. Owner membership slot_index, journal effect ordinal zero, result/retirement maps and P2/P3 obligations remain intentional later-package scope. P1 removes the duplicate ObjectID stored in OutputSlotManifest when replacing it with OutputValue; it does not remove owner membership slot_index. No second GCS publication backend or task success authority is introduced.

## Serialization measurements and limits

There is **no final-production P1 pickle-size or latency measurement** in this package. The existing boundary-experiment-results.json contains real earlier prototype measurements; they are retained as historical exploratory evidence, not relabeled as final runtime savings:

| Child metadata count | Prepared old → prototype pickle bytes | Envelope old → prototype pickle bytes |
|---:|---:|---:|
| 0 | 1,090 → 805 | 1,572 → 1,240 |
| 1 | 1,647 → 1,388 | 2,113 → 1,781 |
| 2 | 1,895 → 1,636 | 2,361 → 2,029 |

The source is audit/two-version-cleanup/k4-structure-staging/boundary-experiment-results.json; its SHA256 and complete recorded object/field/container metrics are embedded in structural-cost.json. That experiment used actual dataclass construction and pickle but different prototype class/module names, prepared bytes and child metadata. It did not execute real ObjectRef borrowing, user serialization, network, resources, owner CAS or GC. Therefore its byte counts cannot be claimed as final wire, control payload or performance reductions. Production bytes, end-to-end latency and future source budget remain unmeasured/low-confidence.
