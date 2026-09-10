# K7 size calibration

Fixed B: 69106772567a4131f5ec76e898a3c4bf3bb6dbe6. Current Git: 54d1bca489b999d57e71fcc293973f3324b204a3. A separate worktree observation includes the four current-document edits; no final tested HEAD is inferred. This is statistics only: no project imports, tests, or base writes.

## Metric and scope

Reused the original code_count function and categories from audit/two-version-cleanup/build_file_coverage.py. Physical lines use raw bytes splitlines. Token-code lines contain non-comment/non-layout Python tokens excluding AST docstrings; multiline literals count. Code is counted only in src/tests/scripts Python files. These are not necessary/deletable LOC or logical statement counts.

**Primary runtime comparison is Python source only**, matching the previous 59,386 physical-line denominator. Source-all is shown separately because it includes 604 unchanged golden JSON lines. Examples retain the original physical-only category, so code=0 means not measured by this metric, not no code.

| Category | Fixed B files / physical / code | Current files / physical / code | Physical delta | Code delta |
|---|---:|---:|---:|---:|
| source Python only | 56 / 59,386 / 48,555 | 56 / 57,977 / 47,437 | -1,409 | -1,118 |
| source all (includes JSON) | 58 / 59,990 / 48,555 | 58 / 58,581 / 47,437 | -1,409 | -1,118 |
| tests | 340 / 130,975 / 110,374 | 328 / 115,375 / 96,684 | -15,600 | -13,690 |
| documents | 12 / 10,367 / 0 | 18 / 123,654 / 0 | +113,287 | +0 |
| history | 48 / 33,713 / 0 | 0 / 0 / 0 | -33,713 | +0 |
| evidence | 65 / 2,440 / 0 | 1,108 / 121,120 / 0 | +118,680 | +0 |
| tooling | 11 / 2,092 / 784 | 10 / 26,137 / 659 | +24,045 | -125 |
| examples | 7 / 557 / 0 | 7 / 557 / 0 | +0 | +0 |

## Why the source remains near 48k code lines

Python source changes from 59,386 physical / 48,555 token-code lines to 57,977 / 47,437: -1,409 physical (-2.37%) and -1,118 code (-2.30%). Near 48k refers to code-bearing lines, not physical lines. Documentation deletion and test reduction are not credited to this runtime denominator.

| Current source | Physical | Token code |
|---|---:|---:|
| src/miniray/core.py | 12,788 | 10,812 |
| src/miniray/node.py | 7,256 | 6,273 |
| src/miniray/protocol.py | 6,654 | 5,511 |
| src/miniray/ownership.py | 4,132 | 3,292 |
| src/miniray/control.py | 3,722 | 3,095 |
| src/miniray/api.py | 3,103 | 2,576 |
| src/miniray/worker.py | 2,198 | 1,843 |
| src/miniray/transport.py | 1,205 | 1,000 |
| src/miniray/foreign_lineage_runtime.py | 1,025 | 859 |
| src/miniray/reconstruction_runtime.py | 984 | 769 |

Core, Node, protocol, ownership, control, API and Worker still contain 39,853 physical / 33,402 code lines, 70.4% of source code. They retain submission/lease/direct push, deeply validated typed wire values, owner/reference/replica custody, recovery/shutdown, membership and Actor/PG coordination. The cleanup removed unused interfaces and redundant singleton dimensions; it did not authorize removal of those supported mechanisms. This inventory neither proves all remaining lines necessary nor labels them redundant.

The sole complete source-module retirement relative to fixed B is runtime_state.py (-401 physical/-287 code); put_work.py adds 75/49. Main within-file decreases are Core -210/-173, Node -204/-176, ownership -188/-164 and publication DTO -72/-65. Enhanced-only ordinary-publication/global-graph modules were never in this fixed B denominator, so their absence cannot count as B cleanup savings.

## Scoped P1-P6 net changes

| Package/subitem | Physical delta | Code delta | Disposition |
|---|---:|---:|---|
| P1 | -183 | -141 | applied |
| P2 journal | -7 | -2 | applied |
| P2 effect | -33 | -30 | applied |
| P2 snapshot | -7 | -7 | applied |
| P4 | 110 | 77 | applied |
| P5 | 24 | 19 | applied |
| P3 membership/retirement | -30 | not isolated | applied |
| P3 duplicate prevalidation | -1 | -1 | applied |
| P2 progress merge | 0 | not isolated | retained current design; trial not applied |
| P6 domain extraction | 0 | not isolated | retained current design; trial not applied |

These deltas use exact isolated before/after trees or saved cost records. Their scopes can overlap intermediate baselines; they must not be summed as an explanation of the full cleanup delta. P4 and the narrow ACK add a few lines for explicit fields/boundaries. Rejected progress/P6 experiments contribute zero applied delta. The P3 one-line redundant prevalidation removal leaves final owner commit validation intact.

## Evidence, documents, tooling and budget

The 48 history working copies removed 33,713 lines of archived material, not runtime. Current document growth is dominated by project-cleanup-plan.json (92,667 lines), retirement-inventory.json (25,194), and history-index.json (2,454). Those detailed machine records were requested and remain evidence; possible formatting compression is a separate later action, not deletion of facts or runtime savings. Tooling growth is mainly baseline_manifest.json (24,664 lines of reviewed input bindings); measured tooling Python code actually falls 784 to 659.

New evidence contains 1,043 files, 118,680 physical lines and 7,888,328 bytes: snapshot hashes, logs, failed attempts, migration decisions and patches. It is separate from source; it is neither runtime complexity nor source savings. See JSON new_evidence.by_prefix for directory contributions.

Confidence in a 20k/25k target remains low. Reaching 20k from 47,437 code lines would require another 57.8% reduction; 25k requires 47.3%. The measured 2.30% reduction does not establish such a semantics-preserving design. No deletable-line promise follows from the count.

Git archives and Windows checkout bytes differ through CRLF/LF handling. HEAD and measured worktree agree on Python source physical/code totals. Four documentation edits were pending at the initial collection; a later shared-tail fix may add lines and must be recorded as a new precise delta rather than relabeling this 54d1bca observation. Final testing/HEAD acceptance remains a separate root record.
