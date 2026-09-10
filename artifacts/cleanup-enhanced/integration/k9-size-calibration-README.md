# K9 formal E worktree size calibration

Measured fixed E ce29981a547f83b53b0c1df9f91354dcf89d8e4f, accepted B delivery f9a9b35015f114afda9c87e653b6fedcda2eb0b2, and the current uncommitted mini-ray-enhanced worktree. E Git HEAD is still its B fork; it is **not an E tested HEAD**. Runtime source hashes remained unchanged during measurement; later test-fixture edits require a new test snapshot.

## Same metric, actual source

Reused audit/two-version-cleanup/build_file_coverage.py code_count: physical raw.splitlines; Python token-bearing physical lines exclude AST docstrings, comments and layout. Primary source Python excludes golden JSON. Multiline literals count; this is neither logical statements nor necessary/deletable LOC.

| Scope | Fixed E files / physical / code | Accepted B files / physical / code | Current E files / physical / code |
|---|---:|---:|---:|
| source_python | 59 / 61,440 / 50,277 | 56 / 57,995 / 47,449 | 59 / 60,019 / 49,138 |
| source_non_python | 2 / 820 / 0 | 2 / 604 / 0 | 2 / 820 / 0 |
| source_all | 61 / 62,260 / 50,277 | 58 / 58,599 / 47,449 | 61 / 60,839 / 49,138 |
| tests | 348 / 133,422 / 112,419 | 329 / 115,549 / 96,828 | 336 / 118,755 / 99,593 |
| documents | 13 / 10,498 / 0 | 18 / 123,642 / 0 | 19 / 123,765 / 0 |
| history | 48 / 33,713 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |
| tooling | 11 / 2,107 / 789 | 10 / 26,211 / 659 | 10 / 27,246 / 659 |
| artifacts | 126 / 4,744 / 0 | 1,190 / 162,698 / 0 | 1,252 / 165,251 / 0 |
| examples | 7 / 562 / 0 | 7 / 557 / 0 | 7 / 557 / 0 |

**E Python source: 59 files, 60,019 physical / 49,138 token-code lines.** Relative to fixed E: -1,421 physical / -1,139 code. Relative to accepted B: +2,024 physical / +1,689 code. These are measured values, not projections.

There are 16 meaningful B-to-E source paths: 15 Python files and one golden JSON. The two source-data JSON files contain 820 physical lines, separately from Python; B has 604, so E adds 216 golden lines. Source-all is 60,839 physical lines and must not be substituted for the Python-only denominator.

## Runtime concentration

| Current E source | Physical | Token code |
|---|---:|---:|
| src/miniray/core.py | 12,965 | 10,972 |
| src/miniray/node.py | 7,340 | 6,345 |
| src/miniray/protocol.py | 6,658 | 5,515 |
| src/miniray/ownership.py | 4,132 | 3,292 |
| src/miniray/control.py | 3,781 | 3,149 |
| src/miniray/api.py | 3,104 | 2,577 |
| src/miniray/worker.py | 2,202 | 1,847 |
| src/miniray/transport.py | 1,205 | 1,000 |
| src/miniray/foreign_lineage_runtime.py | 1,025 | 859 |
| src/miniray/enhanced_publication.py | 996 | 795 |

The E increment is the two custom guarantees and their real consumers: GCS publication facts, reserved/committed graph validation, typed proofs, Node/owner C0-C7 transitions, death/retirement work and trace differences. Shared B mechanisms are inherited; E is not a third runtime or a promise of production-Ray completeness.

## Raw identity and evidence boundary

- All src raw-tree SHA256: 93c6600e89ab5b5d4a777c7f255a8ed2a0818ab6cf143b6021d9ad29db2adb0b
- Python src raw-tree SHA256: 7f0eefeae18267748ffe1e48244d8d1f75b62ccabd8200a5be022bc28e227b88
- Algorithm: SHA256 of sorted JSON path-to-raw-file-SHA256 mapping, UTF-8, separators comma/colon, no whitespace. See raw-source-hashes.json.

Current tests/docs/tooling/artifacts are independent categories. Evidence growth, inherited historical logs and verbose registry/machine-plan JSON are not runtime lines and do not count as cleanup savings. The earlier 30-file pure392/37-smoke trial result is not proof of this uncommitted worktree or the newly added post-C5 case; no pass count is assigned here. No project source/test was modified, imported or executed, and no pytest/collection ran.

A 20k/25k budget remains unproven: measured current E is 49,138 code lines, and its shared base is 47,449. Removing documentation or compressing evidence formatting cannot close the runtime gap. The table calibrates actual two-branch sizes; final E commit/testing/installation evidence must be recorded independently by root.
