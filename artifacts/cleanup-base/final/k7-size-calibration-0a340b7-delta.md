# K7 additive size update: 0a340b7

The original 54d1bca calibration is unchanged. This delta compares exact Git blobs using the same established physical/token metric.

| Scope | Physical delta | Token-code delta |
|---|---:|---:|
| Source (core.py) | +18 | +12 |
| Tests (new L1 file) | +174 | +144 |

Updated Python source: **57,995 physical / 47,449 token-code lines**; source-all including the unchanged golden JSON: **58,599 physical**.

Relative to fixed B691: -1,391 physical / -1,106 token-code lines. The new evidence/test case is separate from runtime size. The new file has one loopback function and two parameter values; no new test or acceptance run occurred in this statistics task.

Exact Git blobs measure +18 physical and +12 token-code source lines, matching the root estimate; textual diff hunk counts can differ with mixed line endings. Shared-tail correctness work is not counted as structural cleanup savings. Final B/E acceptance remains recorded separately by root.

Commits: 54d1bca489b999d57e71fcc293973f3324b204a3 -> 0a340b792c89667e493f7e7313935e45e29071bf.
