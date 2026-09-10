# Final B/E size calibration

Uses the exact `code_count` function from `audit/two-version-cleanup/build_file_coverage.py`. Physical lines are raw splitlines; code lines are token-bearing Python physical lines excluding docstrings/comments/layout. Code is counted only for Python files under src/tests/scripts, matching the original invocation.

Current scope is **tracked working-tree bytes**, including tracked uncommitted docs and excluding untracked files; it is not silently relabeled a HEAD-only snapshot.

| Category | Fixed B files / physical / code | Current B files / physical / code | Fixed E files / physical / code | Current E files / physical / code |
|---|---:|---:|---:|---:|
| source_python | 56 / 59,386 / 48,555 | 56 / 57,995 / 47,449 | 59 / 61,440 / 50,277 | 59 / 60,019 / 49,138 |
| source_non_python | 2 / 604 / 0 | 2 / 604 / 0 | 2 / 820 / 0 | 2 / 820 / 0 |
| source_all | 58 / 59,990 / 48,555 | 58 / 58,599 / 47,449 | 61 / 62,260 / 50,277 | 61 / 60,839 / 49,138 |
| tests | 340 / 130,975 / 110,374 | 329 / 115,549 / 96,828 | 348 / 133,422 / 112,419 | 336 / 119,476 / 100,248 |
| scripts | 5 / 1,527 / 784 | 4 / 25,580 / 659 | 5 / 1,542 / 789 | 4 / 26,618 / 659 |
| docs_history | 48 / 33,713 / 0 | 0 / 0 / 0 | 48 / 33,713 / 0 | 0 / 0 / 0 |
| ordinary_docs | 12 / 10,367 / 0 | 18 / 123,642 / 0 | 13 / 10,498 / 0 | 19 / 123,767 / 0 |
| artifacts | 65 / 2,440 / 0 | 1,190 / 162,698 / 0 | 126 / 4,744 / 0 | 1,373 / 211,723 / 0 |
| examples | 7 / 557 / 0 | 7 / 557 / 0 | 7 / 562 / 0 | 7 / 557 / 0 |
| other_tracked | 6 / 565 / 0 | 6 / 631 / 0 | 6 / 565 / 0 | 6 / 631 / 0 |

## Source-only comparison

- base_vs_fixed: +0 Python files, -1,391 physical lines, -1,106 code lines.
- enhanced_vs_fixed: +0 Python files, -1,421 physical lines, -1,139 code lines.
- enhanced_over_base: +3 Python files, +2,024 physical lines, +1,689 code lines.

## Inputs and interpretation

- Fixed B: `69106772567a4131f5ec76e898a3c4bf3bb6dbe6`.
- Fixed E: `ce29981a547f83b53b0c1df9f91354dcf89d8e4f`.
- Current B HEAD: `f9a9b35015f114afda9c87e653b6fedcda2eb0b2`; tracked dirty bytes included: True.
- Current E HEAD: `3f5b725fb26390b86c78f085486fb73d897d3e42`; tracked dirty bytes included: False.
- Docs/history removals and acceptance-artifact growth are not runtime savings. `0` code lines in non-counted categories means not applicable under this metric. Binary artifact physical lines are mechanically raw splitlines, not executable LOC.
- Per-file raw hashes and compact count records are in `k9-final-size-calibration-hashes.json`; the summary JSON binds its SHA and each input tree.
- No tests/project imports were run and no formal repository was modified.
