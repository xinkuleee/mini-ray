# P6 evaluation rebased onto frozen P1 trial 02

Prepared only. P4's final disposition is pending, so this is not permission
to adopt P6 or apply it to B/E. No pytest, test execution, WSL, commit, branch
creation or source-checkout mutation occurred.

- Evaluation root: `audit/assessment-p6-evaluation`.
- Exact input: `execution/base-p1trial-02/project.tar`, SHA-256
  `332d78b7dcedc67da3c1db3b384f5ee915b69b4516c411e4ac25a196f2024f73`.
- Frozen output: `execution/base-p6trial-01/project.tar`, SHA-256
  `fb6cd120fd05e410b88e31fb4835e09b55109e85c59a4270e02904af6ded320f`.
- Semantic patch: `p1-to-p6-rebase.patch`, SHA-256
  `d22cf6b774f4df7083b0860b45d6b41676f8cdbbfb0df5dc96ff35a0307bc16c`.

The assembly copied exactly the 419 frozen execution inputs after verifying
every archive member against `snapshot.json`. No complete history/artifact
tree, bytecode cache, Git directory or original P1 tool directory was copied.
The frozen output contains those inputs plus `task_submissions.py`: 420 files.
This `_assessment` directory is not an execution input and is excluded.

## Semantic rebase and preserved P1

The original P6 patch used old `TaskExecutionKey` import context. Instead of
overwriting P1's Core, assembly checked that the P6 input and P1trial02
`_register_submission` and `_ForeignDependencyGuard` ASTs were identical.
It moved the guard to the candidate's single `TaskDependencyGuard` type,
installed the candidate submission method, and retained P1's `TaskExecution`
import and scalar result/execution forms. The 234 other Core methods are
AST-identical to the frozen P1 source. The new domain is byte-identical to the
original P6 candidate and accepts narrow custody capabilities, never Core.

Exactly four semantic paths changed before registry refresh:

1. `src/miniray/core.py`: input preparation/abort extraction and type import.
2. `src/miniray/task_submissions.py`: original P6 narrow per-call input domain.
3. `tests/unit/test_explicit_put_arguments.py`: existing encoder fault-injection
   target follows the moved encoder.
4. `tests/unit/test_core_placement_group_scheduling.py`: same binding-only change;
   its original heavy test remains heavy and unregistered.

All other copied P1 source/test/tool/doc inputs are byte-identical. Only the
evaluation manifest is additionally refreshed. The semantic patch normalizes
CRLF to LF for comparison; raw before/after hashes remain explicit.

## Independent manifest and finite review

The root-specific registration helper was copied into this directory and
changed only to point at this non-Git evaluation root and its recorded origin.
The original `audit/p1-evaluation-tools` helper was not overwritten. The
current runner module was imported for static closure hashing and schema
validation only; no test modules or runtime classes were imported/executed.

All 311 existing migration registrations retain selectors, markers, package,
purpose and cost text. Only `reviewed_files`, `reviewed_tree_hash` and
`review_source_commit` were refreshed. The delivery gate remains 26 pure
files and 32 smoke selectors.

The requested core-nested selectors were absent from the input manifest.
Following root authorization and explicit review, four existing unit
functions were registered under `P6-vertical-evaluation`, giving 315 reviewed
migrations:

- `tests/unit/test_core_nested_task_arguments.py::test_nested_local_ref_is_held_but_never_gates_readiness`
- `tests/unit/test_core_nested_task_arguments.py::test_nested_hold_survives_system_retry_and_releases_once`
- `tests/unit/test_core_nested_task_arguments.py::test_ambiguous_foreign_nested_retain_is_recorded_before_rpc`
- `tests/unit/test_core_nested_task_arguments.py::test_top_level_and_nested_foreign_handles_share_one_logical_hold`

Each uses the existing `_no_pure_runtime` fixture: at most two threadless
Cores, two logical Tasks and four small handles; no user function execution,
Node, Worker, process, socket, thread, timer, sleep or blocking wait. Owner
Retain/Release handlers and owner/recovery/lineage tables are real. Local
receipt waits require an already-set Event. FIFO sizes are capped at 16 and
the existing mailbox drain at 128 events. The original fault/retry scenarios
and all test bodies are unchanged. The two live attempt-borrow heavy
functions remain excluded. The fixture uses explicit owner-table borrower
tokens; this is input-custody composition evidence, not full Acquire/bytes
transport evidence.

## Root's initial execution selection

Use the unchanged frozen runner and cache environment policy. No direct pytest
invocation or unregistered broad mixed-file expansion is needed.

1. Existing `--pure` delivery gate, including the ordinary explicit-input and
   nested-input budget rollback cases.
2. Existing ordinary exact case:
   `tests/unit/test_spillback_runtime.py::test_core_preserves_identity_and_does_not_release_from_submitter`.
3. The four registered nested cases above via separate exact `--case` calls.
4. Existing whole-unit registration `tests/unit/test_single_output_submission_rollback.py`
   via `--case`; this retains the current registration rather than adding
   overlapping exact selectors. Its enqueue failure and input/lineage rollback
   cases cover the candidate's final admission/abort boundary.

If proceeding to broader promotion assessment, the original registered
contained reconstruction and adoption-ACK process selectors remain available:

- `tests/integration/test_task_contained_reconstruction_path.py::test_foreign_task_outer_renews_imports_replaces_edges_and_collects`
- `tests/integration/test_output_retirement_ack_path.py::test_lost_actual_adoption_ack_replays_retirement_without_reexecution`

## Benefit assessment remains open

Relative to accepted P1 input, Core is 249 physical lines shorter and the new
module adds 246: net runtime **−3 lines** (original pre-P1 P6 trial reported
−5). Each changed test adds one import line. The extraction still introduces
seven custody capability fields, two Protocol types and per-call domain/DTO
objects. It does not move finish/drain authority or eliminate long-lived
pending tables. The original candidate's evidence-backed recommendation was
not to adopt this small slice absent further demonstrated benefit.

Passing the finite ordinary/contained cases would show viability, not resolve
that tradeoff or satisfy P4. Root should record implementation or justified
retain/revert only after the prerequisite disposition and net-benefit review.

Static evidence: all execution Python files parsed, new domain has no Core
import or `getattr`, four-path semantic diff plus independent manifest delta,
234 unchanged Core methods, preserved P1 DTO spelling, exact archive/input
hashes, unchanged gate and unchanged existing selector metadata. Runtime
results are intentionally absent.
