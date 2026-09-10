# K7 CI requirement review and narrow candidate

The actual workflow is `.github/workflows/baseline.yml`; `test.yml` does not
exist in the inspected checkout. This preserves the plan's required path.
The candidate modifies only that workflow and its existing private tool
`scripts/_ci_baseline.py`. No base file, manifest, lock, dependency, runtime
source or test has been changed.

Final patch SHA-256:
`cfb9dc8e8c05ccc3fd5507c311bc6a64ae38f511266864985b03347aa93ebe9c`.

## Already implemented versus remaining evidence gaps

| Plan section 6 requirement | Current evidence / disposition |
|---|---|
| Neutral name and manual dispatch | `Teaching branch baseline`, `workflow_dispatch` present; preserve. GitHub only exposes manual workflow dispatch when the workflow exists on the repository's default branch, an external deployment condition not verified here. |
| Push ref / PR target / selected manual ref choose B/E | Branch filters plus independent job `if` expressions implement this. A manual main/tag run selects neither job; main is not a third edition. |
| B does not wait for E | Separate jobs, no `needs`, no matrix and no other-branch fetch. An absent E branch before K8 does not block B. |
| Full candidate commit checkout | Both jobs checkout `${{ github.sha }}`. PR event SHA is the merge-preview commit; no substitution of PR source HEAD is made. |
| Own manifest and independent evidence | `_load_manifest()` from the checked-out branch; edition checked against event target; `baseline-base-*` and `baseline-enhanced-*` artifact names. |
| Event/source identity fields | Current private runner already writes event/ref/SHA, PR branches, candidate SHA, actual checkout/tested SHA, edition-manifest hash and dispatch ref. Preserve the existing candidate field's PR-source meaning and make it explicit. |
| Source identity if install fails | Gap: current source-identity file is written only when the gate step runs. Candidate adds an identity-only step before dependency installation; it does not invoke the bounded gate. |
| Lock/build/runner/source identities | Current workflow copies lock/manifest and records git HEAD; only manifest has an explicit digest. Candidate additionally hashes lock, build config, workflow, runner tools and all source Python files. |
| Actual environment versions | Already records git HEAD, `uname -a`, Python patch, uv and installed package versions. Preserve Python 3.12.13, uv 0.11.26, Ubuntu 24.04 and frozen sync. |
| Pure plus smoke serial and bounded | Current private runner executes one pure batch then exact manifest smokes serially; child owns 30-second deadline/cleanup. No migration registry sweep or extra timeout is added. |
| Remote run / final branch HEAD identity | Not verified. No CI run is claimed; K9 still must record both actual branch acceptance mappings. PR preview success is not branch HEAD execution. |

## Candidate behavior

Each independent job gains one standard-library identity-recording step after
tool setup and before `uv sync`. This calls the existing private CI module
with `identity_only=True`; it loads/validates the current manifest and writes
metadata, but never runs pytest or creates a runtime participant. Its
`tested_source_commit` is null and `identity_record_only` true. If install
fails, the already-existing `always()` artifact step can still preserve the
event and checkout identity.

The actual gate invocation uses the existing default `main()` path and
re-records identity with its checkout SHA. Exit codes in `results.json`, not
the identity file alone, establish which selections ran and passed.

Additional metadata explicitly separates:

- `candidate_sha`: PR source commit for PRs, event branch commit otherwise,
  retaining existing tool/test semantics.
- `candidate_sha_kind`, PR source SHA, target SHA and merge-preview SHA.
- `checked_out_sha` and observational `checkout_matches_event_sha`.
- `candidate_target_branch_ref`, selected dispatch ref and event-time branch
  HEAD for non-PR events.
- `current_branch_head_after_run: null`: no fake current HEAD resolution.
- Run ID/attempt and exact lock/build/manifest/workflow/runner/source hashes.

The manifest digest remains its existing field. A source-tree digest is over
the sorted `(path, SHA256)` list, and the individual source hashes are retained
for audit. Per-selection elapsed time is now recorded without changing the
subprocess call, ordering, fail-fast behavior, deadline or cleanup owner.
The source scan occurs once in the pre-install identity invocation and once
at gate start; it is outside the selection loop and is never repeated for
each smoke. Per-test exit records reuse the recorded checkout identity.

This adds no approval flow, additional test gate, remote comparison job,
dependency pin, cache policy or arbitrary dispatch selector. The existing
manual dispatch runs the selected branch's delivery gate. Plan-permitted
migration cases remain separately registered and are not silently run by CI.

## Static validation

- YAML 1.2 parsing succeeded with strict and unique-key options. The already
  installed Red Hat `vscode-yaml` 1.24.0 bundle supplied its YAML parser module
  only; the language-server entry was not started, no server/schema/network
  access occurred, and no parser dependency was added to the project. Exact
  parser bundle hash and script are recorded in `yaml-validation.json`.
- Parsed workflow checks confirm two jobs, original event branches/manual
  dispatch, SHA checkout, exact Python/uv/OS pins, no `needs`/matrix, separate
  artifact names, and identity recording before sync.
- Modified Python parses with AST. The original serial selection loop still
  has one `subprocess.run` call and no new timeout.
- An actual identity-only invocation against temporary fake PR/checkout
  metadata passed. It asserted null `tested_source_commit`, distinct source
  and merge SHAs, source hashes, and no subprocess/test launch. Git identity
  and manifest loading were explicit stubs; no real CI or tests ran.
- The patch includes the correct no-final-newline marker for the original
  private tool. `git apply --check --ignore-space-change` passes against the
  untouched raw-CRLF baseline; normalized-LF source can use the normal patch.
- Existing `test_baseline_runner.py` CI identity assertions remain compatible:
  default `main()` retains candidate/source/checkout fields and the same
  launch sequence. No tests were edited or executed for this preparation.

`ci-identity.patch` is the two-path application patch. `static-review.json`
binds exact before/after and patch hashes. Base inputs must match those
before-hashes, or be semantically rebased, before application. The helper is
private tool configuration; it does not select a B/E runtime implementation.

No pytest, outer network, workflow dispatch, Git mutation or remote CI run
was performed. YAML parsing is syntax/structure evidence, not a GitHub Actions
execution or proof that the workflow has been published to the default branch.
