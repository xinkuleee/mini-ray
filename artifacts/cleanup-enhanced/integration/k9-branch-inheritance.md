# K9 branch and source-inheritance check

Read-only audit of actual local refs and current source/artifact bytes. No
tests, runtime acceptance review, remote lookup, mutation or commit occurred.
Detailed path hashes and checks are in k9-branch-inheritance.json.

## Actual branch identities

| Identity | Observed value |
|---|---|
| refs/heads/teaching-base | f9a9b35015f114afda9c87e653b6fedcda2eb0b2 |
| refs/heads/teaching-enhanced | f9a9b35015f114afda9c87e653b6fedcda2eb0b2 |
| B tested source | 0a340b792c89667e493f7e7313935e45e29071bf |
| E fork / merge-base | f9a9b35015f114afda9c87e653b6fedcda2eb0b2 |
| main | ce29981a547f83b53b0c1df9f91354dcf89d8e4f |

Both required refs exist. E is a separate worktree/branch rooted in B's
accepted delivery HEAD. E has uncommitted runtime/test/document additions;
its HEAD still equals the fork commit. **There is no E implementation commit
or E commit acceptance mapping yet.** Current E evidence belongs to its
frozen working-candidate identity, not a fictional new branch HEAD.

B is clean. Comparing tested0a340b7 to deliveryf9a9b35 finds only acceptance
evidence and documentation changes; no src/tests/scripts/examples/workflow,
lock, build or conftest input changed. The tested source is an ancestor of
the exact E fork. main remains the old enhanced checkpoint, not a third
cleaned runtime. No push or remote CI success is implied.

## Shared source and enhancement accounting

Current B/E source union has61 files/assets: **45 inherited unchanged after
LF normalization and16 differing paths**, all accounted. Raw and normalized
hashes are recorded separately. No unaccounted runtime path was found.

The16 current differences are API, control, Core, three E-only publication
modules, E ordinary-task golden trace, Node, output_protocol, journal, Node
adapter, owner_service, protocol, publication_gate, put_work and Worker.

The original fixed B→E inventory also had16 source paths, but its ownership.py
delta was entirely common CF002 and is now inherited unchanged. The current
typed put_work.py instead needs E publication/abort/full-death fields. Thus
the current16 are the original16 minus already-inherited ownership.py plus
the accepted B typed-work extension. Mixed Core/control methods retain CF
fixes and add only E publication/graph responsibilities; no old Core/Node
whole-file restoration is being treated as inheritance.

## Common late-adoption tail fix

Source commit0a340b7 changed Core._drive_output_publication_adoption so an
old successful adoption ACK cannot clear an active same-attempt Node-loss
cleanup marker or discard its bytes. A just-installed Node death transfers
the exact obligation outside the lock; an already-obsolete completed takeover
remains terminal.

The complete tail AST—from takeover initialization through return—is
**identical in commit0a340b7, current B, and current E**. E's additional C5/C7
operations before this shared tail do not replace it. JSON records B/E line
anchors, AST digest, and the earlier enhanced-inheritance patch/test ledger.
The E equivalent commit is null because this is still uncommitted working
source. Its inherited common L1 test is E-adapted only to route real GCS
messages and check the actual C7 receipt; root owns its runtime result.

## Fixed tags and historical artifact preservation

- teaching-base-v0.1 still resolves to
  69106772567a4131f5ec76e898a3c4bf3bb6dbe6.
- teaching-enhanced-v0.2 still resolves to
  ce29981a547f83b53b0c1df9f91354dcf89d8e4f.
- All65 B stage1 artifacts retain their original Windows-checkout raw bytes
  in B and inherited E. Their LF-normalized content matches the original Git
  blobs. They must not be described as raw-LF-identical to those blobs.
- All61 newly imported E stage2 artifacts are **raw-byte identical** to
  git cat-file blob output from fixed ce29981. They differ from the old
  audit archive's CRLF checkout hashes solely because they preserve Git's
  original LF bytes. This is not an artifact content defect.

No historical artifact content mismatch was found; none was rewritten by
this audit. The JSON retains both comparisons rather than normalizing away
the raw-byte provenance distinction.

## Frozen candidate and bounded static follow-up

Formal E snapshot enhanced-k8-01 is bound to archive
07434e70486d0ac497348211bfebd7a0559c3b7c1b717f6b3511a78ad63df3ea,433 files.
Its source raw hashes matched current E at audit. Results were not reviewed
or relabeled; later test-only fixes are outside this source-inheritance check.

As specifically requested, an AST-only scan covered338 migration registrations
and their explicit test/helper closure (174 Python paths):

- Missing required E NodeAdapter callback keywords: **0**.
- Old TaskExecutionKey/TaskOutputManifest/OutputSlotManifest names: **0**.
- Suspicious old positional effect/materialization call arities: **0**.

This targeted scan does not prove all338 migrations execute successfully and
does not expand the test gate. B/E manifests independently identify base
26/32/338 and enhanced30/37/338.

No existing inheritance problem was found. The remaining identity action is
the already-planned E source commit/final acceptance mapping after root's
actual validation; this audit adds no new scope or delivery requirement.
