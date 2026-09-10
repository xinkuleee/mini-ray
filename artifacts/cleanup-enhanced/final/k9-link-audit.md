# K9 local documentation link audit

Checked B delivery HEAD f9a9b35015f114afda9c87e653b6fedcda2eb0b2 and E source
HEAD dbc504c0cbc69dcdc4b64128b72b6f852bdc34fa. No network, tests or source
changes occurred. Detailed occurrences and document hashes are in JSON.

The requested README/design/learning/testing/current-status/acceptance/history
navigation pages have **no missing local link targets or anchor failures**.
The independent focused check covered278 relative-link occurrences across
15 pages.

The broader current README plus all top-level Markdown docs scan covered
15 B files and16 E files. B has752 valid local and223 unverified remote
link occurrences; E has778 valid local and223 unverified remote occurrences.
It found35 missing local occurrences per branch, all confined to
project-cleanup-index.md and retirement-inventory.md and all pointing at
13 deliberately retired test/helper paths. No fragment mismatch was found.

Two staged branch-specific patches replace those dead local links with
fixed6910677 Git retrieval URLs. Every target was checked using local
git cat-file; remote hosting/published branch availability was not checked.
The original retirement decisions and historical artifact contents remain
unchanged. This does not restore retired files or claim remote verification.

The patches also correct only three stale current-navigation statements:
B learning page still said K7 ongoing/E absent; B status said E not created;
E's current addendum to historical B acceptance said the branch map was still
to be established. The proposed wording says E has been created and points
to its own status without claiming completed E acceptance. Historical
acceptance body text remains untouched.

- B patch: audit/k9-link-docpatch/base.patch, SHA256
  61843ce751ffcb9a6a465a0ef3b6215b6a28c3a8e6af4329e42613ef5c14fcdb.
- E patch: audit/k9-link-docpatch/enhanced.patch, SHA256
  20d120095dac7bde14c27328fbf0ea60c77b2ff95ee28fe2b67c8683512757aa.

Both patches pass git apply --check --ignore-space-change against their
current worktrees. Seven doc paths total are affected, with no runtime,
test, manifest, lock, artifact or main README change. Root may apply the
documentation patches as evidence-only changes; main's append-only branch
navigation remains root's separate planned step.
