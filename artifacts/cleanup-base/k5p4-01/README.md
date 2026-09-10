# Composite P4 integration patch

**Use after P3 acceptance.** This package combines the three existing P4 typed-work changes and the store-full observer correction. It contains normalized a/src and a/tests paths; no manual input/candidate prefix adjustment is required.

- Patch: p4-composite.patch, 36,729 bytes.
- SHA256: 06216a1c9fee5fe50400e5df5c476921576a0dd4b6de8404ea51757f35b5f5cb.
- Baseline: frozen base-p3trial-01 archive eb1c4987f79ce3d123889d056b956a9ed6165de20243b44093e7996d73aa83d4.
- Expected post-state: frozen base-p4trial-02 archive cfc3730b2d2b595e39e623e698275b707b0c89ee9e7881733a820d633d7c6fb5.
- Exactly seven changed paths: core.py, new put_work.py, and five existing test files. Manifest, runner, dependencies and P6 are excluded.

The patch was generated from verified archive members, normalizing CRLF on both sides before diffing. before/ and post/ retain original raw bytes. Every post file exactly matches the frozen P4 trial02 member. file-hashes.json records all before/post raw/LF hashes and post AST fingerprints. summary.json records the three source-patch hashes plus the exact observer fix.

The after-P3 application check passed. At packaging time, the current branch had not yet applied P3; its application check therefore failed at the Core retirement and targeted_owner_defer hunks, as recorded without suppression in summary.json. Root must apply/review P3 first, then recheck this patch. This is not a reason to rewrite the patch against pre-P3 APIs or copy an old Core over current work.

Only four Core methods change: _put_value, _drive_put_handoff_cleanup, _drive_output_node_loss_once and _retire_lost_output_memberships. Every other Core method remains AST-identical to the after-P3 input. All other source files, including P3 ownership, P1 TaskExecution and P2 journal, retain their original bytes. Later P2 effect/snapshot changes outside these methods must be preserved by applying the narrow hunks, never by replacing full Core files from this package.

The new records retain actual request intent, typed response/death proofs and source custody; they create no authority or service. Five tests retain their names/decorators and actual receipt assertions. The observer fix distinguishes the initial pending store-full rejection from exactly one late Seal after GC, requiring absence of original work and retaining both actual absence-fenced replies. The late negative remains.

No runtime validation was performed during packaging. Root reports P4 trial02 pure 343 passed / 1 deselected; trial01's Node-loss/defer and contained runtime checks refer to the same source bytes, with only the corrected test observation differing. The original execution directories remain the validation authority; this package neither rewrites results nor declares the entire two-branch cleanup complete. Root must refresh its current branch's reviewed closure after application, preserving the existing gate and selectors.
