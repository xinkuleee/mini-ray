# K7 dependency and frozen-install readiness

Inspected current B at `2bdff69a2eec0365d3df0861954dfcb500fc5d48`. This is
preparation, not an install result or K7 acceptance. No dependencies, lock,
source or environments changed; no install, download, pytest or Linux command
ran during this review. Only the existing Windows uv version/help was invoked.

## Pins and saved runtime comparison

Python: pyproject `>=3.12,<3.13`, lock `==3.12.*`; reproducible workflow pin
**3.12.13**. uv pin **0.11.26**. Build isolation pins **setuptools 84.0.0,
wheel 0.48.0, packaging 26.3**. Build-only setuptools/wheel are not expected
in uv.lock's runtime package graph.

| Package | Current lock | Saved Linux execution |
|---|---|---|
| cloudpickle | 3.1.2 | 3.1.2 |
| pytest | 8.4.2 | 8.4.2 |
| packaging | 26.3 | 26.3 |
| pluggy | 1.6.0 | 1.6.0 |
| iniconfig | 2.3.0 | 2.3.0 |
| pygments | 2.21.0 | 2.21.0 |
| colorama | 0.4.6, Windows only | Absent correctly on Linux |

Saved `base-p5trial-01` and `base-preflight-01` environments report Python
3.12.13 and all six applicable locked dependencies above. The harness uses
PYTHONPATH to frozen source plus `linux-packages`; that is not evidence of a
newly built/installed mini-ray package or its isolated build requirements.

Current raw SHA-256:

- pyproject.toml: `57bf07b37a00e326f47cf5ee24904c7eaba59fe4c82381a74d868f8c62501292`
- uv.lock: `5c22361c7257f2b740cd3e9c72786b3d0cd1e230df7fc39e2d122e1d42c9879d`

The older stage1 install copy has different raw hashes but identical LF-
normalized bytes and parsed TOML. No dependency change warrants re-locking.
Its older source install does not verify current cleaned source packaging.

## Tools and offline artifacts available

- Workspace Linux uv archive:
  `audit/stage1-dependency-repro/uv-0.11.26-linux-musl.tar.gz`, SHA-256
  `62bf1a53501adf4083224b69b33737450ac516935f5a5e483e9dfaf2665084de`.
  Member `uv-x86_64-unknown-linux-musl/uv` has mode 0755 and SHA-256
  `b52da01f0a12ffb9ad8ee7b4dd07258e7ab64175714a542d96f43a1cbb64b5de`.
  Historical linux-00.log confirms 0.11.26; root must recheck the extracted
  binary. No standalone Linux uv was found in workspace files.
- Host binary `C:/Users/t-hdong/.local/bin/uv.exe` was actually queried:
  `uv 0.11.26 (396ef7ce4 2026-06-30 x86_64-pc-windows-msvc)`. SHA-256
  `deeaa21aac3e3e40b3fa00788208aa9a319cefbb3c2aa598cf580565a82ebc34`.
  It is not a Linux install substitute.
- Saved Linux Python/runtime archive: `audit/linux-validation-runtime-executable.tar`,
  SHA-256 `146439268a4588c18c6302b8d3a8fb467f73346af00c7e0f1cc480a14f93206e`.
  Manifest Python binary SHA-256:
  `8bfa8a72453d01d54f7b712a8304740e00ae1eefb33e81dc86c5e3a3ed16c79a`.
- `audit/stage1-dependency-repro/wheels` contains the six locked universal
  wheels plus setuptools84.0.0 and wheel0.48.0. Every wheel was re-hashed:
  six match current uv.lock; two build wheels match `offline-artifacts.json`.
- `audit/uv-cache` contains pypi cache metadata for those packages, but it is
  a Windows cache. Its presence is not proof of portable Linux cache readiness.

Build wheel SHA-256 values (not covered by runtime uv.lock):

- setuptools84: `51a52592b3b99e102b609654876bd65f19f999935166d1352678931132b0c670`
- wheel0.48: `3217dcc807155e45db462d7ef2431f5ddda0d7273b700d05a67b271ceb1287ab`

All required Linux distribution bytes are local. However, **strict offline
frozen sync is not yet proven for current source**. The historical
`offline-attempt-linux-02.log` failed on the locked pygments URL even with
`--offline --no-index --find-links` pointing at those wheels. The later
successful `linux-02.log` explicitly downloaded pygments and did not use
offline mode. That may have populated `/dev/shm/mini-ray-install-check/uv-cache`,
whose continued presence was not probed here. `/dev/shm` is volatile.

## Minimal root validation procedure

1. Freeze the final K7 source and extract only that source into a fresh
   disposable Linux project directory. Record the source archive SHA/commit
   and pyproject/lock hashes. Do not reuse the old stage1 source or editable
   wheel. Keep the project venv fresh.
2. Extract the verified Linux uv archive. Select the actual Python3.12.13
   binary and verify both versions. Preserve build isolation and lock pins.
3. Clear ambient PYTHONPATH, PYTHONHOME and VIRTUAL_ENV. Use an explicitly
   identified Linux uv cache, `UV_PYTHON_DOWNLOADS=never`, and offline mode.
4. Run the prepared commands below with captured exit codes/logs. They were
   not run by this review. Replace the uppercase path tokens with those
   verified locations; no network fallback is implicit.

```bash
UV --version
PYTHON --version
UV lock --check --offline --no-python-downloads --python PYTHON --cache-dir LINUX_CACHE --project K7_PROJECT
UV sync --frozen --extra test --offline --no-python-downloads --python PYTHON --cache-dir LINUX_CACHE --project K7_PROJECT
UV pip check --python K7_PROJECT/.venv/bin/python --offline --cache-dir LINUX_CACHE
UV pip freeze --python K7_PROJECT/.venv/bin/python --offline --cache-dir LINUX_CACHE
```

Known prior locations: `/dev/shm/mini-ray-cleanup-runtime/python/bin/python3.12`,
`/dev/shm/mini-ray-install-check/uv-x86_64-unknown-linux-musl/uv`, and
`/dev/shm/mini-ray-install-check/uv-cache`. Availability must be rechecked.

5. From outside the source directory, use the newly installed venv Python
   without PYTHONPATH to record sys.version/executable, distribution versions,
   mini-ray direct_url.json and an installed `import miniray` path. Do not
   call init or run tests. The editable metadata/import path must identify
   the new K7 project. Capture actual build logs: isolated build pins need
   not appear in runtime pip freeze. Verify source/lock hashes afterwards.

The historical 90-second install-command cap can be reused with recorded
exit codes; this is separate from unchanged five-/30-second runtime limits.
Do not disable build isolation, use `--no-install-project`, or substitute a
manual pip install and label it planned frozen-install verification.

If Linux cache content is missing, the offline sync must fail and remain an
accurate blocker for offline reproducibility. Wheel availability alone does
not resolve the previously observed locked-URL lookup failure. A successfully
seeded cache or explicitly authorized online frozen sync would need separate
recorded evidence. Historical failures/successes must not be overwritten or
relabeled as verification of the final K7 source.
