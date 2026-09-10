"""Record CI event identity and run this checkout's finite gate serially."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from scripts.run_baseline import _load_manifest


def main(*, identity_only=False):
    evidence = Path("baseline-evidence")
    evidence.mkdir(exist_ok=True)
    event_path = os.environ.get("GITHUB_EVENT_PATH")
    event = json.loads(Path(event_path).read_text(encoding="utf-8")) if event_path else {}
    pull = event.get("pull_request", {})
    event_type = os.environ.get("GITHUB_EVENT_NAME", "local")
    ref = os.environ.get("GITHUB_REF", "")
    target = pull.get("base", {}).get("ref") if pull else ref.removeprefix("refs/heads/")
    edition = {"teaching-base": "base", "teaching-enhanced": "enhanced"}.get(target)
    if edition is None:
        raise SystemExit("CI candidate must target a teaching branch")
    manifest = _load_manifest()
    if manifest.edition != edition:
        raise SystemExit("event branch and checkout edition manifest disagree")
    checked = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    input_paths = ("uv.lock", "pyproject.toml", "scripts/baseline_manifest.json",
                   "scripts/run_baseline.py", "scripts/_test_process.py",
                   "scripts/_ci_baseline.py", ".github/workflows/baseline.yml")
    input_hashes = {name: hashlib.sha256(Path(name).read_bytes()).hexdigest()
                    for name in input_paths if Path(name).is_file()}
    source_hashes = {path.as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                     for path in sorted(Path("src").rglob("*.py"))}
    source_tree_hash = hashlib.sha256(json.dumps(
        sorted(source_hashes.items()), separators=(",", ":")
    ).encode("utf-8")).hexdigest()
    identity = {"event_type": event_type, "event_ref": ref,
                "event_sha": os.environ.get("GITHUB_SHA"),
                "pr_source_branch_if_applicable": pull.get("head", {}).get("ref"),
                "pr_target_branch_if_applicable": pull.get("base", {}).get("ref"),
                "candidate_sha": pull.get("head", {}).get("sha", os.environ.get("GITHUB_SHA")),
                "candidate_sha_kind": "pr_source_commit" if pull else "branch_event_commit",
                "pr_source_sha_if_applicable": pull.get("head", {}).get("sha"),
                "pr_target_sha_if_applicable": pull.get("base", {}).get("sha"),
                "pr_merge_preview_sha_if_applicable": os.environ.get("GITHUB_SHA") if pull else None,
                "checked_out_sha": checked, "tested_source_commit": None if identity_only else checked,
                "checkout_matches_event_sha": checked == os.environ.get("GITHUB_SHA"),
                "checkout_kind": "merge_preview" if pull else "branch_commit",
                "edition": edition,
                "edition_manifest_identity": hashlib.sha256(Path("scripts/baseline_manifest.json").read_bytes()).hexdigest(),
                "dispatch_ref_if_applicable": ref if event_type == "workflow_dispatch" else None,
                "candidate_target_branch_ref": "refs/heads/" + target,
                "head_at_event_if_not_pr": os.environ.get("GITHUB_SHA") if not pull else None,
                "current_branch_head_after_run": None,
                "run_id": os.environ.get("GITHUB_RUN_ID"),
                "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
                "input_sha256": input_hashes, "source_tree_sha256": source_tree_hash,
                "source_file_sha256": source_hashes,
                "identity_record_only": identity_only}
    (evidence / "source-identity.json").write_text(json.dumps(identity, indent=2) + "\n", encoding="utf-8")
    if identity_only:
        return 0
    selections = [("pure", None), *(("smoke", item.selector) for item in manifest.smoke)]
    results = []
    for index, (mode, selector) in enumerate(selections):
        command = [sys.executable, "scripts/run_baseline.py", "--" + mode]
        if selector is not None:
            command.append(selector)
        prefix = f"{index:02d}-{mode}"
        print("Running:", selector or "fixed pure batch", flush=True)
        started = time.monotonic()
        # The child owns its 30-second test deadline and cleanup grace.
        with (evidence / (prefix + ".log")).open("w") as output:
            result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT)
        record = {"mode": mode, "selector": selector, "command": command,
                  "exit_code": result.returncode, "log": prefix + ".log",
                  "seconds": time.monotonic() - started,
                  "tested_source_commit": checked}
        results.append(record)
        (evidence / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        if result.returncode:
            print((evidence / record["log"]).read_text(), flush=True)
            return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
