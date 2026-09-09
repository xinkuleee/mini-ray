"""Token/AST inventory only; never import or execute repository modules."""
import ast
from collections import Counter, defaultdict
import io
import json
from pathlib import Path
import subprocess
import tokenize

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent / "mini-ray"
GIT = ["git", "-c", "safe.directory=" + ROOT.as_posix()]
files = sorted(path.relative_to(ROOT).as_posix() for base in ("src", "tests", "scripts") for path in (ROOT / base).rglob("*.py") if "__pycache__" not in path.parts)

CATEGORIES = {
    "core.py": "core", "node.py": "node", "control.py": "control",
    "protocol.py": "protocol", "ownership.py": "ownership",
    "api.py": "api",
}
PUBLICATION = {"contained_cycle.py", "output_discovery.py", "output_publication.py",
    "output_publication_journal.py", "output_publication_node.py", "output_protocol.py",
    "output_recovery.py", "publication_sources.py", "publication_gate.py",
    "stored_publication.py", "owner_death_fence_registry.py"}
REFERENCES = {"contained_edges.py", "foreign_lineage.py", "foreign_lineage_runtime.py",
    "owner_reconstruction.py", "owner_service.py", "ref_transfer.py",
    "retained_replacement.py", "transfer_pins.py", "replica_cleanup.py"}
RECOVERY = {"recovery.py", "reconstruction_runtime.py", "targeted_reconstruction.py", "task_outputs.py"}
ACTOR_PG = {"actor_worker.py", "actor_state.py", "actor_client.py", "actor_arguments.py",
    "placement.py", "placement_group_runtime.py"}

def category(name):
    if name in CATEGORIES:
        return CATEGORIES[name]
    if name in PUBLICATION:
        return "publication_and_global_graph_satellites"
    if name in REFERENCES:
        return "reference_satellites"
    if name in RECOVERY:
        return "recovery_satellites"
    if name in ACTOR_PG:
        return "actor_and_pg_satellites"
    return "other_source"

def measure(path):
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.body and isinstance(node.body[0], ast.Expr):
                value = node.body[0].value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    docstrings.add((value.lineno, value.col_offset))
    code_lines, doc_lines, comment_lines = set(), set(), set()
    ignored = {tokenize.NL, tokenize.NEWLINE, tokenize.INDENT, tokenize.DEDENT, tokenize.ENDMARKER, tokenize.ENCODING}
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type in ignored:
            continue
        lines = set(range(token.start[0], token.end[0] + 1))
        if token.type == tokenize.COMMENT:
            comment_lines.update(lines)
        elif token.type == tokenize.STRING and token.start in docstrings:
            doc_lines.update(lines)
        else:
            code_lines.update(lines)
    doc_only = doc_lines - code_lines
    comment_only = comment_lines - code_lines - doc_only
    counts = Counter(physical=len(source.splitlines()), code_bearing=len(code_lines),
        docstring_only=len(doc_only), comment_only=len(comment_only),
        statements=sum(isinstance(node, ast.stmt) for node in ast.walk(tree)),
        classes=sum(isinstance(node, ast.ClassDef) for node in ast.walk(tree)),
        functions=sum(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) for node in ast.walk(tree)))
    counts["blank"] = counts["physical"] - counts["code_bearing"] - counts["docstring_only"] - counts["comment_only"]
    return counts

groups = defaultdict(Counter)
rows = []
for name in files:
    if not name.endswith(".py"):
        continue
    counts = measure(ROOT / name)
    counts["files"] = 1
    if name.startswith("src/"):
        group = category(Path(name).name)
        groups[group].update(counts)
        groups["ALL_SOURCE"].update(counts)
        rows.append({"file": name, "group": group, **counts})
    elif name.startswith("tests/"):
        groups["ALL_TESTS"].update(counts)
    elif name.startswith("scripts/"):
        groups["RUNNERS"].update(counts)
report = {
    "scope": "current worktree existing tracked and untracked src/tests/scripts Python files; historical archived docs excluded",
    "commit": subprocess.check_output(GIT + ["rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
    "method": "physical partition: token-bearing source lines excluding AST docstrings; docstring-only lines; comment-only lines; blank. Multiline literals are code. Counts are not executable LOC, necessary LOC, or deletable LOC. Groups are exclusive satellite file classifications; integration in core/node/control is separate.",
    "groups": dict(groups),
    "source_files": sorted(rows, key=lambda row: row["physical"], reverse=True),
}
output = HERE / "mini-ray-stage1-current-complexity.json"
output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(json.dumps(report["groups"], ensure_ascii=False, indent=2))
